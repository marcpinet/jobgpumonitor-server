"""Alert rules over run documents. Each alert has a stable ``key`` so it fires once per run."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .config import AlertsConfig
from .model import BAD_STATUSES, display_name, fmt_bytes, fmt_duration


@dataclass
class Alert:
    key: str
    level: str          # info | success | warning | error
    title: str
    body: str
    run_id: str


def _where(run: Dict[str, Any]) -> str:
    bits = []
    if run.get("nodes") or run.get("host"):
        bits.append(str(run.get("nodes") or run.get("host")))
    if run.get("gpus"):
        names = {g.get("name") for g in run["gpus"] if g.get("name")}
        bits.append(f"{len(run['gpus'])}x " + "/".join(sorted(n for n in names if n)) if names else f"{len(run['gpus'])} GPU")
    if run.get("partition"):
        bits.append(f"partition {run['partition']}")
    return ", ".join(bits)


def _metrics_line(run: Dict[str, Any]) -> str:
    m = run.get("metrics") or {}
    items = []
    for k, v in list(m.items())[:6]:
        if isinstance(v, float):
            items.append(f"{k}={v:.4g}")
        else:
            items.append(f"{k}={v}")
    return ", ".join(items)


def _efficiency(run: Dict[str, Any]) -> str:
    s = run.get("summary") or {}
    parts = []
    for g in s.get("gpus") or []:
        if g.get("util_mean") is not None:
            parts.append(f"GPU util {g['util_mean']:.0f}% (idle {int(100 * (g.get('idle_fraction') or 0))}%)")
        if g.get("mem_used_max"):
            parts.append(f"GPU mem max {fmt_bytes(g['mem_used_max'])}")
    rss = run.get("max_rss") or s.get("cgroup_mem_max") or s.get("rss_max")
    if rss:
        lim = run.get("mem_limit")
        parts.append(f"RAM max {fmt_bytes(rss)}" + (f" / {fmt_bytes(lim)}" if lim else ""))
    return ", ".join(parts)


def _failure_reason(run: Dict[str, Any]) -> str:
    st = run.get("status")
    sched = run.get("scheduler_state")
    exc = run.get("exception") or {}
    if st == "oom":
        return "killed by the scheduler: out of memory" + (f" (max RSS {fmt_bytes(run['max_rss'])})" if run.get("max_rss") else "")
    if st == "timeout":
        return f"time limit reached ({fmt_duration(run.get('time_limit_s'))})"
    if st == "cancelled":
        return "cancelled"
    if st == "preempted":
        return "preempted by a higher-priority job"
    if st == "node_fail":
        return "node failure"
    if exc.get("type"):
        msg = (exc.get("message") or "")[:200]
        where = exc.get("where")
        return f"{exc['type']}: {msg}" + (f"\n  at {where}" if where else "")
    if st == "killed":
        return f"killed by signal {run.get('signal') or '?'}"
    if st == "interrupted":
        return "interrupted (KeyboardInterrupt)"
    if run.get("exit_code") not in (None, 0):
        return f"exit code {run['exit_code']}"
    if sched:
        return f"scheduler state {sched}"
    return "unknown reason"


def _stderr_tail(run: Dict[str, Any], n: int = 3) -> str:
    tail = run.get("stderr_tail") or ""
    lines = [ln for ln in tail.splitlines() if ln.strip() and not ln.startswith("[jgm]")]
    return "\n".join(lines[-n:])


def evaluate(run: Dict[str, Any], changes: List[str], cfg: AlertsConfig, sent: Optional[set] = None) -> List[Alert]:
    """Alerts triggered by *changes* just applied to ``run`` (call after each event batch)."""
    sent = sent or set()
    out: List[Alert] = []
    name = display_name(run)
    rid = run["run_id"]

    if "started" in changes and cfg.started and "started" not in sent:
        queued = None
        if run.get("submit_ts") is not None and run.get("start_ts"):
            queued = run["start_ts"] - run["submit_ts"]
        body = _where(run)
        if queued is not None and queued > 60:
            body += f"\nqueued {fmt_duration(queued)}"
        if run.get("time_limit_s"):
            body += f"\ntime limit {fmt_duration(run['time_limit_s'])}"
        out.append(Alert("started", "info", f"🚀 {name} started", body.strip(), rid))

    if ("ended" in changes or "scheduler_ended" in changes) and cfg.finished:
        status = run.get("status") or "unknown"
        # A job may run several programs one after the other: when a scheduler probe follows
        # this job, wait for its terminal verdict instead of concluding on the first run.end
        # (evaluate_periodic sends the alert anyway if the probe stays silent too long).
        waiting_for_scheduler = run.get("scheduler_state") and not run.get("scheduler_terminal") and "scheduler_ended" not in changes
        if "finished" not in sent and not waiting_for_scheduler:
            out.append(_finished_alert(run, name, status))
            run["notified_status"] = status
        elif "scheduler_ended" in changes and run.get("notified_status") not in (None, status) and status in BAD_STATUSES:
            a = _finished_alert(run, name, status)
            a.key = "finished_correction"
            a.title = "⚠️ correction: " + a.title
            out.append(a)
            run["notified_status"] = status

    if cfg.will_timeout and "will_timeout" not in sent and run.get("phase") == "running":
        eta = run.get("eta_vs_deadline_s")
        if eta is not None and eta < -120:
            bars = run.get("progress") or []
            b = bars[-1] if bars else {}
            body = (f"tqdm ETA {fmt_duration(b.get('eta_s'))} but only {fmt_duration(b.get('deadline_remaining_s'))} left "
                    f"({fmt_duration(-eta)} short)")
            if b.get("desc"):
                body = f"{b['desc']}: " + body
            out.append(Alert("will_timeout", "warning", f"⏱ {name} will not finish in time", body, rid))

    if cfg.mem_pct and "memory" not in sent and run.get("phase") == "running":
        pct = run.get("mem_pct_last")
        if pct is not None and pct >= cfg.mem_pct:
            body = f"{pct:.0f}% of the memory limit" + (f" ({fmt_bytes(run['mem_limit'])})" if run.get("mem_limit") else "")
            out.append(Alert("memory", "warning", f"🧠 {name} close to its memory limit", body, rid))
    return out


def _finished_alert(run: Dict[str, Any], name: str, status: str) -> Alert:
    rid = run["run_id"]
    dur = fmt_duration(run.get("duration_s") or ((run.get("end_ts") or 0) - (run.get("start_ts") or 0) or None))
    if status == "ok":
        body = f"ran {dur}"
        m = _metrics_line(run)
        if m:
            body += "\n" + m
        e = _efficiency(run)
        if e:
            body += "\n" + e
        if run.get("warnings"):
            body += f"\n{run['warnings']} warning(s) logged"
        return Alert("finished", "success", f"✅ {name} finished", body, rid)
    body = _failure_reason(run) + f"\nafter {dur}"
    if run.get("exit_code") not in (None, 0):
        body += f", exit code {run['exit_code']}"
    m = _metrics_line(run)
    if m:
        body += "\nlast metrics: " + m
    tail = _stderr_tail(run)
    if tail and not (run.get("exception") or {}).get("type"):
        body += "\n" + tail
    if run.get("stdout"):
        body += f"\nlog: {run['stdout']}"
    level = "error" if status in BAD_STATUSES else "warning"
    label = {"oom": "OOM", "timeout": "timed out", "cancelled": "cancelled", "preempted": "preempted",
             "node_fail": "node failure", "interrupted": "interrupted", "killed": "killed"}.get(status, "failed")
    return Alert("finished", level, f"❌ {name} {label}", body, rid)


def evaluate_periodic(run: Dict[str, Any], cfg: AlertsConfig, sent: Optional[set] = None, now: Optional[float] = None) -> List[Alert]:
    """Time-based alerts for active runs (call every poll)."""
    sent = sent or set()
    now = now or time.time()
    out: List[Alert] = []
    name = display_name(run)
    rid = run["run_id"]
    if run.get("phase") == "ended":
        # finished alert deferred while waiting for the scheduler's verdict: give up after a while
        if cfg.finished and "finished" not in sent and not run.get("scheduler_terminal") and now - (run.get("end_ts") or now) > cfg.finished_grace_s:
            status = run.get("status") or "unknown"
            out.append(_finished_alert(run, name, status))
            run["notified_status"] = status
        return out
    if run.get("phase") != "running":
        return out

    hb = run.get("last_heartbeat_ts")
    if hb and "stalled" not in sent and not run.get("scheduler_terminal"):
        limit = cfg.stalled_after_s or max(60.0, 3.0 * float(run.get("heartbeat_s") or 15))
        silent = now - hb
        if silent > limit:
            body = f"no heartbeat for {fmt_duration(silent)}"
            if run.get("scheduler_state"):
                body += f", scheduler says {run['scheduler_state']}"
            body += "\nthe process is dead or frozen; if the scheduler still says RUNNING the allocation is being wasted"
            out.append(Alert("stalled", "error", f"💀 {name} stopped reporting", body, rid))

    if cfg.gpu_idle_min and "gpu_idle" not in sent and run.get("gpus"):
        since = run.get("gpu_idle_since")
        if since and now - since >= cfg.gpu_idle_min * 60:
            body = f"every visible GPU below 5% for {fmt_duration(now - since)}"
            if run.get("progress"):
                body += "; progress bar still moving" if run["progress"][-1].get("rate") else ""
            out.append(Alert("gpu_idle", "warning", f"🥱 {name}: GPU idle", body, rid))
    return out
