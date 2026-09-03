"""Fold protocol events into one run document. Pure functions, no I/O.

A run document is a plain dict (stored as JSON). ``apply`` mutates it and returns the list
of *changes* worth reacting to: ``started``, ``ended``, ``scheduler_ended``, ``heartbeat``,
``exception``, ``progress``, ``sample``, ``queued``.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

_SCHED_STATUS = {
    "COMPLETED": "ok", "FAILED": "error", "TIMEOUT": "timeout", "OUT_OF_MEMORY": "oom", "CANCELLED": "cancelled",
    "PREEMPTED": "preempted", "NODE_FAIL": "node_fail", "BOOT_FAIL": "node_fail", "DEADLINE": "timeout",
    "REVOKED": "cancelled", "UNKNOWN_ENDED": "unknown", "TERMINATED": "ok", "ERROR": "error",
}
BAD_STATUSES = {"error", "timeout", "oom", "cancelled", "preempted", "node_fail", "killed", "interrupted", "unknown"}


def iso_to_ts(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    try:
        base, _, frac = s.rstrip("Z").partition(".")
        t = time.mktime(time.strptime(base, "%Y-%m-%dT%H:%M:%S")) - time.timezone
        return t + (float("0." + frac) if frac else 0.0)
    except ValueError:
        return None


def new_run(run_id: str) -> Dict[str, Any]:
    cluster, key, restart = (run_id.split("/") + ["", "0"])[:3]
    return {
        "run_id": run_id, "cluster": cluster, "job_key": key, "restart": int(restart) if restart.isdigit() else 0,
        "job_id": None, "job_name": None, "user": None,
        "phase": "unknown",          # queued | running | ended | unknown
        "status": None,              # ok | error | killed | interrupted | timeout | oom | cancelled | preempted | node_fail | unknown
        "status_source": None,       # process | wrapper | scheduler
        "scheduler": None, "scheduler_state": None, "scheduler_reason": None, "scheduler_hint": None, "scheduler_terminal": False,
        "host": None, "nodes": None, "partition": None, "gpus": [], "gpu_backend": None, "container": None,
        "argv": None, "command": None, "stdout": None, "stderr": None, "workdir": None, "git": None,
        "submit_ts": None, "start_ts": None, "end_ts": None, "deadline_ts": None, "time_limit_s": None,
        "duration_s": None, "exit_code": None, "signal": None,
        "heartbeat_s": None, "last_heartbeat_ts": None, "last_event_ts": None, "first_event_ts": None,
        "progress": [], "metrics": {}, "eta_vs_deadline_s": None,
        "gpu_util_last": None, "gpu_idle_since": None, "mem_pct_last": None, "mem_limit": None, "max_rss": None,
        "exception": None, "warnings": 0, "last_logs": [], "last_signal": None, "stderr_tail": None,
        "summary": None, "emitters": [], "ranks": None, "notified_status": None,
        "events": 0, "updated_ts": None,
    }


def _set_if_none(run: Dict[str, Any], key: str, value: Any) -> None:
    if value is not None and run.get(key) is None:
        run[key] = value


def _process_status(status: Optional[str]) -> Optional[str]:
    return {"ok": "ok", "error": "error", "interrupted": "interrupted", "killed": "killed", "interactive": "ok"}.get(status or "")


def apply(run: Dict[str, Any], env: Dict[str, Any]) -> List[str]:
    changes: List[str] = []
    etype = env.get("type", "")
    data = env.get("data") or {}
    source = env.get("source")
    rank = env.get("rank")
    ts = iso_to_ts(env.get("ts")) or time.time()
    run["events"] = run.get("events", 0) + 1
    run["last_event_ts"] = max(run.get("last_event_ts") or 0, ts)
    _set_if_none(run, "first_event_ts", ts)
    run["updated_ts"] = time.time()
    emitter = env.get("emitter")
    if emitter and emitter not in run["emitters"]:
        run["emitters"].append(emitter)
    primary = source in ("process", "wrapper") and rank in (None, 0)

    if etype == "run.start":
        sched = data.get("scheduler") or {}
        _set_if_none(run, "job_id", sched.get("job_id"))
        _set_if_none(run, "job_name", sched.get("job_name") or data.get("name"))
        _set_if_none(run, "user", data.get("user"))
        _set_if_none(run, "scheduler", sched.get("name"))
        _set_if_none(run, "partition", sched.get("partition"))
        _set_if_none(run, "nodes", sched.get("nodes"))
        _set_if_none(run, "time_limit_s", sched.get("time_limit_s"))
        _set_if_none(run, "host", data.get("host"))
        _set_if_none(run, "container", data.get("container"))
        _set_if_none(run, "git", data.get("git"))
        if data.get("command"):
            run["command"] = " ".join(data["command"])
        elif data.get("argv") and run.get("argv") is None:
            run["argv"] = data["argv"]
        stdio = data.get("stdio") or {}
        if stdio.get("stdout") and not str(stdio["stdout"]).startswith(("pipe:", "socket:", "/dev/")):
            _set_if_none(run, "stdout", stdio["stdout"])
        res = data.get("resources") or {}
        gpu = res.get("gpu") or {}
        if gpu.get("devices") and not run["gpus"]:
            run["gpus"] = [{"index": d.get("index"), "name": d.get("name"), "mem_total": d.get("mem_total")} for d in gpu["devices"]]
            run["gpu_backend"] = gpu.get("backend")
        cg = res.get("cgroup") or {}
        _set_if_none(run, "mem_limit", cg.get("mem_limit") or cg.get("fallback_limit"))
        dl = data.get("deadline") or {}
        if dl.get("end_ts"):
            run["deadline_ts"] = dl["end_ts"]
        ec = data.get("emitter_config") or {}
        _set_if_none(run, "heartbeat_s", ec.get("heartbeat_s"))
        if data.get("rank"):
            run["ranks"] = data["rank"].get("world_size")
        st = data.get("start_ts") or ts
        run["start_ts"] = min(run["start_ts"], st) if run.get("start_ts") else st
        if run["phase"] != "ended":
            if run["phase"] != "running":
                changes.append("started")
            run["phase"] = "running"

    elif etype == "run.heartbeat":
        if primary or not run.get("last_heartbeat_ts") or ts > run["last_heartbeat_ts"]:
            run["last_heartbeat_ts"] = ts
        if primary:
            if data.get("progress") is not None:
                run["progress"] = data["progress"]
            if data.get("metrics"):
                run["metrics"] = data["metrics"]
        changes.append("heartbeat")

    elif etype == "resource.sample":
        gpus = data.get("gpus") or []
        utils = [g["util"] for g in gpus if g.get("util") is not None]
        if utils:
            u = sum(utils) / len(utils)
            run["gpu_util_last"] = round(u, 1)
            if max(utils) < 5:
                _set_if_none(run, "gpu_idle_since", ts)
            else:
                run["gpu_idle_since"] = None
        cg = data.get("cgroup") or {}
        if cg.get("mem_pct") is not None:
            run["mem_pct_last"] = cg["mem_pct"]
            _set_if_none(run, "mem_limit", cg.get("mem_limit"))
        changes.append("sample")

    elif etype == "progress.update":
        bars = [b for b in run["progress"] if b.get("bar_id") != data.get("bar_id")]
        if not data.get("done"):
            bars.append(data)
        run["progress"] = bars[-8:]
        if data.get("eta_vs_deadline_s") is not None and not data.get("done"):
            run["eta_vs_deadline_s"] = data["eta_vs_deadline_s"]
        changes.append("progress")

    elif etype == "metric.log":
        run["metrics"].update(data.get("metrics") or {})

    elif etype == "log.line":
        if data.get("levelno", 30) >= 30:
            run["warnings"] = run.get("warnings", 0) + 1
        run["last_logs"] = (run.get("last_logs") or [])[-4:] + [f"{data.get('level')}: {data.get('message', '')[:300]}"]

    elif etype == "run.exception":
        if data.get("fatal", True):
            run["exception"] = {k: data.get(k) for k in ("type", "message", "is_oom", "kind", "thread")}
            frames = data.get("frames") or []
            if frames:
                f = frames[-1]
                run["exception"]["where"] = f"{f.get('file')}:{f.get('line')} in {f.get('func')}"
            changes.append("exception")

    elif etype == "signal.received":
        run["last_signal"] = data.get("signal")

    elif etype == "run.end":
        st = _process_status(data.get("status"))
        if source == "wrapper" or (primary and run.get("status_source") != "wrapper"):
            if source == "wrapper" or run.get("status_source") not in ("wrapper", "scheduler"):
                run["status"] = st
                run["status_source"] = source
            if data.get("exit_code") is not None and (source == "wrapper" or run.get("exit_code") is None):
                run["exit_code"] = data["exit_code"]
            _set_if_none(run, "signal", data.get("signal"))
            if data.get("stderr_tail"):
                run["stderr_tail"] = data["stderr_tail"][-2000:]
            if data.get("summary"):
                run["summary"] = data["summary"]
            if data.get("metrics"):
                run["metrics"] = data["metrics"]
            if data.get("exception") and not run.get("exception"):
                run["exception"] = data["exception"]
            run["end_ts"] = max(run.get("end_ts") or 0, data.get("end_ts") or ts)
            if data.get("duration_s") is not None:
                run["duration_s"] = data["duration_s"]
            if run["phase"] != "ended":
                run["phase"] = "ended"
                changes.append("ended")

    elif etype == "scheduler.state":
        run["scheduler"] = data.get("scheduler") or run.get("scheduler")
        run["scheduler_state"] = data.get("state")
        run["scheduler_reason"] = data.get("state_reason")
        run["scheduler_hint"] = (data.get("extra") or {}).get("reason_hint")
        run["scheduler_terminal"] = bool(data.get("terminal"))
        for k in ("job_id", "job_name", "user", "partition", "stdout", "stderr", "workdir", "submit_ts", "time_limit_s"):
            if data.get(k) is not None:
                run[k] = data[k] if k in ("stdout", "stderr", "job_name") or run.get(k) is None else run[k]
        if data.get("nodes"):
            run["nodes"] = data["nodes"]
        if data.get("command") and not run.get("command"):
            run["command"] = data["command"]
        if data.get("max_rss"):
            run["max_rss"] = data["max_rss"]
        state = data.get("state") or ""
        if data.get("terminal"):
            st = _SCHED_STATUS.get(state, "unknown")
            # the scheduler is authoritative for how the allocation ended, except that a clean
            # COMPLETED must not hide an error the process itself reported
            if not (st == "ok" and run.get("status") in BAD_STATUSES):
                run["status"] = st
                run["status_source"] = "scheduler"
            if data.get("exit_code") is not None and run.get("exit_code") is None:
                run["exit_code"] = data["exit_code"]
            if data.get("exit_signal") and not run.get("signal"):
                run["signal"] = data["exit_signal"]
            if data.get("end_ts"):
                run["end_ts"] = max(run.get("end_ts") or 0, data["end_ts"])
            if data.get("elapsed_s") is not None and run.get("duration_s") is None:
                run["duration_s"] = data["elapsed_s"]
            if data.get("start_ts") and not run.get("start_ts"):
                run["start_ts"] = data["start_ts"]
            if run["phase"] != "ended":
                run["phase"] = "ended"
                changes.append("ended")
            changes.append("scheduler_ended")
        elif data.get("active"):
            if data.get("start_ts"):
                run["start_ts"] = min(run["start_ts"], data["start_ts"]) if run.get("start_ts") else data["start_ts"]
            if data.get("end_ts"):
                run["deadline_ts"] = data["end_ts"]
            if run["phase"] != "ended":
                if run["phase"] != "running":
                    changes.append("started")
                run["phase"] = "running"
        else:  # PENDING and friends
            if run["phase"] in ("unknown", "queued"):
                if run["phase"] != "queued":
                    changes.append("queued")
                run["phase"] = "queued"

    return changes


def display_name(run: Dict[str, Any]) -> str:
    name = run.get("job_name") or (run.get("argv") or [None])[0] or run.get("command") or run["job_key"]
    if isinstance(name, str) and "/" in name:
        name = name.rsplit("/", 1)[-1]
    jid = run.get("job_id") or run.get("job_key")
    return f"{name} ({jid})" if jid and str(jid) not in str(name) else str(name)


def fmt_duration(s: Optional[float]) -> str:
    if s is None:
        return "?"
    s = int(s)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m{s % 60:02d}s"
    h, rem = divmod(s, 3600)
    if h < 48:
        return f"{h}h{rem // 60:02d}"
    return f"{h // 24}d{h % 24}h"


def fmt_bytes(n: Optional[float]) -> str:
    if not n:
        return "?"
    for unit in ("B", "K", "M", "G", "T"):
        if n < 1024:
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}P"
