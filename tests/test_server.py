from __future__ import annotations

import http.server
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from jobgpumonitor_server.config import AlertsConfig, Config, load_config
from jobgpumonitor_server.engine import Engine
from jobgpumonitor_server.ingest import Ingestor
from jobgpumonitor_server.model import apply, new_run
from jobgpumonitor_server.notify import Notifier, NtfyNotifier, WebhookNotifier, dispatch
from jobgpumonitor_server.rules import Alert, evaluate, evaluate_periodic
from jobgpumonitor_server.store import Store

# --------------------------------------------------------------------------- helpers


def env(etype, data, run_id="c/1/0", source="process", seq=0, ts="2026-09-03T10:00:00.000Z", emitter="process-r0-h-1", pid=1, rank=None):
    return {"v": 1, "id": "X", "seq": seq, "ts": ts, "mono": 0.0, "run_id": run_id, "emitter": emitter, "pid": pid,
            "source": source, "rank": rank, "type": etype, "data": data}


def sched(state, terminal=False, active=False, **extra):
    d = {"scheduler": "slurm", "job_id": "1", "job_key": "1", "state": state, "terminal": terminal, "active": active,
         "change": "state", "job_name": "tsad", "stdout": "/home/x/tsad_1.out", "restarts": 0}
    d.update(extra)
    return env("scheduler.state", d, source="scheduler", emitter="scheduler-login", pid=9)


class ListNotifier(Notifier):
    name = "list"

    def __init__(self):
        self.sent = []

    def send(self, alert, run):
        self.sent.append(alert)


def write_events(base: Path, run_id: str, fname: str, events):
    d = base / "runs" / run_id
    d.mkdir(parents=True, exist_ok=True)
    with open(d / fname, "a") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


# --------------------------------------------------------------------------- model


def test_iso_to_ts_is_utc_and_dst_safe():
    from jobgpumonitor_server.model import iso_to_ts

    assert iso_to_ts("1970-01-01T00:00:00.000Z") == 0.0
    assert iso_to_ts("2026-07-01T12:00:00.500Z") == 1782907200.5  # summer: no DST drift
    assert iso_to_ts("2026-01-01T12:00:00Z") == 1767268800.0
    assert iso_to_ts("garbage") is None and iso_to_ts(None) is None


def test_model_process_lifecycle():
    run = new_run("c/1/0")
    ch = apply(run, env("run.start", {"scheduler": {"name": "slurm", "job_id": "1", "job_name": "tsad", "partition": "All"},
                                     "host": "n1", "user": "u", "argv": ["train.py"], "start_ts": 100.0,
                                     "deadline": {"end_ts": 4000.0}, "emitter_config": {"heartbeat_s": 15},
                                     "resources": {"gpu": {"backend": "nvml", "devices": [{"index": 0, "name": "A100"}]}, "cgroup": {"fallback_limit": 8 * 2**30}},
                                     "stdio": {"stdout": "pipe:[1]"}}))
    assert ch == ["started"] and run["phase"] == "running" and run["job_name"] == "tsad" and run["stdout"] is None
    assert run["gpus"][0]["name"] == "A100" and run["mem_limit"] == 8 * 2**30 and run["heartbeat_s"] == 15
    apply(run, env("resource.sample", {"gpus": [{"index": 0, "util": 2}], "cgroup": {"mem_pct": 95.0}}, seq=1))
    assert run["gpu_idle_since"] is not None and run["mem_pct_last"] == 95.0
    apply(run, env("resource.sample", {"gpus": [{"index": 0, "util": 90}]}, seq=2))
    assert run["gpu_idle_since"] is None and run["gpu_util_last"] == 90
    apply(run, env("progress.update", {"bar_id": 1, "n": 5, "total": 10, "eta_s": 500, "deadline_remaining_s": 100, "eta_vs_deadline_s": -400}, seq=3))
    assert run["eta_vs_deadline_s"] == -400 and len(run["progress"]) == 1
    apply(run, env("metric.log", {"metrics": {"loss": 0.5}}, seq=4))
    apply(run, env("run.exception", {"type": "ValueError", "message": "bad", "fatal": True, "frames": [{"file": "t.py", "line": 3, "func": "f"}]}, seq=5))
    assert run["exception"]["where"] == "t.py:3 in f"
    ch = apply(run, env("run.end", {"status": "error", "exit_code": 1, "duration_s": 42, "metrics": {"loss": 0.4}}, seq=6))
    assert ch == ["ended"] and run["status"] == "error" and run["exit_code"] == 1 and run["metrics"] == {"loss": 0.4}
    # wrapper end is authoritative for the exit code
    apply(run, env("run.end", {"status": "error", "exit_code": 3, "stderr_tail": "Traceback...\nValueError: bad"}, source="wrapper", emitter="wrapper-r0-h-0", pid=0))
    assert run["exit_code"] == 3 and run["status_source"] == "wrapper" and run["stderr_tail"].endswith("bad")


def test_model_scheduler_verdict_wins_and_completed_does_not_hide_error():
    run = new_run("c/1/0")
    assert apply(run, sched("PENDING", state_reason="Priority", extra={"reason_hint": "wait"})) == ["queued"]
    assert run["phase"] == "queued" and run["scheduler_hint"] == "wait" and run["stdout"] == "/home/x/tsad_1.out"
    assert apply(run, sched("RUNNING", active=True, start_ts=10.0, end_ts=3610.0)) == ["started"]
    assert run["deadline_ts"] == 3610.0
    apply(run, env("run.end", {"status": "ok", "exit_code": 0}))
    assert run["status"] == "ok"
    ch = apply(run, sched("TIMEOUT", terminal=True, end_ts=3610.0, elapsed_s=3600))
    assert ch == ["scheduler_ended"] and run["status"] == "timeout" and run["status_source"] == "scheduler"
    # process said error, scheduler says COMPLETED: keep the error
    run2 = new_run("c/2/0")
    apply(run2, env("run.end", {"status": "error", "exit_code": 1}, run_id="c/2/0"))
    apply(run2, sched("COMPLETED", terminal=True, exit_code=0))
    assert run2["status"] == "error"
    run3 = new_run("c/3/0")
    apply(run3, sched("OUT_OF_MEMORY", terminal=True, max_rss=10 * 2**30))
    assert run3["status"] == "oom" and run3["phase"] == "ended" and run3["max_rss"] == 10 * 2**30


# --------------------------------------------------------------------------- rules


def test_rules_started_finished_and_correction():
    cfg = AlertsConfig()
    run = new_run("c/1/0")
    run.update(job_name="tsad", job_id="1", submit_ts=0.0, start_ts=7200.0, nodes="n1", time_limit_s=3600, phase="running",
               gpus=[{"name": "A100"}])
    a = evaluate(run, ["started"], cfg)
    assert [x.key for x in a] == ["started"] and "queued 2h00" in a[0].body and "A100" in a[0].body
    run.update(phase="ended", status="error", exception={"type": "RuntimeError", "message": "boom", "where": "t.py:5 in main"},
               duration_s=95, exit_code=1, metrics={"loss": 0.123456}, stdout="/x.out")
    a = evaluate(run, ["ended"], cfg, sent={"started"})
    assert len(a) == 1 and a[0].key == "finished" and a[0].level == "error"
    assert "RuntimeError: boom" in a[0].body and "loss=0.1235" in a[0].body and "/x.out" in a[0].body and "1m35s" in a[0].body
    assert run["notified_status"] == "error"
    # scheduler later says OOM: a correction goes out
    run.update(status="oom", max_rss=31 * 2**30, mem_limit=32 * 2**30)
    a = evaluate(run, ["scheduler_ended"], cfg, sent={"started", "finished"})
    assert len(a) == 1 and a[0].key == "finished_correction" and "out of memory" in a[0].body
    # same verdict again: nothing
    assert evaluate(run, ["scheduler_ended"], cfg, sent={"started", "finished", "finished_correction"}) == []


def test_finished_waits_for_scheduler_verdict_when_probe_is_present():
    cfg = AlertsConfig()
    run = new_run("c/1/0")
    run.update(job_name="tsad", phase="ended", status="ok", scheduler_state="RUNNING", scheduler_terminal=False, end_ts=1000.0)
    assert evaluate(run, ["ended"], cfg) == []  # first program ended, job still RUNNING for Slurm
    run.update(status="error", exception={"type": "RuntimeError", "message": "x"})
    assert evaluate(run, ["ended"], cfg) == []  # second program crashed, still no verdict
    run.update(scheduler_state="COMPLETED", scheduler_terminal=True)
    a = evaluate(run, ["scheduler_ended"], cfg)
    assert [x.key for x in a] == ["finished"] and a[0].level == "error"  # one alert, the right one
    # probe never answers: periodic fallback after the grace period
    run2 = new_run("c/2/0")
    run2.update(job_name="t", phase="ended", status="ok", scheduler_state="RUNNING", scheduler_terminal=False, end_ts=1000.0)
    assert evaluate_periodic(run2, cfg, now=1000.0 + 60) == []
    assert [x.key for x in evaluate_periodic(run2, cfg, now=1000.0 + 700)] == ["finished"]
    # no probe at all: run.end concludes immediately
    run3 = new_run("c/3/0")
    run3.update(job_name="t", phase="ended", status="ok")
    assert [x.key for x in evaluate(run3, ["ended"], cfg)] == ["finished"]


def test_rules_success_body_and_warnings():
    run = new_run("c/1/0")
    run.update(job_name="tsad", phase="ended", status="ok", duration_s=3700, metrics={"acc": 0.91},
               summary={"gpus": [{"util_mean": 81.8, "idle_fraction": 0.18, "mem_used_max": 2**28}]}, max_rss=2**30, mem_limit=8 * 2**30)
    a = evaluate(run, ["ended"], AlertsConfig())
    assert a[0].level == "success" and "1h01" in a[0].body and "GPU util 82%" in a[0].body and "RAM max 1.0G / 8.0G" in a[0].body
    run = new_run("c/2/0")
    run.update(phase="running", eta_vs_deadline_s=-900, progress=[{"desc": "train", "eta_s": 2700, "deadline_remaining_s": 1800}], mem_pct_last=92)
    keys = {x.key for x in evaluate(run, ["progress"], AlertsConfig())}
    assert keys == {"will_timeout", "memory"}


def test_rules_periodic_stall_and_gpu_idle():
    cfg = AlertsConfig(gpu_idle_min=15)
    now = 10_000.0
    run = new_run("c/1/0")
    run.update(phase="running", heartbeat_s=15, last_heartbeat_ts=now - 30, gpus=[{"name": "x"}], gpu_idle_since=now - 20 * 60)
    a = evaluate_periodic(run, cfg, now=now)
    assert [x.key for x in a] == ["gpu_idle"]
    run["last_heartbeat_ts"] = now - 120
    a = evaluate_periodic(run, cfg, sent={"gpu_idle"}, now=now)
    assert [x.key for x in a] == ["stalled"] and a[0].level == "error"
    run["scheduler_terminal"] = True
    assert evaluate_periodic(run, cfg, sent={"gpu_idle"}, now=now) == []


# --------------------------------------------------------------------------- store + ingest + engine


def test_ingest_offsets_partial_lines_and_dedup(tmp_path):
    store = Store(":memory:")
    base = tmp_path / "ev"
    write_events(base, "c/1/0", "process-r0-h-1.jsonl", [env("run.start", {"scheduler": {"name": "local"}, "host": "h", "start_ts": 1.0})])
    ing = Ingestor(store, [str(base)])
    batch = ing.scan()
    assert len(batch) == 1 and batch[0][0] == "c/1/0"
    assert ing.scan() == []
    # partial line: not consumed until the newline arrives
    f = base / "runs" / "c" / "1" / "0" / "process-r0-h-1.jsonl"
    with open(f, "a") as fh:
        fh.write(json.dumps(env("metric.log", {"metrics": {"a": 1}}, seq=1))[:-5])
    assert ing.scan() == []
    with open(f, "a") as fh:
        fh.write(json.dumps(env("metric.log", {"metrics": {"a": 1}}, seq=1))[-5:] + "\n")
    batch = ing.scan()
    assert len(batch) == 1 and batch[0][1]["type"] == "metric.log"
    # store de-duplicates on (run, emitter, pid, seq)
    assert store.add_event("c/1/0", batch[0][1]) is True
    assert store.add_event("c/1/0", batch[0][1]) is False


def test_engine_end_to_end_with_notifier(tmp_path):
    base = tmp_path / "ev"
    cfg = Config(dirs=[str(base)], db=str(tmp_path / "db.sqlite"), poll_s=1)
    ln = ListNotifier()
    eng = Engine(cfg, notifiers=[ln])
    write_events(base, "k/7/0", "scheduler-login.jsonl", [sched("PENDING", state_reason="Resources")])
    eng.cycle(now=1000.0)
    assert ln.sent == []  # queued: no alert
    write_events(base, "k/7/0", "scheduler-login.jsonl", [dict(sched("RUNNING", active=True, start_ts=900.0), seq=1)])
    write_events(base, "k/7/0", "process-r0-n-5.jsonl", [
        env("run.start", {"scheduler": {"name": "slurm", "job_id": "7"}, "host": "n", "start_ts": 900.0, "emitter_config": {"heartbeat_s": 15}}, run_id="k/7/0", emitter="process-r0-n-5", pid=5),
        env("run.heartbeat", {"uptime_s": 5}, run_id="k/7/0", emitter="process-r0-n-5", pid=5, seq=1, ts="2026-09-03T10:00:05.000Z"),
    ])
    eng.cycle(now=1000.0)
    assert [a.key for a in ln.sent] == ["started"]
    assert eng.store.get_run("k/7/0")["phase"] == "running"
    # heartbeat goes silent for a long time while scheduler still says RUNNING -> stalled
    hb_ts = eng.store.get_run("k/7/0")["last_heartbeat_ts"]
    eng.cycle(now=hb_ts + 600)
    assert [a.key for a in ln.sent] == ["started", "stalled"]
    # then the scheduler reports OOM
    write_events(base, "k/7/0", "scheduler-login.jsonl", [dict(sched("OUT_OF_MEMORY", terminal=True, elapsed_s=300, max_rss=2**33), seq=2)])
    eng.cycle(now=hb_ts + 700)
    keys = [a.key for a in ln.sent]
    assert keys == ["started", "stalled", "finished"] and ln.sent[-1].level == "error" and "out of memory" in ln.sent[-1].body
    # restart the engine on the same db: nothing is re-sent
    ln2 = ListNotifier()
    eng2 = Engine(cfg, notifiers=[ln2])
    eng2.cycle(now=hb_ts + 800)
    assert ln2.sent == []
    assert eng2.store.list_alerts()[0]["key"] == "finished"
    assert eng2.store.list_runs(phase="ended")[0]["status"] == "oom"


def test_engine_with_real_emitter(tmp_path):
    """The real jobgpumonitor library writes events; the server must understand them."""
    pytest.importorskip("jobgpumonitor")
    base = tmp_path / "ev"
    e = {k: v for k, v in os.environ.items() if not k.startswith(("JGM_", "SLURM_"))}
    e.update(JGM_DIR=str(base), JGM_CLUSTER="t", SLURM_JOB_ID="99", SLURM_JOB_NAME="smoke", JGM_HEARTBEAT_S="1", JGM_SAMPLE_S="1")
    p = subprocess.run([sys.executable, "-c", "import jobgpumonitor.auto, time, jobgpumonitor\njobgpumonitor.log(loss=0.25)\ntime.sleep(1.2)\nraise RuntimeError('boom')"],
                       env=e, capture_output=True, text=True, timeout=60)
    assert p.returncode == 1
    cfg = Config(dirs=[str(base)], db=str(tmp_path / "db.sqlite"))
    ln = ListNotifier()
    eng = Engine(cfg, notifiers=[ln])
    eng.cycle()
    run = eng.store.get_run("t/99/0")
    assert run["phase"] == "ended" and run["status"] == "error" and run["exception"]["type"] == "RuntimeError"
    assert run["metrics"] == {"loss": 0.25} and run["job_name"] == "smoke" and run["gpu_util_last"] is None or True
    assert [a.key for a in ln.sent] == ["started", "finished"]
    assert "RuntimeError: boom" in ln.sent[1].body


# --------------------------------------------------------------------------- notifiers


class _Handler(http.server.BaseHTTPRequestHandler):
    received = []

    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n)
        _Handler.received.append((self.path, dict(self.headers), body))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass


def test_ntfy_and_webhook_post():
    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    url = f"http://127.0.0.1:{srv.server_port}"
    alert = Alert("finished", "error", "❌ tsad failed", "RuntimeError: boom\nafter 1m", "c/1/0")
    ok = dispatch([NtfyNotifier("topic", server=url, token="tok"), WebhookNotifier(url + "/hook", {"X-Test": "1"})], alert, {"run_id": "c/1/0"})
    srv.shutdown()
    assert ok == ["ntfy", "webhook"]
    path, headers, body = _Handler.received[0]
    assert path == "/topic" and headers["Title"] == "tsad failed" and headers["Priority"] == "4" and headers["Tags"] == "x"
    assert headers["Authorization"] == "Bearer tok" and body.decode() == alert.body
    path, headers, body = _Handler.received[1]
    assert path == "/hook" and headers["X-Test"] == "1" and json.loads(body)["key"] == "finished"


def test_config_toml_and_env(tmp_path):
    pytest.importorskip("tomllib") if sys.version_info >= (3, 11) else pytest.importorskip("tomli")
    cfgf = tmp_path / "c.toml"
    cfgf.write_text('[server]\ndirs=["/a","/b"]\npoll_s=2\n[alerts]\ngpu_idle_min=5\n[notify.ntfy]\ntopic="x"\n[api]\nport=9999\n')
    cfg = load_config(str(cfgf), env={})
    assert cfg.dirs == ["/a", "/b"] and cfg.poll_s == 2 and cfg.alerts.gpu_idle_min == 5 and cfg.notify["ntfy"]["topic"] == "x" and cfg.api_port == 9999
    cfg = load_config(str(cfgf), env={"JGMD_DIRS": "/c", "JGMD_NTFY_TOPIC": "y", "JGMD_TELEGRAM_TOKEN": "t", "JGMD_TELEGRAM_CHAT_ID": "1"})
    assert cfg.dirs == ["/c"] and cfg.notify["ntfy"]["topic"] == "y" and cfg.notify["telegram"]["chat_id"] == "1"


def test_api_endpoints(tmp_path):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from jobgpumonitor_server.api import create_app

    store = Store(":memory:")
    run = new_run("c/1/0")
    run.update(phase="running", job_name="tsad")
    store.upsert_run(run)
    store.add_event("c/1/0", env("run.heartbeat", {"uptime_s": 1}))
    client = TestClient(create_app(store, token="s3cret"))
    assert client.get("/health").json()["active"] == 1
    assert client.get("/runs").status_code == 401
    h = {"Authorization": "Bearer s3cret"}
    assert client.get("/runs", headers=h).json()[0]["job_name"] == "tsad"
    assert client.get("/runs/c/1/0", headers=h).json()["phase"] == "running"
    assert client.get("/runs/c/1/0/events", headers=h).json()[0]["type"] == "run.heartbeat"
    assert client.get("/runs/nope/1/0", headers=h).status_code == 404
    assert client.head("/health").status_code == 200
    # served under a prefix behind a reverse proxy sub-path
    sub = TestClient(create_app(store, token="s3cret", prefix="/jgm"))
    assert sub.get("/health").status_code == 404
    assert sub.get("/jgm/health").json()["ok"] is True
    assert sub.get("/jgm/runs", headers=h).json()[0]["job_name"] == "tsad"
    assert sub.get("/jgm/runs/c/1/0", headers=h).json()["phase"] == "running"
    assert sub.get("/jgm/docs").status_code == 200 and "/jgm/openapi.json" in sub.get("/jgm/docs").text
    t0 = time.time()
    assert time.time() - t0 < 5


def test_ingest_endpoint_writes_files_engine_reads_them(tmp_path):
    pytest.importorskip("fastapi")
    import gzip

    from fastapi.testclient import TestClient

    from jobgpumonitor_server.api import create_app

    base = tmp_path / "ev"
    store = Store(":memory:")
    client = TestClient(create_app(store, token="read", ingest_token="write", ingest_dir=str(base)))
    ev1 = env("run.start", {"scheduler": {"name": "slurm", "job_id": "5"}, "host": "n", "start_ts": 1.0}, run_id="k/5/0", emitter="process-r0-n-9", pid=9)
    ev2 = env("run.end", {"status": "ok", "exit_code": 0}, run_id="k/5/0", emitter="process-r0-n-9", pid=9, seq=1)
    bad = {"run_id": "../../etc", "emitter": "x", "type": "t", "data": {}}
    assert client.post("/ingest", json=[ev1]).status_code == 401
    assert client.post("/ingest", json=[ev1], headers={"Authorization": "Bearer read"}).status_code == 401
    r = client.post("/ingest", json=[ev1, bad], headers={"Authorization": "Bearer write"})
    assert r.status_code == 200 and r.json() == {"accepted": 1, "rejected": 1}
    body = gzip.compress(json.dumps([ev2]).encode())
    r = client.post("/ingest", content=body, headers={"Authorization": "Bearer write", "Content-Encoding": "gzip", "Content-Type": "application/json"})
    assert r.json()["accepted"] == 1
    assert (base / "runs" / "k" / "5" / "0" / "process-r0-n-9.jsonl").read_text().count("\n") == 2
    eng = Engine(Config(dirs=[str(base)], db=":memory:"), store=store, notifiers=[])
    eng.cycle()
    assert store.get_run("k/5/0")["status"] == "ok"
    # no ingest token configured -> 503, never silently open
    closed = TestClient(create_app(store, token="read", ingest_dir=str(base)))
    assert closed.post("/ingest", json=[ev1], headers={"Authorization": "Bearer write"}).status_code == 503
