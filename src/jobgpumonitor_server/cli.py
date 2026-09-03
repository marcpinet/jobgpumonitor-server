"""``jgmd``: serve (ingest + alerts [+ API]), runs, show, alerts, notify-test, init."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional

from . import __version__
from .config import DEFAULT_CONFIG_PATH, EXAMPLE_CONFIG, Config, load_config
from .engine import Engine
from .model import display_name, fmt_duration
from .notify import build_notifiers, dispatch
from .rules import Alert
from .store import Store


def _cfg(args: argparse.Namespace) -> Config:
    return load_config(args.config)


def cmd_init(args: argparse.Namespace) -> int:
    path = args.config or DEFAULT_CONFIG_PATH
    if os.path.exists(path) and not args.force:
        print(f"{path} exists (use --force to overwrite)", file=sys.stderr)
        return 1
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(EXAMPLE_CONFIG)
    print(f"wrote {path}; edit the [notify.*] section, then run: jgmd serve")
    return 0


def cmd_serve(args: argparse.Namespace) -> int:
    cfg = _cfg(args)
    engine = Engine(cfg)
    chans = ", ".join(n.name for n in engine.notifiers) or "none"
    print(f"jgmd {__version__}: dirs={cfg.dirs} db={cfg.db} poll={cfg.poll_s:.0f}s notify={chans}"
          + (f" config={cfg.path}" if cfg.path else " (no config file, env/defaults)"), file=sys.stderr)
    if args.once:
        c = engine.cycle()
        print(f"events={c['events']} runs_changed={c['runs_changed']} alerts={c['alerts']}", file=sys.stderr)
        return 0
    stop = threading.Event()
    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(s, lambda *_: stop.set())
        except (ValueError, OSError):
            pass
    if args.api:
        from .api import serve_api

        t = threading.Thread(target=engine.run_forever, args=(stop,), name="jgmd-engine", daemon=True)
        t.start()
        pre = "/" + cfg.api_prefix.strip("/") if cfg.api_prefix.strip("/") else ""
        print(f"API on http://{cfg.api_host}:{cfg.api_port}{pre}  (docs at {pre}/docs)", file=sys.stderr)
        serve_api(engine.store, cfg.api_host, cfg.api_port, cfg.api_token, cfg.api_prefix, cfg.ingest_token, cfg.dirs[0])
        stop.set()
        return 0
    engine.run_forever(stop)
    return 0


def _status_icon(run: Dict[str, Any]) -> str:
    ph, st = run.get("phase"), run.get("status")
    if ph == "queued":
        return "⏳"
    if ph == "running":
        return "▶"
    return {"ok": "✅", None: "?"}.get(st, "❌")


def cmd_runs(args: argparse.Namespace) -> int:
    cfg = _cfg(args)
    store = Store(cfg.db)
    runs = store.list_runs(phase=args.phase, limit=args.limit)
    if args.json:
        print(json.dumps(runs, indent=1, default=str))
        return 0
    if not runs:
        print("no runs yet")
        return 0
    print(f"{'':2} {'run':<34} {'name':<22} {'phase':<8} {'status':<10} {'dur':>8}  detail")
    for r in runs:
        dur = r.get("duration_s") or ((r.get("last_event_ts") or 0) - (r.get("start_ts") or 0) if r.get("start_ts") else None)
        detail = ""
        if r.get("phase") == "running":
            bars = r.get("progress") or []
            if bars:
                b = bars[-1]
                detail = f"{b.get('desc') or ''} {b.get('n')}/{b.get('total')} eta {fmt_duration(b.get('eta_s'))}"
            if r.get("gpu_util_last") is not None:
                detail += f"  gpu {r['gpu_util_last']:.0f}%"
        elif r.get("phase") == "queued":
            detail = f"{r.get('scheduler_state')} {r.get('scheduler_reason') or ''}"
        elif r.get("exception"):
            detail = f"{r['exception'].get('type')}: {(r['exception'].get('message') or '')[:50]}"
        elif r.get("status") not in (None, "ok"):
            detail = str(r.get("scheduler_state") or "")
        name = (r.get("job_name") or display_name(r))[:22]
        print(f"{_status_icon(r):2} {r['run_id']:<34} {name:<22} {r.get('phase') or '':<8} {r.get('status') or '':<10} {fmt_duration(dur):>8}  {detail}")
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    cfg = _cfg(args)
    store = Store(cfg.db)
    run = store.get_run(args.run_id)
    if run is None:
        # allow a bare job id
        for r in store.list_runs(limit=1000):
            if r.get("job_key") == args.run_id or r.get("job_id") == args.run_id:
                run = r
                break
    if run is None:
        print("unknown run", file=sys.stderr)
        return 1
    if args.events:
        for e in store.events(run["run_id"], limit=args.events):
            print(json.dumps(e, separators=(",", ":")))
        return 0
    print(json.dumps(run, indent=1, default=str))
    return 0


def cmd_alerts(args: argparse.Namespace) -> int:
    cfg = _cfg(args)
    store = Store(cfg.db)
    for a in store.list_alerts(limit=args.limit):
        ts = time.strftime("%Y-%m-%d %H:%M", time.localtime(a["ts"]))
        print(f"{ts}  {a['level']:<8} {a['run_id']:<34} {a['title']}   [{a['delivered'] or 'not delivered'}]")
    return 0


def cmd_notify_test(args: argparse.Namespace) -> int:
    cfg = _cfg(args)
    notifiers = build_notifiers(cfg.notify)
    alert = Alert("test", "info", "🔔 jobgpumonitor-server test", args.message or "notifications are working", "test/0/0")
    ok = dispatch(notifiers, alert, {"run_id": "test/0/0"})
    print(f"delivered via: {', '.join(ok) or 'nothing'}  (configured: {', '.join(n.name for n in notifiers)})")
    return 0 if ok else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="jgmd", description="jobgpumonitor server: run state, alerts, API")
    p.add_argument("--version", action="version", version=f"jobgpumonitor-server {__version__}")
    p.add_argument("-c", "--config", help=f"TOML config (default {DEFAULT_CONFIG_PATH})")
    sub = p.add_subparsers(dest="command")

    s = sub.add_parser("serve", help="ingest events, evaluate rules, send notifications")
    s.add_argument("--once", action="store_true", help="one cycle then exit (cron friendly)")
    s.add_argument("--api", action="store_true", help="also serve the HTTP API (needs the [api] extra)")
    s.set_defaults(func=cmd_serve)

    r = sub.add_parser("runs", help="list runs")
    r.add_argument("--phase", choices=["queued", "running", "ended"])
    r.add_argument("-n", "--limit", type=int, default=40)
    r.add_argument("--json", action="store_true")
    r.set_defaults(func=cmd_runs)

    sh = sub.add_parser("show", help="show one run (run_id or job id)")
    sh.add_argument("run_id")
    sh.add_argument("--events", type=int, default=0, help="print the last N raw events instead")
    sh.set_defaults(func=cmd_show)

    a = sub.add_parser("alerts", help="alert history")
    a.add_argument("-n", "--limit", type=int, default=40)
    a.set_defaults(func=cmd_alerts)

    nt = sub.add_parser("notify-test", help="send a test notification through every configured channel")
    nt.add_argument("message", nargs="?")
    nt.set_defaults(func=cmd_notify_test)

    i = sub.add_parser("init", help="write an example config file")
    i.add_argument("--force", action="store_true")
    i.set_defaults(func=cmd_init)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 2
    return int(args.func(args) or 0)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
