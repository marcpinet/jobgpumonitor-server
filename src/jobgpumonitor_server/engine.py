"""The main loop: scan files -> fold events into runs -> evaluate rules -> notify -> persist."""

from __future__ import annotations

import sys
import time
from typing import Any, Dict, List, Optional

from .config import Config
from .ingest import Ingestor
from .model import apply, new_run
from .notify import Notifier, build_notifiers, dispatch
from .rules import Alert, evaluate, evaluate_periodic
from .store import Store


class Engine:
    def __init__(self, cfg: Config, store: Optional[Store] = None, notifiers: Optional[List[Notifier]] = None, quiet: bool = False) -> None:
        self.cfg = cfg
        self.store = store or Store(cfg.db)
        self.ingestor = Ingestor(self.store, cfg.dirs)
        self.notifiers = notifiers if notifiers is not None else build_notifiers(cfg.notify)
        self.quiet = quiet
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._last_prune = 0.0
        self.cycles = 0
        self.ingested = 0
        self.alerts_sent = 0

    def _run(self, run_id: str) -> Dict[str, Any]:
        run = self._cache.get(run_id)
        if run is None:
            run = self.store.get_run(run_id) or new_run(run_id)
            self._cache[run_id] = run
        return run

    def _notify(self, run: Dict[str, Any], alerts: List[Alert]) -> None:
        for a in alerts:
            delivered = dispatch(self.notifiers, a, run)
            self.store.record_alert(run["run_id"], a.key, a.level, a.title, a.body, delivered)
            self.alerts_sent += 1

    def cycle(self, now: Optional[float] = None) -> Dict[str, int]:
        """One pass. Returns counters (for ``--once`` and tests)."""
        now = now or time.time()
        self.cycles += 1
        batch = self.ingestor.scan()
        changed: Dict[str, List[str]] = {}
        for run_id, env in batch:
            if not self.store.add_event(run_id, env):
                continue  # duplicate
            run = self._run(run_id)
            changes = apply(run, env)
            self.ingested += 1
            changed.setdefault(run_id, []).extend(changes)
        for run_id, changes in changed.items():
            run = self._run(run_id)
            sent = self.store.alert_keys(run_id)
            alerts = evaluate(run, changes, self.cfg.alerts, sent)
            self.store.upsert_run(run)
            self._notify(run, alerts)
        # time-based rules on active runs, plus recently ended ones still waiting for the scheduler
        for run in self.store.active_runs() + self.store.list_runs(phase="ended", limit=50):
            run = self._run(run["run_id"])
            sent = self.store.alert_keys(run["run_id"])
            alerts = evaluate_periodic(run, self.cfg.alerts, sent, now)
            if alerts:
                self._notify(run, alerts)
                self.store.upsert_run(run)
        # forget ended runs from the cache, prune old events daily
        for rid in [r for r, doc in self._cache.items() if doc.get("phase") == "ended" and now - (doc.get("updated_ts") or now) > 600]:
            del self._cache[rid]
        if now - self._last_prune > 86400:
            self._last_prune = now
            self.store.prune_events(self.cfg.keep_events_days)
        return {"events": len(batch), "runs_changed": len(changed), "alerts": self.alerts_sent}

    def run_forever(self, stop=None) -> None:  # type: ignore[no-untyped-def]
        while stop is None or not stop.is_set():
            t = time.monotonic()
            try:
                self.cycle()
            except Exception as e:  # keep the loop alive whatever happens
                print(f"[jgmd] cycle failed: {e}", file=sys.stderr)
            delay = max(0.5, self.cfg.poll_s - (time.monotonic() - t))
            if stop is None:
                time.sleep(delay)
            else:
                stop.wait(delay)
