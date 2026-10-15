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

#: How long a channel that missed an alert keeps being retried (with backoff).
ALERT_RETRY_FOR_S = 6 * 3600.0


def _retry_delay(attempts: int) -> float:
    """1 min after the first failure, then doubling, at most 30 min."""
    return min(60.0 * 2 ** max(0, attempts - 1), 1800.0)


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

    def _notify(self, run: Dict[str, Any], alerts: List[Alert], now: Optional[float] = None) -> None:
        now = now or time.time()
        for a in alerts:
            delivered = dispatch(self.notifiers, a, run)
            failed = [n.name for n in self.notifiers if n.name not in delivered]
            self.store.record_alert(run["run_id"], a.key, a.level, a.title, a.body, delivered,
                                    pending=failed, next_try_ts=now + _retry_delay(1))
            self.alerts_sent += 1

    def _retry_alerts(self, now: float) -> None:
        """Send again, to the channels that missed them, the alerts a failure kept from going out."""
        for row in self.store.pending_alerts(now):
            names = [n for n in (row["pending"] or "").split(",") if n]
            targets = [n for n in self.notifiers if n.name in names]
            delivered = [n for n in (row["delivered"] or "").split(",") if n]
            if not targets or now - row["ts"] > ALERT_RETRY_FOR_S:
                print(f"[jgmd] giving up on alert {row['key']!r} of {row['run_id']} for {', '.join(names)}", file=sys.stderr)
                self.store.update_alert_delivery(row["id"], delivered, [], row["attempts"], None)
                continue
            alert = Alert(row["key"], row["level"], row["title"], row["body"], row["run_id"])
            run = self.store.get_run(row["run_id"]) or {"run_id": row["run_id"]}
            ok = dispatch(targets, alert, run)
            attempts = int(row["attempts"] or 0) + 1
            left = [n for n in names if n not in ok]
            self.store.update_alert_delivery(row["id"], delivered + ok, left, attempts, now + _retry_delay(attempts))

    def cycle(self, now: Optional[float] = None) -> Dict[str, int]:
        """One pass. Returns counters (for ``--once`` and tests)."""
        now = now or time.time()
        self.cycles += 1
        changed: Dict[str, List[str]] = {}
        to_send: List[tuple] = []
        try:
            # one transaction for the whole batch: offsets, events, logs and runs land together
            # (and the SD card sees one sync per cycle instead of one per event)
            with self.store.batch():
                batch = self.ingestor.scan()
                for run_id, env in batch:
                    if env.get("type") == "log.chunk":
                        # the text goes to the logs table; the events table only keeps a light record
                        data = env.get("data") or {}
                        light = dict(env, data={k: v for k, v in data.items() if k != "text"})
                        if not self.store.add_event(run_id, light):
                            continue
                        self.store.append_log(run_id, data, env.get("emitter"))
                    elif not self.store.add_event(run_id, env):
                        continue  # duplicate
                    run = self._run(run_id)
                    changes = apply(run, env)
                    self.ingested += 1
                    changed.setdefault(run_id, []).extend(changes)
                for run_id, changes in changed.items():
                    run = self._run(run_id)
                    sent = self.store.alert_keys(run_id)
                    to_send.append((run, evaluate(run, changes, self.cfg.alerts, sent)))
                    self.store.upsert_run(run)
        except Exception:
            self._cache.clear()  # the cached documents may hold changes that were rolled back
            raise
        for run, alerts in to_send:  # network calls: outside the transaction
            self._notify(run, alerts, now)
        # time-based rules on active runs, plus recently ended ones still waiting for the scheduler
        fresh = self.store.cluster_freshness()
        for run in self.store.active_runs() + self.store.list_runs(phase="ended", limit=50):
            run = self._run(run["run_id"])
            sent = self.store.alert_keys(run["run_id"])
            alerts = evaluate_periodic(run, self.cfg.alerts, sent, now, cluster_fresh_ts=fresh.get(run.get("cluster")))
            if alerts:
                self._notify(run, alerts, now)
                self.store.upsert_run(run)
        self._retry_alerts(now)
        # forget ended runs from the cache, prune old events daily
        for rid in [r for r, doc in self._cache.items() if doc.get("phase") == "ended" and now - (doc.get("updated_ts") or now) > 600]:
            del self._cache[rid]
        if now - self._last_prune > 86400:
            self._last_prune = now
            self.store.prune_events(self.cfg.keep_events_days)
            self.store.prune_logs(self.cfg.keep_events_days)
            self.ingestor.prune_files(self.cfg.keep_events_days, now)
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
