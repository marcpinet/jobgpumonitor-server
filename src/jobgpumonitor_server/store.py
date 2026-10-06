"""SQLite persistence: runs (one JSON document each), raw events, file offsets, sent alerts."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Set

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, cluster TEXT, job_key TEXT, restart INTEGER,
  phase TEXT, status TEXT, job_name TEXT, user TEXT,
  start_ts REAL, end_ts REAL, updated_ts REAL, doc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS runs_phase ON runs(phase, updated_ts);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, ts TEXT NOT NULL, type TEXT NOT NULL,
  emitter TEXT, pid INTEGER, seq INTEGER, doc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_run ON events(run_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS events_unique ON events(run_id, emitter, pid, seq);
CREATE TABLE IF NOT EXISTS offsets (path TEXT PRIMARY KEY, offset INTEGER NOT NULL, inode INTEGER, mtime REAL);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, key TEXT NOT NULL, level TEXT, title TEXT, body TEXT,
  ts REAL NOT NULL, delivered TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS alerts_unique ON alerts(run_id, key);
CREATE TABLE IF NOT EXISTS logs (
  run_id TEXT NOT NULL, stream TEXT NOT NULL, path TEXT, text TEXT NOT NULL DEFAULT '',
  size INTEGER NOT NULL DEFAULT 0, received INTEGER NOT NULL DEFAULT 0, truncated INTEGER NOT NULL DEFAULT 0,
  eof INTEGER NOT NULL DEFAULT 0, updated_ts REAL NOT NULL, PRIMARY KEY (run_id, stream)
);
"""

#: Per stream, keep at most this much text (the head is dropped, a marker is kept).
LOG_KEEP_BYTES = 4 * 1024 * 1024


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._db.execute("PRAGMA journal_mode=WAL")
            self._db.execute("PRAGMA synchronous=NORMAL")
            self._db.executescript(_SCHEMA)
            self._add_columns("logs", {"emitter": "TEXT"})
            # ingestion time: retention counts from when we received an event, not from when
            # it was emitted (a backlog shipped late must not be dropped as soon as it lands)
            self._add_columns("events", {"ingested_ts": "REAL"})
            # channels an alert still has to reach, retried with backoff
            self._add_columns("alerts", {"pending": "TEXT", "attempts": "INTEGER NOT NULL DEFAULT 0", "next_try_ts": "REAL"})

    def _add_columns(self, table: str, cols: Dict[str, str]) -> None:
        have = {r[1] for r in self._db.execute(f"PRAGMA table_info({table})")}
        for name, decl in cols.items():
            if name not in have:
                self._db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

    # ------------------------------------------------------------------ runs

    def upsert_run(self, run: Dict[str, Any]) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO runs(run_id,cluster,job_key,restart,phase,status,job_name,user,start_ts,end_ts,updated_ts,doc)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET cluster=excluded.cluster,"
                " job_key=excluded.job_key, restart=excluded.restart, phase=excluded.phase, status=excluded.status,"
                " job_name=excluded.job_name, user=excluded.user, start_ts=excluded.start_ts, end_ts=excluded.end_ts,"
                " updated_ts=excluded.updated_ts, doc=excluded.doc",
                (run["run_id"], run.get("cluster"), run.get("job_key"), run.get("restart"), run.get("phase"),
                 run.get("status"), run.get("job_name"), run.get("user"), run.get("start_ts"), run.get("end_ts"),
                 run.get("updated_ts") or time.time(), json.dumps(run, separators=(",", ":"))),
            )

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._db.execute("SELECT doc FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return json.loads(row["doc"]) if row else None

    def list_runs(self, phase: Optional[str] = None, limit: int = 100, cluster: Optional[str] = None) -> List[Dict[str, Any]]:
        q = "SELECT doc FROM runs"
        cond, args = [], []
        if phase:
            cond.append("phase=?")
            args.append(phase)
        if cluster:
            cond.append("cluster=?")
            args.append(cluster)
        if cond:
            q += " WHERE " + " AND ".join(cond)
        q += " ORDER BY updated_ts DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._db.execute(q, args).fetchall()
        return [json.loads(r["doc"]) for r in rows]

    def cluster_freshness(self) -> Dict[str, float]:
        """Per cluster, when an event of any of its runs was last ingested."""
        with self._lock:
            rows = self._db.execute("SELECT cluster, MAX(updated_ts) AS t FROM runs GROUP BY cluster").fetchall()
        return {r["cluster"]: r["t"] for r in rows if r["t"] is not None}

    def active_runs(self) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._db.execute("SELECT doc FROM runs WHERE phase IN ('queued','running')").fetchall()
        return [json.loads(r["doc"]) for r in rows]

    # ------------------------------------------------------------------ events

    def add_event(self, run_id: str, env: Dict[str, Any]) -> bool:
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO events(run_id,ts,type,emitter,pid,seq,doc,ingested_ts) VALUES(?,?,?,?,?,?,?,?)",
                    (run_id, env.get("ts"), env.get("type"), env.get("emitter"), env.get("pid"), env.get("seq"),
                     json.dumps(env, separators=(",", ":")), time.time()),
                )
                return True
            except sqlite3.IntegrityError:
                return False  # already ingested (file re-read after truncation / restart)

    def events(self, run_id: str, after_id: int = 0, limit: int = 1000, types: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        q = "SELECT id, doc FROM events WHERE run_id=? AND id>?"
        args: List[Any] = [run_id, after_id]
        if types:
            ts = list(types)
            q += " AND type IN (" + ",".join("?" * len(ts)) + ")"
            args += ts
        q += " ORDER BY id LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._db.execute(q, args).fetchall()
        out = []
        for r in rows:
            d = json.loads(r["doc"])
            d["_id"] = r["id"]
            out.append(d)
        return out

    def prune_events(self, older_than_days: int) -> int:
        """Drop events ingested more than ``older_than_days`` ago (rows from before the
        ``ingested_ts`` column fall back to their emission time)."""
        if older_than_days <= 0:
            return 0
        cutoff = time.time() - older_than_days * 86400
        cutoff_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(cutoff))
        with self._lock:
            cur = self._db.execute(
                "DELETE FROM events WHERE ingested_ts < ? OR (ingested_ts IS NULL AND ts < ?)", (cutoff, cutoff_iso))
            return cur.rowcount

    # ------------------------------------------------------------------ logs (stdout/stderr chunks)

    def append_log(self, run_id: str, data: Dict[str, Any], emitter: Optional[str] = None) -> bool:
        """Append a chunk. A stream is owned by the first emitter that delivered it (the job's
        wrapper and the login-node probe may both tail the same output): others are ignored."""
        stream = data.get("stream") or "stdout"
        text = data.get("text") or ""
        if data.get("skipped"):
            text = f"[… {_human_bytes(data['skipped'])} sautés …]\n" + text
        with self._lock:
            row = self._db.execute("SELECT text, received, emitter FROM logs WHERE run_id=? AND stream=?", (run_id, stream)).fetchone()
            if row and row["emitter"] and emitter and row["emitter"] != emitter:
                return False
            cur = row["text"] if row else ""
            received = (row["received"] if row else 0) + len(text.encode("utf-8"))
            new = _append_folded(cur, text)
            if len(new) > LOG_KEEP_BYTES:
                cut = new.find("\n", len(new) - LOG_KEEP_BYTES)
                new = "[… début tronqué …]\n" + new[cut + 1 if cut >= 0 else len(new) - LOG_KEEP_BYTES:]
            self._db.execute(
                "INSERT INTO logs(run_id,stream,path,text,size,received,truncated,eof,updated_ts,emitter) VALUES(?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(run_id,stream) DO UPDATE SET path=excluded.path, text=excluded.text, size=excluded.size,"
                " received=excluded.received, truncated=excluded.truncated, eof=excluded.eof, updated_ts=excluded.updated_ts,"
                " emitter=COALESCE(logs.emitter, excluded.emitter)",
                (run_id, stream, data.get("path"), new, int(data.get("size") or 0), received,
                 1 if data.get("truncated") else 0, 1 if data.get("eof") else 0, time.time(), emitter),
            )
            return True

    def get_log(self, run_id: str, stream: str, tail: Optional[int] = None) -> Optional[Dict[str, Any]]:
        with self._lock:
            row = self._db.execute("SELECT * FROM logs WHERE run_id=? AND stream=?", (run_id, stream)).fetchone()
        if not row:
            return None
        text = row["text"]
        cut_head = False
        if tail and len(text) > tail:
            text = text[-tail:]
            cut_head = True
        return {"run_id": run_id, "stream": stream, "path": row["path"], "text": text, "size": row["size"],
                "received": row["received"], "truncated": bool(row["truncated"]), "eof": bool(row["eof"]),
                "updated_ts": row["updated_ts"], "head_cut": cut_head}

    def log_streams(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._db.execute("SELECT stream, path, size, received, truncated, eof, updated_ts FROM logs WHERE run_id=? ORDER BY stream DESC", (run_id,)).fetchall()
        return [dict(r) for r in rows]  # stdout first

    def prune_logs(self, older_than_days: int) -> int:
        if older_than_days <= 0:
            return 0
        with self._lock:
            cur = self._db.execute("DELETE FROM logs WHERE updated_ts < ?", (time.time() - older_than_days * 86400,))
            return cur.rowcount

    # ------------------------------------------------------------------ offsets

    def get_offset(self, path: str) -> tuple[int, Optional[int]]:
        with self._lock:
            row = self._db.execute("SELECT offset, inode FROM offsets WHERE path=?", (path,)).fetchone()
        return (row["offset"], row["inode"]) if row else (0, None)

    def set_offset(self, path: str, offset: int, inode: Optional[int], mtime: float) -> None:
        with self._lock:
            self._db.execute(
                "INSERT INTO offsets(path,offset,inode,mtime) VALUES(?,?,?,?) ON CONFLICT(path) DO UPDATE SET"
                " offset=excluded.offset, inode=excluded.inode, mtime=excluded.mtime",
                (path, offset, inode, mtime),
            )

    def known_paths(self) -> Dict[str, float]:
        with self._lock:
            rows = self._db.execute("SELECT path, mtime FROM offsets").fetchall()
        return {r["path"]: r["mtime"] for r in rows}

    # ------------------------------------------------------------------ alerts

    def alert_keys(self, run_id: str) -> Set[str]:
        with self._lock:
            rows = self._db.execute("SELECT key FROM alerts WHERE run_id=?", (run_id,)).fetchall()
        return {r["key"] for r in rows}

    def record_alert(self, run_id: str, key: str, level: str, title: str, body: str, delivered: List[str],
                     pending: Optional[List[str]] = None, next_try_ts: Optional[float] = None) -> bool:
        with self._lock:
            try:
                self._db.execute(
                    "INSERT INTO alerts(run_id,key,level,title,body,ts,delivered,pending,attempts,next_try_ts)"
                    " VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (run_id, key, level, title, body, time.time(), ",".join(delivered), ",".join(pending or []), 1,
                     next_try_ts if pending else None),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def pending_alerts(self, now: float) -> List[Dict[str, Any]]:
        """Alerts that some channel has not received yet and that are due for another try."""
        with self._lock:
            rows = self._db.execute(
                "SELECT id,run_id,key,level,title,body,ts,delivered,pending,attempts FROM alerts"
                " WHERE pending IS NOT NULL AND pending != '' AND next_try_ts <= ? ORDER BY id", (now,)).fetchall()
        return [dict(r) for r in rows]

    def update_alert_delivery(self, alert_id: int, delivered: List[str], pending: List[str], attempts: int,
                              next_try_ts: Optional[float]) -> None:
        with self._lock:
            self._db.execute("UPDATE alerts SET delivered=?, pending=?, attempts=?, next_try_ts=? WHERE id=?",
                             (",".join(delivered), ",".join(pending), attempts, next_try_ts if pending else None, alert_id))

    def list_alerts(self, limit: int = 100, run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        q = "SELECT run_id,key,level,title,body,ts,delivered,pending,attempts FROM alerts"
        args: List[Any] = []
        if run_id:
            q += " WHERE run_id=?"
            args.append(run_id)
        q += " ORDER BY id DESC LIMIT ?"
        args.append(limit)
        with self._lock:
            rows = self._db.execute(q, args).fetchall()
        return [dict(r) for r in rows]

    def close(self) -> None:
        with self._lock:
            self._db.close()


def _fold_redraws(text: str) -> str:
    """What a terminal shows once carriage returns redrew a line (tqdm): the last state of
    each line. A last line still being drawn keeps its trailing ``\\r`` so that what comes
    next overwrites it."""
    lines = text.split("\n")
    last = len(lines) - 1
    for i, line in enumerate(lines):
        if "\r" in line:
            parts = [p for p in line.split("\r") if p]
            folded = parts[-1] if parts else ""
            if i == last and line.endswith("\r"):
                folded += "\r"
            lines[i] = folded
    return "\n".join(lines)


def _append_folded(cur: str, text: str) -> str:
    """``cur + text``, with redraws folded across the junction (emitters fold within a
    chunk; a line redrawn over two chunks is folded here)."""
    j = cur.rfind("\n") + 1
    if "\r" not in text and "\r" not in cur[j:]:
        return cur + text
    return cur[:j] + _fold_redraws(cur[j:] + text)


def _human_bytes(n: Any) -> str:
    try:
        v = float(n)
    except (TypeError, ValueError):
        return str(n)
    for unit in ("octets", "Ko", "Mo"):
        if v < 1024:
            return f"{v:.0f} {unit}" if unit == "octets" else f"{v:.1f} {unit}"
        v /= 1024.0
    return f"{v:.1f} Go"
