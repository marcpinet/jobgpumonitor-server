"""Incremental reader of the emitter's JSONL files (offsets persisted, partial lines re-read later)."""

from __future__ import annotations

import glob
import json
import os
from typing import Any, Dict, List, Tuple

from .store import Store

_MAX_LINE = 4 * 1024 * 1024
_MAX_CHUNK = 64 * 1024 * 1024


class Ingestor:
    def __init__(self, store: Store, dirs: List[str]) -> None:
        self.store = store
        self.dirs = [os.path.expanduser(d) for d in dirs]
        self.errors = 0

    def files(self) -> List[str]:
        out: List[str] = []
        for d in self.dirs:
            out.extend(glob.glob(os.path.join(d, "runs", "*", "*", "*", "*.jsonl")))
        return sorted(out)

    @staticmethod
    def run_id_of(path: str) -> str:
        parts = os.path.normpath(path).split(os.sep)
        return "/".join(parts[-4:-1])

    def scan(self, max_events: int = 50_000) -> List[Tuple[str, Dict[str, Any]]]:
        """Read everything new since the last scan. Returns ``[(run_id, event), ...]`` in file order.

        The stored offset always points at the start of a line, so a line that is still being
        written (no newline yet) is simply read again on the next scan.
        """
        out: List[Tuple[str, Dict[str, Any]]] = []
        for path in self.files():
            try:
                st = os.stat(path)
            except OSError:
                continue
            offset, inode = self.store.get_offset(path)
            if (inode is not None and inode != st.st_ino) or st.st_size < offset:
                offset = 0  # file replaced or truncated: events are de-duplicated by the store
            if st.st_size == offset:
                continue
            try:
                with open(path, "rb") as f:
                    f.seek(offset)
                    chunk = f.read(min(st.st_size - offset, _MAX_CHUNK))
            except OSError:
                continue
            lines = chunk.split(b"\n")
            tail = lines.pop()
            consumed = len(chunk) - len(tail)
            if tail and len(tail) >= _MAX_LINE:
                consumed = len(chunk)  # give up on a monstrous line
            run_id = self.run_id_of(path)
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    env = json.loads(line.decode("utf-8", "replace"))
                except ValueError:
                    self.errors += 1
                    continue
                if isinstance(env, dict) and env.get("type") and isinstance(env.get("data"), dict):
                    out.append((run_id, env))
            self.store.set_offset(path, offset + consumed, st.st_ino, st.st_mtime)
            if len(out) >= max_events:
                break
        return out


def read_run_events(path_or_dir: str) -> List[Dict[str, Any]]:
    """Every event of a run directory (or one file), ordered by timestamp then sequence."""
    files = [path_or_dir] if os.path.isfile(path_or_dir) else sorted(glob.glob(os.path.join(path_or_dir, "*.jsonl")))
    out: List[Dict[str, Any]] = []
    for f in files:
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        out.append(json.loads(line))
                    except ValueError:
                        pass
    out.sort(key=lambda e: (e.get("ts", ""), e.get("emitter", ""), e.get("seq", 0)))
    return out
