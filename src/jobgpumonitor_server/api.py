"""Read-only HTTP API over the store (optional: ``pip install "jobgpumonitor-server[api]"``).

    GET /health
    GET /runs?phase=running&cluster=…&limit=100
    GET /runs/{run_id}                 (run_id contains slashes: /runs/marcel-c3/8224458/0)
    GET /runs/{run_id}/events?after=0&limit=1000&types=run.heartbeat,metric.log
    GET /runs/{run_id}/stream          server-sent events, new events as they are ingested
    GET /alerts?limit=100
    POST /ingest                       JSON list of events (gzip ok), bearer = ingest_token
"""

import asyncio
import gzip
import hmac
import json
import os
import re
import time
from typing import Any, Dict, List, Optional

from .store import Store


def append_events(base_dir: str, events: List[Any]) -> "tuple[int, int]":
    """Append well-formed events to ``<base_dir>/runs/<run_id>/<emitter>.jsonl``. Returns (accepted, rejected)."""
    accepted = rejected = 0
    handles: Dict[str, Any] = {}
    try:
        for ev in events:
            if not (isinstance(ev, dict) and isinstance(ev.get("run_id"), str) and isinstance(ev.get("emitter"), str)
                    and isinstance(ev.get("type"), str) and isinstance(ev.get("data"), dict)
                    and _RUN_ID_RE.match(ev["run_id"]) and _EMITTER_RE.match(ev["emitter"])):
                rejected += 1
                continue
            path = os.path.join(base_dir, "runs", *ev["run_id"].split("/"), ev["emitter"] + ".jsonl")
            fh = handles.get(path)
            if fh is None:
                os.makedirs(os.path.dirname(path), exist_ok=True)
                fh = handles[path] = open(path, "a", encoding="utf-8")
            fh.write(json.dumps(ev, separators=(",", ":"), ensure_ascii=False) + "\n")
            accepted += 1
    finally:
        for fh in handles.values():
            fh.flush()
            fh.close()
    return accepted, rejected


def create_app(store: Store, token: str = "", prefix: str = "", ingest_token: str = "", ingest_dir: str = "") -> Any:
    """Build the app. With ``prefix`` (e.g. ``/jgm``) every route is served under that path,
    which lets a reverse proxy expose the API as a sub-path of an existing host."""
    app = _build_app(store, token, ingest_token, ingest_dir)
    prefix = "/" + prefix.strip("/") if prefix and prefix.strip("/") else ""
    if not prefix:
        return app
    from fastapi import FastAPI

    outer = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    outer.mount(prefix, app)
    return outer


_RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]+/[A-Za-z0-9_.\-]+/[0-9]+$")
_EMITTER_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_MAX_BODY = 32 * 1024 * 1024


def _build_app(store: Store, token: str = "", ingest_token: str = "", ingest_dir: str = "") -> Any:
    try:
        from fastapi import Depends, FastAPI, HTTPException, Query, Request
        from fastapi.responses import StreamingResponse
    except ImportError as e:  # pragma: no cover
        raise SystemExit('the API needs fastapi and uvicorn: pip install "jobgpumonitor-server[api]"') from e

    app = FastAPI(title="jobgpumonitor-server", version="0.1.0")

    async def auth(request: Request) -> None:
        if not token:
            return
        got = request.headers.get("authorization") or ""
        if not hmac.compare_digest(got.encode(), f"Bearer {token}".encode()):
            raise HTTPException(status_code=401, detail="bad token", headers={"WWW-Authenticate": "Bearer"})

    async def ingest_auth(request: Request) -> None:
        if not ingest_token:
            raise HTTPException(status_code=503, detail="ingest disabled: set [api] ingest_token")
        got = request.headers.get("authorization") or ""
        if not hmac.compare_digest(got.encode(), f"Bearer {ingest_token}".encode()):
            raise HTTPException(status_code=401, detail="bad ingest token", headers={"WWW-Authenticate": "Bearer"})

    @app.post("/ingest", dependencies=[Depends(ingest_auth)])
    async def ingest(request: Request) -> Dict[str, Any]:
        """Accept a JSON list of events (optionally gzip-encoded) and append them to the local
        event directory, where the engine picks them up exactly like locally written files."""
        raw = await request.body()
        if len(raw) > _MAX_BODY:
            raise HTTPException(status_code=413, detail="body too large")
        if request.headers.get("content-encoding", "").lower() == "gzip":
            try:
                raw = gzip.decompress(raw)
            except (OSError, EOFError) as e:
                raise HTTPException(status_code=400, detail="bad gzip") from e
        try:
            events = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise HTTPException(status_code=400, detail="body must be a JSON list of events") from e
        if not isinstance(events, list):
            raise HTTPException(status_code=400, detail="body must be a JSON list of events")
        accepted, rejected = append_events(ingest_dir, events)
        return {"accepted": accepted, "rejected": rejected}

    @app.api_route("/health", methods=["GET", "HEAD"])
    def health() -> Dict[str, Any]:
        return {"ok": True, "ts": time.time(), "active": len(store.active_runs())}

    @app.get("/runs", dependencies=[Depends(auth)])
    def runs(phase: Optional[str] = None, cluster: Optional[str] = None, limit: int = Query(100, le=1000)) -> List[Dict[str, Any]]:
        return store.list_runs(phase=phase, limit=limit, cluster=cluster)

    @app.get("/alerts", dependencies=[Depends(auth)])
    def alerts(limit: int = Query(100, le=1000), run_id: Optional[str] = None) -> List[Dict[str, Any]]:
        return store.list_alerts(limit=limit, run_id=run_id)

    @app.get("/runs/{run_id:path}/events", dependencies=[Depends(auth)])
    def events(run_id: str, after: int = 0, limit: int = Query(1000, le=10000), types: Optional[str] = None) -> List[Dict[str, Any]]:
        return store.events(run_id, after_id=after, limit=limit, types=types.split(",") if types else None)

    @app.get("/runs/{run_id:path}/stream", dependencies=[Depends(auth)])
    async def stream(run_id: str, after: int = 0) -> Any:
        async def gen():
            last = after
            while True:
                batch = store.events(run_id, after_id=last, limit=500)
                for e in batch:
                    last = e["_id"]
                    yield f"id: {last}\nevent: {e['type']}\ndata: {json.dumps(e, separators=(',', ':'))}\n\n"
                if not batch:
                    yield ": keepalive\n\n"
                    await asyncio.sleep(2)

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/runs/{run_id:path}", dependencies=[Depends(auth)])
    def run(run_id: str) -> Dict[str, Any]:
        doc = store.get_run(run_id)
        if doc is None:
            raise HTTPException(status_code=404, detail="unknown run")
        return doc

    return app


def serve_api(store: Store, host: str, port: int, token: str = "", prefix: str = "", ingest_token: str = "", ingest_dir: str = "") -> None:  # pragma: no cover
    import uvicorn

    uvicorn.run(create_app(store, token, prefix, ingest_token, ingest_dir), host=host, port=port, log_level="warning")
