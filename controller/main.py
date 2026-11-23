"""Mini Distributed Cloud Object Store - Controller (FastAPI)."""
import collections
import json
import logging
import mimetypes
import os
import time
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, File, Form, Query, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

import background
import config
from db import Database
from models import HeartbeatRequest, RegisterRequest
from services import ApiError, ObjectStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("controller")


class EventBuffer(logging.Handler):
    """Keeps the last controller log lines so the web UI can show a live event feed."""

    def __init__(self):
        super().__init__()
        self.items = collections.deque(maxlen=300)
        self.seq = 0

    def emit(self, record):
        self.seq += 1
        self.items.append({"id": self.seq, "ts": record.created, "level": record.levelname,
                           "msg": record.getMessage()})


events = EventBuffer()
log.addHandler(events)
STATIC_INDEX = os.path.join(os.path.dirname(__file__), "static", "index.html")


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(os.path.dirname(config.DB_PATH) or ".", exist_ok=True)
    db = Database(config.DB_PATH)
    db.mark_all_workers_offline()   # workers prove liveness again via heartbeat
    client = httpx.AsyncClient(
        timeout=httpx.Timeout(config.WORKER_TIMEOUT, connect=config.WORKER_CONNECT_TIMEOUT),
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20))
    app.state.svc = ObjectStore(db, client)
    bg = background.start_all(app.state.svc)
    log.info("controller started: RF=%d chunk_size=%d", config.REPLICATION_FACTOR, config.CHUNK_SIZE)
    yield
    for t in bg:
        t.cancel()
    for t in list(app.state.svc.tasks):
        t.cancel()
    await client.aclose()
    db.close()


app = FastAPI(title="Mini Distributed Cloud Object Store", lifespan=lifespan)


def svc(request: Request) -> ObjectStore:
    return request.app.state.svc


# ------------------------------------------------------------------ error handling
def err(status: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail})


@app.exception_handler(ApiError)
async def _api_error(_: Request, exc: ApiError):
    return err(exc.status, exc.error, exc.detail)


@app.exception_handler(RequestValidationError)
async def _validation_error(_: Request, exc: RequestValidationError):
    parts = [f"{'.'.join(str(p) for p in e.get('loc', []))}: {e.get('msg')}" for e in exc.errors()]
    return err(400, "invalid_request", "; ".join(parts) or "invalid request")


@app.exception_handler(StarletteHTTPException)
async def _http_error(_: Request, exc: StarletteHTTPException):
    names = {400: "invalid_request", 404: "not_found", 405: "method_not_allowed", 409: "conflict"}
    return err(exc.status_code, names.get(exc.status_code, "http_error"), str(exc.detail))


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception):
    log.exception("unhandled error")
    return err(500, "internal_error", f"{type(exc).__name__}: {exc}")


# --------------------------------------------------------------------- client API
@app.post("/api/v1/objects", status_code=201)
async def upload_object(request: Request, file: UploadFile = File(...),
                        metadata: Optional[str] = Form(None), object_id: Optional[str] = Form(None)):
    custom = {}
    if metadata is not None and metadata.strip() != "":
        try:
            custom = json.loads(metadata)
        except json.JSONDecodeError as exc:
            raise ApiError(400, "invalid_request", f"'metadata' is not valid JSON: {exc}")
        if not isinstance(custom, dict):
            raise ApiError(400, "invalid_request", "'metadata' must be a JSON object of key-value pairs")
    name = file.filename or "unnamed"
    ctype = file.content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
    obj = await svc(request).upload(file, name, ctype, custom, object_id or None)
    return JSONResponse(status_code=201, content=obj, headers={"Location": f"/api/v1/objects/{obj['object_id']}"})


@app.get("/api/v1/objects")
async def list_objects(request: Request, limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    return svc(request).list_objects(limit, offset)


@app.get("/api/v1/objects/{object_id}")
async def download_object(object_id: str, request: Request):
    obj, body = await svc(request).download(object_id)
    ascii_name = obj["name"].encode("ascii", "replace").decode().replace('"', "_").replace("?", "_")
    disposition = f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(obj["name"])}'
    return Response(content=body, media_type=obj["content_type"],
                    headers={"Content-Disposition": disposition, "ETag": f'"{obj["checksum"]}"'})


@app.get("/api/v1/objects/{object_id}/metadata")
async def get_metadata(object_id: str, request: Request):
    return svc(request).get_metadata(object_id)


@app.patch("/api/v1/objects/{object_id}/metadata")
async def patch_metadata(object_id: str, request: Request):
    try:
        body = await request.json()
    except Exception:
        raise ApiError(400, "invalid_request", "request body must be valid JSON")
    if not isinstance(body, dict) or not all(isinstance(k, str) for k in body):
        raise ApiError(400, "invalid_request", "request body must be a JSON object of key-value pairs")
    expected = None
    if_match = request.headers.get("if-match")
    if if_match is not None:
        try:
            expected = int(if_match.strip('"'))
        except ValueError:
            raise ApiError(400, "invalid_request", "If-Match must be the integer object version")
    # Existence is checked first so a bad/empty payload on a missing object is still a 404.
    if not svc(request).db.object_exists(object_id):
        raise ApiError(404, "not_found", f"object {object_id} does not exist")
    return svc(request).patch_metadata(object_id, body, expected)


@app.delete("/api/v1/objects/{object_id}", status_code=204)
async def delete_object(object_id: str, request: Request):
    await svc(request).delete(object_id)
    return Response(status_code=204)


# ------------------------------------------------------------------ cluster status
@app.get("/api/v1/cluster/status")
async def cluster_status(request: Request):
    return svc(request).cluster_status()


@app.get("/health")
async def health():
    return {"status": "ok"}


# ------------------------------------------------------------ internal (workers)
@app.post("/internal/workers/register")
async def register_worker(req: RegisterRequest, request: Request):
    s = svc(request)
    s.db.upsert_worker(req.worker_id, req.url.rstrip("/"), "HEALTHY", time.time(),
                       chunk_count=len(req.chunks), disk_usage=sum(c.size for c in req.chunks))
    log.info("WORKER REGISTERED worker_id=%s url=%s inventory=%d chunks", req.worker_id, req.url, len(req.chunks))
    background.schedule_reconcile(s, req.worker_id)
    return {"status": "registered", "worker_id": req.worker_id}


@app.post("/internal/workers/heartbeat")
async def heartbeat(req: HeartbeatRequest, request: Request):
    s = svc(request)
    prev = s.db.record_heartbeat(req.worker_id, time.time(), req.chunk_count, req.disk_usage)
    if prev is None:
        return err(404, "unknown_worker", "worker is not registered; please register first")
    if prev == "OFFLINE":
        log.info("WORKER BACK ONLINE worker_id=%s", req.worker_id)
        background.schedule_reconcile(s, req.worker_id)
    return {"status": "ok"}


# ------------------------------------------------------------ web UI & demo helpers
@app.get("/", include_in_schema=False)
async def web_ui():
    return FileResponse(STATIC_INDEX, media_type="text/html")


@app.get("/api/v1/events")
async def get_events(after: int = Query(0, ge=0)):
    return {"events": [e for e in events.items if e["id"] > after], "last_id": events.seq,
            "demo_mode": config.DEMO_MODE}


@app.post("/api/v1/demo/corrupt")
async def demo_corrupt(request: Request):
    """Demo only: corrupt one replica of a chunk on a worker's disk (for the data-integrity demo)."""
    if not config.DEMO_MODE:
        raise ApiError(404, "not_found", "demo endpoints are disabled (DEMO_MODE=0)")
    try:
        body = await request.json()
        chunk_id, worker_id = body["chunk_id"], body["worker_id"]
    except Exception:
        raise ApiError(400, "invalid_request", "body must be JSON with chunk_id and worker_id")
    s = svc(request)
    if worker_id not in s.db.chunk_replicas(chunk_id):
        raise ApiError(404, "not_found", f"{worker_id} holds no replica of {chunk_id}")
    r = await s.client.post(f"{s.url_of(worker_id)}/internal/debug/corrupt/{chunk_id}")
    if r.status_code != 200:
        raise ApiError(503, "worker_error", f"worker refused: {r.text[:200]}")
    log.warning("DEMO: chunk %s corrupted on disk of %s (not yet detected)", chunk_id, worker_id)
    return r.json()
