"""Mini Distributed Cloud Object Store - Storage Worker (FastAPI)."""
import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.exceptions import HTTPException as StarletteHTTPException

import config
import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(config.WORKER_ID)


def err(status: int, error: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": error, "detail": detail})


async def controller_link():
    """Registers with the controller (re-registering whenever needed) and heartbeats."""
    registered = False
    async with httpx.AsyncClient(timeout=5.0) as client:
        while True:
            try:
                if not registered:
                    inventory = await asyncio.to_thread(storage.list_chunks, False)
                    r = await client.post(f"{config.CONTROLLER_URL}/internal/workers/register", json={
                        "worker_id": config.WORKER_ID, "url": config.WORKER_URL, "chunks": inventory})
                    r.raise_for_status()
                    registered = True
                    log.info("registered with controller (%d chunks on disk)", len(inventory))
                else:
                    stats = await asyncio.to_thread(storage.usage)
                    r = await client.post(f"{config.CONTROLLER_URL}/internal/workers/heartbeat",
                                          json={"worker_id": config.WORKER_ID, **stats})
                    if r.status_code == 404:        # controller lost us -> register again
                        registered = False
                        continue
                    r.raise_for_status()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("controller unreachable (%s); will retry", exc)
            await asyncio.sleep(config.HEARTBEAT_INTERVAL if registered else 1.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage.init_storage()
    task = asyncio.create_task(controller_link())
    yield
    task.cancel()


app = FastAPI(title=f"Worker {config.WORKER_ID}", lifespan=lifespan)


@app.exception_handler(StarletteHTTPException)
async def _http_error(_: Request, exc: StarletteHTTPException):
    return err(exc.status_code, "not_found" if exc.status_code == 404 else "http_error", str(exc.detail))


@app.exception_handler(Exception)
async def _unhandled(_: Request, exc: Exception):
    log.exception("unhandled error")
    return err(500, "internal_error", f"{type(exc).__name__}: {exc}")


def _check_id(chunk_id: str):
    if not storage.valid_chunk_id(chunk_id):
        return err(400, "invalid_request", "invalid chunk_id")
    return None


@app.put("/internal/chunks/{chunk_id}")
async def put_chunk(chunk_id: str, request: Request):
    if (bad := _check_id(chunk_id)):
        return bad
    data = await request.body()
    expected = request.headers.get("x-chunk-checksum")
    if expected and storage.sha256_hex(data) != expected:
        return err(400, "checksum_mismatch", "received bytes do not match X-Chunk-Checksum")
    return await asyncio.to_thread(storage.write_chunk, chunk_id, data)


@app.get("/internal/chunks/{chunk_id}")
async def get_chunk(chunk_id: str):
    if (bad := _check_id(chunk_id)):
        return bad
    data = await asyncio.to_thread(storage.read_chunk, chunk_id)
    if data is None:
        return err(404, "not_found", f"chunk {chunk_id} not stored on {config.WORKER_ID}")
    return Response(content=data, media_type="application/octet-stream")


@app.delete("/internal/chunks/{chunk_id}")
async def delete_chunk(chunk_id: str):
    if (bad := _check_id(chunk_id)):
        return bad
    if not await asyncio.to_thread(storage.delete_chunk, chunk_id):
        return err(404, "not_found", f"chunk {chunk_id} not stored on {config.WORKER_ID}")
    return Response(status_code=204)


@app.get("/internal/workers/{worker_id}/chunks")
async def list_chunks(worker_id: str):
    if worker_id != config.WORKER_ID:
        return err(404, "not_found", f"this worker is {config.WORKER_ID}, not {worker_id}")
    chunks = await asyncio.to_thread(storage.list_chunks, True)
    return {"worker_id": config.WORKER_ID, "chunk_count": len(chunks),
            "disk_usage": sum(c["size"] for c in chunks), "chunks": chunks}


@app.post("/internal/debug/corrupt/{chunk_id}")
async def debug_corrupt(chunk_id: str):
    """Demo only: flips the first bytes of a stored chunk on disk (same size, different content)."""
    if not config.DEMO_MODE:
        return err(404, "not_found", "debug endpoints disabled")
    if (bad := _check_id(chunk_id)):
        return bad
    data = await asyncio.to_thread(storage.read_chunk, chunk_id)
    if data is None:
        return err(404, "not_found", f"chunk {chunk_id} not stored on {config.WORKER_ID}")
    n = min(16, len(data))
    broken = bytes(b ^ 0xFF for b in data[:n]) + data[n:]
    with open(storage.chunk_path(chunk_id), "r+b") as f:   # in place, like bit rot
        f.write(broken[:n])
        f.flush()
    log.warning("DEMO: corrupted chunk %s on disk", chunk_id)
    return {"chunk_id": chunk_id, "worker_id": config.WORKER_ID, "corrupted_bytes": n}


@app.get("/health")
async def health():
    return {"status": "ok", "worker_id": config.WORKER_ID}
