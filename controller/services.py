"""Core controller logic: upload, download (failover + read-repair), metadata, delete, status."""
import asyncio
import hashlib
import logging
import mimetypes
import random
import re
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import httpx

import config
from db import Database

log = logging.getLogger("controller")

OBJECT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ApiError(Exception):
    def __init__(self, status: int, error: str, detail: str):
        super().__init__(detail)
        self.status, self.error, self.detail = status, error, detail


class ObjectStore:
    def __init__(self, db: Database, client: httpx.AsyncClient):
        self.db = db
        self.client = client
        # Serialises delete / re-replication / pruning / reconciliation / scrubbing.
        self.maintenance_lock = asyncio.Lock()
        # Chunk ids written by uploads that have not yet committed their metadata.
        self.inflight: set = set()
        self.tasks: set = set()

    # ------------------------------------------------------------------ helpers
    def healthy_ids(self) -> set:
        return {w["worker_id"] for w in self.db.list_workers() if w["status"] == "HEALTHY"}

    def url_of(self, worker_id: str) -> str:
        w = self.db.get_worker(worker_id)
        if not w:
            raise RuntimeError(f"unknown worker {worker_id}")
        return w["url"]

    def spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self.tasks.add(t)
        t.add_done_callback(self.tasks.discard)
        return t

    # ---------------------------------------------------------------- worker I/O
    async def put_chunk(self, worker_id: str, chunk_id: str, data: bytes, checksum: str):
        r = await self.client.put(f"{self.url_of(worker_id)}/internal/chunks/{chunk_id}", content=data,
                                  headers={"Content-Type": "application/octet-stream",
                                           "X-Chunk-Checksum": checksum})
        r.raise_for_status()
        if r.json().get("checksum") != checksum:
            raise RuntimeError(f"worker {worker_id} stored chunk {chunk_id} with wrong checksum")

    async def get_chunk(self, worker_id: str, chunk_id: str) -> Optional[bytes]:
        r = await self.client.get(f"{self.url_of(worker_id)}/internal/chunks/{chunk_id}")
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.content

    async def delete_chunk(self, worker_id: str, chunk_id: str) -> bool:
        """True if the chunk is gone from the worker (404 counts as gone)."""
        try:
            r = await self.client.delete(f"{self.url_of(worker_id)}/internal/chunks/{chunk_id}")
            return r.status_code in (200, 204, 404)
        except Exception as exc:
            log.warning("delete of chunk %s on %s failed: %s", chunk_id, worker_id, exc)
            return False

    async def list_worker_chunks(self, worker_id: str, attempts: int = 3) -> Optional[dict]:
        last = None
        for i in range(attempts):
            try:
                r = await self.client.get(f"{self.url_of(worker_id)}/internal/workers/{worker_id}/chunks")
                r.raise_for_status()
                return {c["chunk_id"]: c for c in r.json()["chunks"]}
            except Exception as exc:
                last = exc
                await asyncio.sleep(1.0)
        log.warning("could not list chunks of %s: %s", worker_id, last)
        return None

    async def fetch_valid_chunk(self, chunk: dict, candidates: List[str]) -> Tuple[Optional[bytes], List[Tuple[str, str]]]:
        """Tries candidates in order; returns (verified bytes | None, [(worker_id, 'corrupted'|'missing')])."""
        bad: List[Tuple[str, str]] = []
        for wid in candidates:
            try:
                data = await self.get_chunk(wid, chunk["chunk_id"])
            except Exception as exc:
                log.warning("chunk %s unreadable on %s: %s", chunk["chunk_id"], wid, exc)
                continue
            if data is None:
                log.warning("MISSING CHUNK object_id=%s chunk_id=%s worker_id=%s",
                            chunk.get("object_id"), chunk["chunk_id"], wid)
                bad.append((wid, "missing"))
                continue
            actual = sha256_hex(data)
            if actual != chunk["checksum"]:
                log.warning("CORRUPTION DETECTED object_id=%s chunk_id=%s worker_id=%s expected=%s actual=%s",
                            chunk.get("object_id"), chunk["chunk_id"], wid, chunk["checksum"], actual)
                bad.append((wid, "corrupted"))
                continue
            return data, bad
        return None, bad

    async def repair_replica(self, chunk: dict, worker_id: str, data: bytes) -> bool:
        try:
            await self.put_chunk(worker_id, chunk["chunk_id"], data, chunk["checksum"])  # verifies checksum
            self.db.add_replica(chunk["chunk_id"], worker_id)
            log.info("CORRUPTION RESTORED object_id=%s chunk_id=%s worker_id=%s checksum=%s",
                     chunk.get("object_id"), chunk["chunk_id"], worker_id, chunk["checksum"])
            return True
        except Exception as exc:
            log.error("repair of chunk %s on %s failed: %s", chunk["chunk_id"], worker_id, exc)
            return False

    # ------------------------------------------------------------------- upload
    @staticmethod
    async def _read_full(file, n: int) -> bytes:
        buf = bytearray()
        while len(buf) < n:
            part = await file.read(n - len(buf))
            if not part:
                break
            buf += part
        return bytes(buf)

    async def _store_chunk(self, chunk_id, data, checksum, counts, healthy, written) -> List[str]:
        """Writes RF copies to distinct healthy workers (least-loaded first, retrying on others)."""
        rf = config.REPLICATION_FACTOR
        pending = sorted(healthy, key=lambda w: (counts.get(w, 0), random.random()))
        stored: List[str] = []
        while len(stored) < rf and pending:
            batch, pending = pending[: rf - len(stored)], pending[rf - len(stored):]

            async def attempt(w):
                try:
                    await self.put_chunk(w, chunk_id, data, checksum)
                    written.append((chunk_id, w))
                    return True
                except Exception as exc:
                    log.warning("upload: write of %s to %s failed: %s", chunk_id, w, exc)
                    return False

            for w, ok in zip(batch, await asyncio.gather(*[attempt(w) for w in batch])):
                if ok:
                    stored.append(w)
        if len(stored) < rf:
            raise ApiError(503, "insufficient_workers",
                           f"could only place {len(stored)} of {rf} replicas; not enough healthy workers")
        for w in stored:
            counts[w] = counts.get(w, 0) + 1
        return stored

    async def _rollback(self, written):
        for chunk_id, wid in written:
            if not await self.delete_chunk(wid, chunk_id):
                self.db.add_tombstone(wid, chunk_id)

    async def upload(self, file, name: str, content_type: str, custom: dict, object_id: Optional[str]) -> dict:
        rf = config.REPLICATION_FACTOR
        healthy = self.healthy_ids()
        if len(healthy) < rf:
            raise ApiError(503, "insufficient_workers",
                           f"{len(healthy)} healthy worker(s); need at least {rf} to store an object")
        if object_id is not None:
            if not OBJECT_ID_RE.match(object_id):
                raise ApiError(400, "invalid_request", "object_id must match [A-Za-z0-9][A-Za-z0-9_-]{0,63}")
            if self.db.object_exists(object_id):
                raise ApiError(409, "conflict", f"object {object_id} already exists")
        else:
            object_id = str(uuid.uuid4())

        counts = {w: n for w, n in self.db.worker_replica_counts().items() if w in healthy}
        for w in healthy:
            counts.setdefault(w, 0)
        whole = hashlib.sha256()
        total, index = 0, 0
        chunks: List[dict] = []
        written: List[Tuple[str, str]] = []
        mine: List[str] = []
        try:
            while True:
                data = await self._read_full(file, config.CHUNK_SIZE)
                if not data:
                    break
                total += len(data)
                if total > config.MAX_OBJECT_SIZE:
                    raise ApiError(400, "object_too_large", f"object exceeds {config.MAX_OBJECT_SIZE} bytes")
                whole.update(data)
                chunk_id = "chunk-" + uuid.uuid4().hex
                self.inflight.add(chunk_id)
                mine.append(chunk_id)
                checksum = sha256_hex(data)
                replicas = await self._store_chunk(chunk_id, data, checksum, counts, healthy, written)
                chunks.append({"chunk_id": chunk_id, "chunk_index": index, "size": len(data),
                               "checksum": checksum, "replicas": replicas})
                index += 1
            now = utcnow_iso()
            obj = {"object_id": object_id, "name": name, "size": total, "content_type": content_type,
                   "created_at": now, "last_modified_at": now, "checksum": whole.hexdigest(),
                   "num_chunks": len(chunks), "custom_metadata": custom}
            try:
                self.db.insert_object(obj, chunks)
            except Exception as exc:
                if self.db.object_exists(object_id):
                    raise ApiError(409, "conflict", f"object {object_id} already exists")
                raise ApiError(500, "internal_error", f"could not persist metadata: {exc}")
        except Exception as exc:
            await self._rollback(written)
            if isinstance(exc, ApiError):
                raise
            log.exception("upload failed; rolled back")
            raise ApiError(500, "internal_error", f"upload failed and was rolled back: {exc}")
        finally:
            for cid in mine:
                self.inflight.discard(cid)
        log.info("UPLOAD object_id=%s name=%s size=%d chunks=%d", object_id, name, total, len(chunks))
        return self.get_metadata(object_id)

    # ----------------------------------------------------------------- metadata
    def get_metadata(self, object_id: str) -> dict:
        obj = self.db.get_object(object_id)
        if not obj:
            raise ApiError(404, "not_found", f"object {object_id} does not exist")
        healthy = self.healthy_ids()
        out_chunks = []
        for c in self.db.get_object_chunks(object_id):
            out_chunks.append({
                "chunk_id": c["chunk_id"], "chunk_index": c["chunk_index"], "size": c["size"],
                "checksum": c["checksum"],
                "locations": [w for w in c["replicas"] if w in healthy],
                "offline_locations": [w for w in c["replicas"] if w not in healthy]})
        obj["chunks"] = out_chunks
        return obj

    def patch_metadata(self, object_id: str, updates: dict, expected_version: Optional[int]) -> dict:
        res = self.db.patch_metadata(object_id, updates, utcnow_iso(), expected_version)
        if res == "notfound":
            raise ApiError(404, "not_found", f"object {object_id} does not exist")
        if res == "conflict":
            raise ApiError(409, "conflict", "object was modified concurrently (version mismatch)")
        return self.get_metadata(object_id)

    def list_objects(self, limit: int, offset: int) -> dict:
        rows = self.db.list_objects(limit, offset)
        return {"objects": [{"object_id": r["object_id"], "name": r["name"], "size": r["size"],
                             "content_type": r["content_type"], "created_at": r["created_at"],
                             "num_chunks": r["num_chunks"]} for r in rows],
                "total": self.db.count_objects(), "limit": limit, "offset": offset}

    # ----------------------------------------------------------------- download
    async def download(self, object_id: str) -> Tuple[dict, bytes]:
        obj = self.db.get_object(object_id)
        if not obj:
            raise ApiError(404, "not_found", f"object {object_id} does not exist")
        healthy = self.healthy_ids()
        parts: List[bytes] = []
        for c in self.db.get_object_chunks(object_id):
            chunk = dict(c, object_id=object_id)
            candidates = [w for w in c["replicas"] if w in healthy]
            data, bad = await self.fetch_valid_chunk(chunk, candidates)
            if data is None:
                raise ApiError(503, "data_unavailable",
                               f"data integrity failure: no valid replica of chunk {c['chunk_id']} "
                               f"(index {c['chunk_index']}) is available "
                               f"({len(bad)} corrupted/missing, {len(candidates) - len(bad)} unreachable)")
            parts.append(data)
            for wid, _reason in bad:  # read-repair
                await self.repair_replica(chunk, wid, data)
        body = b"".join(parts)
        if sha256_hex(body) != obj["checksum"]:
            raise ApiError(500, "integrity_error", "reassembled object does not match stored checksum")
        return obj, body

    # ------------------------------------------------------------------- delete
    async def delete(self, object_id: str):
        async with self.maintenance_lock:
            held = self.db.delete_object(object_id)
            if held is None:
                raise ApiError(404, "not_found", f"object {object_id} does not exist")
            healthy = self.healthy_ids()
            for chunk_id, wid in held:
                if wid in healthy and await self.delete_chunk(wid, chunk_id):
                    continue
                self.db.add_tombstone(wid, chunk_id)   # purged when the worker returns
        log.info("DELETE object_id=%s chunks_removed=%d", object_id, len(held))

    # ------------------------------------------------------------------- status
    def cluster_status(self) -> dict:
        import time
        now = time.time()
        workers = self.db.list_workers()
        healthy = {w["worker_id"] for w in workers if w["status"] == "HEALTHY"}
        counts = self.db.worker_replica_counts()
        under = over = exact = 0
        for c in self.db.all_chunks_with_replicas():
            n = sum(1 for w in c["replicas"] if w in healthy)
            if n < config.REPLICATION_FACTOR:
                under += 1
            elif n > config.REPLICATION_FACTOR:
                over += 1
            else:
                exact += 1
        return {
            "replication_factor": config.REPLICATION_FACTOR,
            "workers": [{"worker_id": w["worker_id"], "url": w["url"], "status": w["status"],
                         "seconds_since_heartbeat": round(now - w["last_heartbeat"], 1),
                         "chunks_in_metadata": counts.get(w["worker_id"], 0),
                         "reported_chunk_count": w["chunk_count"], "disk_usage_bytes": w["disk_usage"]}
                        for w in workers],
            "replication": {"healthy_workers": len(healthy), "total_chunks": under + over + exact,
                            "fully_replicated": exact, "under_replicated": under, "over_replicated": over,
                            "replication_healthy": under == 0 and over == 0},
        }
