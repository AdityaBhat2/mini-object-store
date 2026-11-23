"""Background loops: health monitor, re-replicator (+pruning), reconciler, integrity scrubber."""
import asyncio
import logging
import time

import config
from services import ObjectStore

log = logging.getLogger("controller")


# ------------------------------------------------------------------ health monitor
async def health_pass(svc: ObjectStore):
    now = time.time()
    for w in svc.db.list_workers():
        if w["status"] == "HEALTHY" and now - w["last_heartbeat"] > config.HEARTBEAT_TIMEOUT:
            svc.db.set_worker_status(w["worker_id"], "OFFLINE")
            log.warning("WORKER OFFLINE worker_id=%s (no heartbeat for %.1fs)", w["worker_id"], now - w["last_heartbeat"])


# ------------------------------------------------------- re-replication & pruning
async def replication_pass(svc: ObjectStore):
    rf = config.REPLICATION_FACTOR
    async with svc.maintenance_lock:
        healthy = svc.healthy_ids()
        if not healthy:
            return
        counts = {w: 0 for w in healthy}
        for w, n in svc.db.worker_replica_counts().items():
            if w in healthy:
                counts[w] = n
        for chunk in svc.db.all_chunks_with_replicas():
            hr = [w for w in chunk["replicas"] if w in healthy]
            if len(hr) < rf:
                await _restore_chunk(svc, chunk, hr, healthy, counts)
            elif len(hr) > rf:
                await _prune_chunk(svc, chunk, hr, counts)


async def _restore_chunk(svc, chunk, hr, healthy, counts):
    rf = config.REPLICATION_FACTOR
    data, bad = await svc.fetch_valid_chunk(chunk, hr)
    if data is None:
        log.error("UNDER-REPLICATED chunk_id=%s object_id=%s: no valid replica on healthy workers "
                  "(%d healthy copies listed)", chunk["chunk_id"], chunk["object_id"], len(hr))
        return
    valid = [w for w in hr if w not in {b[0] for b in bad}]
    for wid, _ in bad:                      # heal corrupt/missing copies on healthy workers
        if await svc.repair_replica(chunk, wid, data):
            valid.append(wid)
    targets = sorted((w for w in healthy if w not in chunk["replicas"]), key=lambda w: (counts.get(w, 0), w))
    for target in targets:
        if len(valid) >= rf:
            break
        try:
            await svc.put_chunk(target, chunk["chunk_id"], data, chunk["checksum"])
        except Exception as exc:
            log.warning("re-replication of %s to %s failed: %s", chunk["chunk_id"], target, exc)
            continue
        svc.db.add_replica(chunk["chunk_id"], target)
        counts[target] = counts.get(target, 0) + 1
        valid.append(target)
        log.info("RE-REPLICATION chunk_id=%s object_id=%s copied to %s (healthy replicas now %d)",
                 chunk["chunk_id"], chunk["object_id"], target, len(valid))
    if len(valid) < rf:
        log.warning("chunk %s still under-replicated (%d/%d): not enough eligible workers",
                    chunk["chunk_id"], len(valid), rf)


async def _prune_chunk(svc, chunk, hr, counts):
    excess = len(hr) - config.REPLICATION_FACTOR
    for wid in sorted(hr, key=lambda w: (-counts.get(w, 0), w))[:excess]:
        if not await svc.delete_chunk(wid, chunk["chunk_id"]):
            svc.db.add_tombstone(wid, chunk["chunk_id"])
        svc.db.remove_replica(chunk["chunk_id"], wid)
        counts[wid] = max(0, counts.get(wid, 0) - 1)
        log.info("PRUNED excess replica chunk_id=%s worker_id=%s (converging to RF=%d)",
                 chunk["chunk_id"], wid, config.REPLICATION_FACTOR)


# ------------------------------------------------- reconciliation & integrity scrub
async def sync_worker(svc: ObjectStore, worker_id: str, reason: str = "reconcile"):
    """Compares a worker's disk with controller metadata and fixes every difference.

    Used both when a worker (re)joins ("reconcile") and by the periodic scrubber ("scrub").
    """
    async with svc.maintenance_lock:
        worker = svc.db.get_worker(worker_id)
        if not worker or worker["status"] != "HEALTHY":
            return
        listing = await svc.list_worker_chunks(worker_id)
        if listing is None:
            return
        healthy = svc.healthy_ids()
        orphans = restored = reindexed = dropped = purged = 0

        # 1. pending tombstones (objects deleted while the worker was offline)
        for chunk_id in svc.db.list_tombstones(worker_id):
            if svc.db.get_chunk(chunk_id) is None and chunk_id in listing:
                if await svc.delete_chunk(worker_id, chunk_id):
                    purged += 1
                    listing.pop(chunk_id, None)
                else:
                    continue
            svc.db.remove_tombstone(worker_id, chunk_id)

        # 2. walk what is on disk
        for chunk_id, info in listing.items():
            if chunk_id in svc.inflight:
                continue
            chunk = svc.db.get_chunk(chunk_id)
            if chunk is None:
                if chunk_id in svc.inflight or svc.db.get_chunk(chunk_id) is not None:
                    continue
                if await svc.delete_chunk(worker_id, chunk_id):
                    orphans += 1
                continue
            if info.get("checksum") == chunk["checksum"] and info.get("size") == chunk["size"]:
                if worker_id not in svc.db.chunk_replicas(chunk_id):
                    svc.db.add_replica(chunk_id, worker_id)
                    reindexed += 1
                continue
            # corrupted on disk -> restore from a healthy peer
            log.warning("CORRUPTION DETECTED (%s) object_id=%s chunk_id=%s worker_id=%s expected=%s actual=%s",
                        reason, chunk["object_id"], chunk_id, worker_id, chunk["checksum"], info.get("checksum"))
            peers = [w for w in svc.db.chunk_replicas(chunk_id) if w != worker_id and w in healthy]
            data, _bad = await svc.fetch_valid_chunk(chunk, peers)
            if data is not None and await svc.repair_replica(chunk, worker_id, data):
                restored += 1
            else:
                svc.db.remove_replica(chunk_id, worker_id)
                log.error("no valid peer copy to restore chunk_id=%s on %s", chunk_id, worker_id)

        # 3. metadata says the worker has it, but the file is gone
        for chunk_id in svc.db.worker_replica_ids(worker_id):
            if chunk_id not in listing:
                svc.db.remove_replica(chunk_id, worker_id)
                dropped += 1
        if any((orphans, restored, reindexed, dropped, purged)) or reason == "reconcile":
            log.info("SYNC(%s) worker_id=%s on_disk=%d reindexed=%d orphans_deleted=%d tombstones_purged=%d "
                     "corrupt_restored=%d stale_rows_dropped=%d",
                     reason, worker_id, len(listing), reindexed, orphans, purged, restored, dropped)


async def scrub_pass(svc: ObjectStore):
    for w in sorted(svc.healthy_ids()):
        await sync_worker(svc, w, reason="scrub")


# ------------------------------------------------------------------------ runner
async def _loop(name, interval, fn, svc):
    while True:
        try:
            await asyncio.sleep(interval)
            await fn(svc)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("background task %s failed; continuing", name)


def schedule_reconcile(svc: ObjectStore, worker_id: str):
    svc.spawn(sync_worker(svc, worker_id, "reconcile"))


def start_all(svc: ObjectStore):
    return [asyncio.create_task(_loop("health", config.HEALTH_CHECK_INTERVAL, health_pass, svc)),
            asyncio.create_task(_loop("replication", config.REPLICATION_INTERVAL, replication_pass, svc)),
            asyncio.create_task(_loop("scrubber", config.SCRUB_INTERVAL, scrub_pass, svc))]
