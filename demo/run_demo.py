#!/usr/bin/env python3
"""Automated end-to-end demonstration of the six required demos (PDF section 10).

  1 Object storage        4 Metadata
  2 Chunking/distribution 5 Worker failure (docker stop/start worker2)
  3 Replication           6 Data integrity (on-disk corruption inside a worker container)

Run:  python demo/run_demo.py [--url http://localhost:8000] [--only 1,2,3] [--extras]
Demos 5 and 6 need the `docker` CLI and the container names from docker-compose.yml.
Exit code is 0 only if every check passes.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time

import httpx

CHUNK = 1024 * 1024
FAILS = []


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def banner(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78)


def check(cond: bool, msg: str):
    print(f"  [{'PASS' if cond else 'FAIL'}] {msg}")
    if not cond:
        FAILS.append(msg)
    return cond


def wait_for(pred, timeout, desc, interval=1.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            v = pred()
            if v:
                print(f"  ... {desc} (after {time.time() - start:.0f}s)")
                return True
        except Exception:
            pass
        time.sleep(interval)
    print(f"  ... TIMEOUT waiting for: {desc}")
    return False


def docker(*args, check_rc=True):
    r = subprocess.run(["docker", *args], capture_output=True, text=True)
    if check_rc and r.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args)} failed: {r.stderr.strip()}")
    return r


class Api:
    def __init__(self, base):
        self.base = base.rstrip("/")
        self.c = httpx.Client(timeout=httpx.Timeout(120.0, connect=5.0))

    def upload(self, data: bytes, name: str, ctype="application/octet-stream", meta=None):
        form = {"metadata": json.dumps(meta)} if meta is not None else {}
        r = self.c.post(f"{self.base}/api/v1/objects", files={"file": (name, data, ctype)}, data=form)
        assert r.status_code == 201, f"upload failed: {r.status_code} {r.text}"
        return r.json()

    def download(self, oid):
        return self.c.get(f"{self.base}/api/v1/objects/{oid}")

    def meta(self, oid):
        r = self.c.get(f"{self.base}/api/v1/objects/{oid}/metadata")
        assert r.status_code == 200, r.text
        return r.json()

    def status(self):
        return self.c.get(f"{self.base}/api/v1/cluster/status").json()

    def worker_status(self, wid):
        return next(w["status"] for w in self.status()["workers"] if w["worker_id"] == wid)

    def delete(self, oid):
        return self.c.delete(f"{self.base}/api/v1/objects/{oid}")

    def all_objects(self):
        out, offset = [], 0
        while True:
            page = self.c.get(f"{self.base}/api/v1/objects", params={"limit": 1000, "offset": offset}).json()
            out += page["objects"]
            offset += 1000
            if offset >= page["total"]:
                return out


def print_chunks(meta):
    print(f"  object_id={meta['object_id']} size={meta['size']} num_chunks={meta['num_chunks']}")
    print("  idx  chunk_id                                  size      locations")
    for c in meta["chunks"]:
        print(f"  {c['chunk_index']:>3}  {c['chunk_id']:<40} {c['size']:>8}  {c['locations']}")


def print_cluster(api, label):
    st = api.status()
    print(f"  [{label}] " + ", ".join(f"{w['worker_id']}={w['status']}({w['chunks_in_metadata']} chunks)" for w in st["workers"]))
    print(f"  [{label}] replication: {st['replication']}")
    return st


def replication_ok(api):
    r = api.status()["replication"]
    return r["under_replicated"] == 0 and r["over_replicated"] == 0


def all_objects_exact_rf(api, rf=2):
    for o in api.all_objects():
        for c in api.meta(o["object_id"])["chunks"]:
            if len(c["locations"]) != rf or len(set(c["locations"])) != rf:
                return False
    return True


def worker_file_sha(worker, chunk_id):
    r = docker("exec", worker, "sha256sum", f"/data/chunks/{chunk_id}")
    return r.stdout.split()[0]


def corrupt_file(worker, chunk_id):
    code = ("p='/data/chunks/%s'; b=bytearray(open(p,'rb').read()); "
            "b[0:16]=bytes(x^0xFF for x in b[0:16]); open(p,'wb').write(b)" % chunk_id)
    docker("exec", worker, "python", "-c", code)


def controller_logs():
    r = docker("logs", "controller", check_rc=False)
    return r.stdout + r.stderr


# ----------------------------------------------------------------------------- demos
def demo1(api):
    banner("DEMO 1 - Object Storage: upload + download, byte-for-byte verification")
    png = b"\x89PNG\r\n\x1a\n" + os.urandom(300_000)   # arbitrary binary with a PNG signature
    m = api.upload(png, "picture.png", "image/png")
    print(f"  uploaded {m['name']} -> object_id={m['object_id']} (201)")
    r = api.download(m["object_id"])
    check(r.status_code == 200, "GET /api/v1/objects/{id} -> 200")
    check(sha(r.content) == sha(png), f"downloaded SHA-256 equals original ({sha(png)[:16]}...)")
    check(r.headers["content-type"].startswith("image/png"), f"Content-Type preserved ({r.headers['content-type']})")
    check("picture.png" in r.headers.get("content-disposition", ""), "Content-Disposition carries filename")
    check(m["checksum"] == sha(png), "object checksum in metadata == SHA-256 of original")
    check(api.download("does-not-exist").status_code == 404, "unknown object -> 404")
    for label, data in (("0-byte file", b""), ("exactly 1 MB", os.urandom(CHUNK)), ("2 MB + 1 byte", os.urandom(2 * CHUNK + 1))):
        e = api.upload(data, "edge.bin")
        ok = sha(api.download(e["object_id"]).content) == sha(data)
        expected_chunks = (len(data) + CHUNK - 1) // CHUNK
        check(ok and e["num_chunks"] == expected_chunks, f"edge case {label}: round-trip OK, {e['num_chunks']} chunk(s)")
        api.delete(e["object_id"])
    api.delete(m["object_id"])


def demo2(api):
    banner("DEMO 2 - Chunking and Distribution: 3.5 MB object -> 4 chunks")
    data = os.urandom(int(3.5 * CHUNK))
    up = api.upload(data, "video-like.bin")
    m = api.meta(up["object_id"])
    print_chunks(m)
    sizes = [c["size"] for c in m["chunks"]]
    check(m["num_chunks"] == 4, "num_chunks == 4")
    check(sizes == [CHUNK, CHUNK, CHUNK, CHUNK // 2], f"chunk sizes {sizes} (3 x 1 MB + 0.5 MB)")
    check([c["chunk_index"] for c in m["chunks"]] == [0, 1, 2, 3], "chunk order preserved (0..3)")
    check(len({c["chunk_id"] for c in m["chunks"]}) == 4, "chunk ids are unique")
    used = sorted({w for c in m["chunks"] for w in c["locations"]})
    print(f"  workers used: {used}")
    check(len(used) >= 3, "chunks are distributed across multiple workers")
    check(sha(api.download(m["object_id"]).content) == sha(data), "reassembled object matches original")
    return m


def demo3(api, m):
    banner("DEMO 3 - Replication: every chunk has 2 copies on 2 different workers")
    m = api.meta(m["object_id"])
    for c in m["chunks"]:
        print(f"  chunk {c['chunk_index']} ({c['chunk_id'][:18]}...) stored on {c['locations']}")
        check(len(c["locations"]) == 2 and len(set(c["locations"])) == 2, f"chunk {c['chunk_index']}: 2 copies on 2 distinct workers")
    st = print_cluster(api, "cluster")
    check(st["replication"]["replication_healthy"], "cluster reports replication healthy")
    api.delete(m["object_id"])


def demo4(api):
    banner("DEMO 4 - Metadata: upload, read, patch without re-uploading")
    data = os.urandom(1_500_000)
    meta0 = {"owner": "alice", "project": "cloud-computing", "tags": ["demo", "binary"]}
    up = api.upload(data, "report.bin", "application/octet-stream", meta0)
    oid = up["object_id"]
    m1 = api.meta(oid)
    print("  system fields:", {k: m1[k] for k in ("object_id", "name", "size", "content_type", "created_at", "last_modified_at", "checksum", "num_chunks")})
    print("  custom metadata:", m1["custom_metadata"])
    check(m1["custom_metadata"] == meta0, "custom metadata stored and returned")
    time.sleep(0.05)
    r = api.c.patch(f"{api.base}/api/v1/objects/{oid}/metadata", json={"project": "cloud-computing-v2", "reviewed": True})
    check(r.status_code == 200, "PATCH metadata -> 200")
    m2 = api.meta(oid)
    print("  after PATCH:", m2["custom_metadata"])
    check(m2["custom_metadata"] == {**meta0, "project": "cloud-computing-v2", "reviewed": True}, "keys added/updated, others kept")
    check(m2["last_modified_at"] > m1["last_modified_at"], "last_modified_at advanced")
    check(m2["created_at"] == m1["created_at"], "created_at unchanged")
    check(m2["checksum"] == m1["checksum"] and [c["chunk_id"] for c in m2["chunks"]] == [c["chunk_id"] for c in m1["chunks"]],
          "object checksum and chunk ids unchanged (no re-upload)")
    check(sha(api.download(oid).content) == sha(data), "object bytes unchanged")
    bad = api.c.patch(f"{api.base}/api/v1/objects/{oid}/metadata", content="{not json", headers={"Content-Type": "application/json"})
    check(bad.status_code == 400, "PATCH with malformed JSON -> 400 (not 422)")
    check(api.c.patch(f"{api.base}/api/v1/objects/nope/metadata", json={"a": 1}).status_code == 404, "PATCH unknown object -> 404")
    check(api.c.post(f"{api.base}/api/v1/objects", data={"metadata": "{}"}).status_code == 400, "upload without file -> 400")
    lst = api.c.get(f"{api.base}/api/v1/objects", params={"limit": 5}).json()
    check(all({"object_id", "name", "size", "content_type"} <= set(o) for o in lst["objects"]), "list endpoint returns required fields")
    d = api.delete(oid)
    check(d.status_code == 204 and api.delete(oid).status_code == 404, "DELETE -> 204, then 404")


def demo5(api, extras):
    banner("DEMO 5 - Worker Failure: docker stop worker2 / docker start worker2")
    victim = "worker2"
    kept, target, tdata = [], None, None
    for i in range(6):   # make sure at least one object has chunks on worker2
        data = os.urandom(4 * CHUNK + 12345)
        up = api.upload(data, f"failover-{i}.bin")
        kept.append(up["object_id"])
        m = api.meta(up["object_id"])
        if target is None and any(victim in c["locations"] for c in m["chunks"]):
            target, tdata = m, data
        if target and i >= 1:
            break
    print("--- BEFORE failure")
    print_cluster(api, "before")
    print_chunks(target)
    check(replication_ok(api), "before: every chunk has exactly 2 healthy replicas")

    print("--- DURING failure: docker stop worker2")
    docker("stop", victim)
    check(wait_for(lambda: api.worker_status(victim) == "OFFLINE", 40, f"{victim} detected OFFLINE"), f"{victim} marked OFFLINE")
    r = api.download(target["object_id"])
    check(r.status_code == 200 and sha(r.content) == sha(tdata), "object with chunks on worker2 downloads OK from surviving replicas")
    check(wait_for(lambda: replication_ok(api), 90, "automatic re-replication restored RF=2 on remaining workers", 2),
          "replication factor restored to 2 without worker2")
    m_now = api.meta(target["object_id"])
    print_chunks(m_now)
    check(all(victim not in c["locations"] and len(c["locations"]) == 2 for c in m_now["chunks"]),
          "all chunks have 2 healthy copies, none on worker2")
    print_cluster(api, "during")

    print("--- AFTER: docker start worker2")
    docker("start", victim)
    check(wait_for(lambda: api.worker_status(victim) == "HEALTHY", 60, f"{victim} re-registered and HEALTHY"), f"{victim} rejoined")
    check(wait_for(lambda: replication_ok(api) and all_objects_exact_rf(api), 90, "reconciled: excess replicas pruned, exactly RF=2 everywhere", 2),
          "after rejoin every chunk has exactly 2 copies on 2 distinct workers")
    st = print_cluster(api, "after")
    counts = [w["chunks_in_metadata"] for w in st["workers"]]
    print(f"  per-worker chunk counts: {counts}")
    check(min(counts) > 0 and max(counts) - min(counts) <= max(3, sum(counts) // 6), "chunks remain balanced across all 4 workers")
    r = api.download(target["object_id"])
    check(r.status_code == 200 and sha(r.content) == sha(tdata), "object still downloads correctly after rejoin")

    if extras:
        print("--- EXTRA: object deleted while a worker is offline is purged on rejoin")
        up = api.upload(os.urandom(3 * CHUNK), "tombstone.bin")
        m = api.meta(up["object_id"])
        holder = m["chunks"][0]["locations"][0]
        cid = m["chunks"][0]["chunk_id"]
        docker("stop", holder)
        wait_for(lambda: api.worker_status(holder) == "OFFLINE", 40, f"{holder} OFFLINE")
        check(api.delete(up["object_id"]).status_code == 204, "DELETE while a replica holder is offline -> 204")
        docker("start", holder)
        wait_for(lambda: api.worker_status(holder) == "HEALTHY", 60, f"{holder} HEALTHY")
        gone = wait_for(lambda: docker("exec", holder, "sh", "-c", f"test ! -e /data/chunks/{cid}", check_rc=False).returncode == 0,
                        60, "orphaned chunk purged from rejoined worker")
        check(gone, "orphaned chunk deleted from the rejoined worker's disk")
        wait_for(lambda: replication_ok(api), 60, "replication healthy again", 2)
    for oid in kept:
        api.delete(oid)


def demo6(api, extras, skip_scrub):
    banner("DEMO 6 - Data Integrity: intentional on-disk corruption")
    data = os.urandom(2 * CHUNK + 4096)
    up = api.upload(data, "integrity.bin")
    m = api.meta(up["object_id"])
    chunk = m["chunks"][1]
    worker = chunk["locations"][0]
    cid = chunk["chunk_id"]
    print(f"  target: chunk {chunk['chunk_index']} ({cid}) on {worker}; expected sha256={chunk['checksum']}")
    before = worker_file_sha(worker, cid)
    check(before == chunk["checksum"], "disk file hash before corruption == metadata checksum")
    corrupt_file(worker, cid)
    corrupted = worker_file_sha(worker, cid)
    check(corrupted != before, f"corrupted the file on disk ({before[:12]}... -> {corrupted[:12]}...)")
    r = api.download(m["object_id"])
    check(r.status_code == 200 and sha(r.content) == sha(data), "download succeeded and returned the ORIGINAL bytes (served from healthy replica)")
    logs = controller_logs()
    check("CORRUPTION DETECTED" in logs and cid in logs, "controller logged CORRUPTION DETECTED for the chunk")
    healed = wait_for(lambda: worker_file_sha(worker, cid) == before, 15, "read-repair rewrote the chunk", 0.5)
    check(healed, f"corrupted chunk on {worker} restored (disk hash back to {before[:12]}...)")
    check("CORRUPTION RESTORED" in controller_logs(), "controller logged CORRUPTION RESTORED")

    if not skip_scrub:
        print("--- background scrubber: corrupt again, do NOT download, wait for scrub cycle")
        corrupt_file(worker, cid)
        check(worker_file_sha(worker, cid) != before, "chunk corrupted again")
        ok = wait_for(lambda: worker_file_sha(worker, cid) == before, 75, "scrubber restored chunk without a client read", 2)
        check(ok, "scrubber self-healed the corrupted chunk")

    if extras:
        print("--- EXTRA: all replicas corrupt -> 503, never corrupted data")
        for w in chunk["locations"]:
            corrupt_file(w, cid)
        r = api.download(m["object_id"])
        check(r.status_code == 503, f"all replicas corrupt -> 503 ({r.json().get('error')})")
    api.delete(m["object_id"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("OBJSTORE_URL", "http://localhost:8000"))
    ap.add_argument("--only", default="1,2,3,4,5,6", help="comma-separated demo numbers")
    ap.add_argument("--extras", action="store_true", help="also test tombstones and all-replicas-corrupt (503)")
    ap.add_argument("--skip-scrub", action="store_true", help="skip the (30s) background scrubber wait in demo 6")
    a = ap.parse_args()
    only = {int(x) for x in a.only.split(",")}
    api = Api(a.url)
    banner("Waiting for cluster (controller + 4 healthy workers)")
    if not wait_for(lambda: api.status()["replication"]["healthy_workers"] == 4, 90, "4 workers HEALTHY", 2):
        sys.exit("cluster is not ready - run: docker compose up --build -d")
    m2 = None
    if 1 in only: demo1(api)
    if 2 in only or 3 in only: m2 = demo2(api)
    if 3 in only: demo3(api, m2)
    elif m2: api.delete(m2["object_id"])
    if 4 in only: demo4(api)
    if 5 in only: demo5(api, a.extras)
    if 6 in only: demo6(api, a.extras, a.skip_scrub)
    banner("SUMMARY")
    if FAILS:
        print(f"  {len(FAILS)} check(s) FAILED:")
        for f in FAILS:
            print("   -", f)
        sys.exit(1)
    print("  ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
