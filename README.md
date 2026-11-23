# Mini Distributed Cloud Object Store

1 controller + 4 workers + CLI/demo clients, run locally with **Docker Compose** (no Kubernetes).
Objects are split into **1 MB (1,048,576-byte)** chunks, each stored on **2 different workers** (RF=2),
with SHA-256 integrity checking, self-healing re-replication and automatic corruption repair.

```
client ──REST──▶ controller (FastAPI + SQLite/WAL metadata, :8000)
                    │  register / heartbeat (3s)  ▲        PUT/GET/DELETE /internal/chunks/{id}
                    ▼                             │
         worker1   worker2   worker3   worker4    (each persists chunks in ./storage/workerN → /data/chunks)
```

## Build, start, stop
```bash
docker compose up --build -d          # build and start controller + worker1..4
docker ps --format '{{.Names}}'       # controller, worker1, worker2, worker3, worker4
curl -s localhost:8000/api/v1/cluster/status | python3 -m json.tool   # all 4 workers HEALTHY

docker compose down                   # stop (data stays in ./storage)
rm -rf storage                        # (optional) wipe all data; may need sudo, files are root-owned
```
Persistent data: `./storage/worker1..4` (raw chunk files named by chunk id) and `./storage/controller/metadata.db`.

## Web console (easiest way to test and demo)
Open **http://localhost:8000/** after `docker compose up --build -d`. It shows:
* **Cluster**: live worker health (HEALTHY/OFFLINE), chunks per worker, and under/over-replicated counts, refreshed every 2 s.
* **Upload**: drag and drop a file, edit custom metadata JSON, or use quick random test data (0 B, 0.5 MB, 1 MB, 3.5 MB, 10 MB).
* **Objects**: click one to see system metadata, a **chunk map** (every chunk's replicas, colour-coded per worker, stale copies on offline workers struck through), and a metadata editor that sends `PATCH`.
* **Download & verify**: downloads the object and checks its SHA-256 in the browser against the stored checksum.
* **Live events**: OFFLINE detection, RE-REPLICATION, PRUNED, `CORRUPTION DETECTED` / `CORRUPTION RESTORED`.
* The top-right chip shows the last API call and its HTTP status.

Demo flows: **integrity**: click ⚡ on a replica (corrupts that chunk file on the worker's disk), then *Download & verify*; the download still matches and the events show detect → restore. **Failure**: run `docker stop worker2` in a terminal and watch the worker card, the chunk map and the events; then `docker start worker2`.
The ⚡ button uses demo-only endpoints (`POST /api/v1/demo/corrupt`, worker `POST /internal/debug/corrupt/{id}`); set `DEMO_MODE=0` on the controller to disable them. Swagger is also available at `/docs`.

## Use it with curl
```bash
# upload (multipart: file + optional JSON metadata)
curl -s -F 'file=@photo.png' -F 'metadata={"owner":"alice","project":"demo"}' localhost:8000/api/v1/objects
OID=<object_id from the response>

curl -s -o out.png -D - localhost:8000/api/v1/objects/$OID               # download
curl -s localhost:8000/api/v1/objects/$OID/metadata | python3 -m json.tool   # metadata + chunk locations
curl -s -X PATCH -H 'Content-Type: application/json' \
     -d '{"project":"demo-v2","reviewed":true}' localhost:8000/api/v1/objects/$OID/metadata
curl -s 'localhost:8000/api/v1/objects?limit=10&offset=0'                # list
curl -s -X DELETE -w '%{http_code}\n' localhost:8000/api/v1/objects/$OID  # 204
```
A `null` value in a PATCH body removes that key. An optional `If-Match: <version>` header on PATCH gives
optimistic concurrency (`409` on mismatch); an optional `object_id` form field on upload gives `409` on duplicates.

## Use it with the CLI
```bash
pip install -r client/requirements.txt
python client/cli.py upload photo.png --meta '{"owner":"alice"}'
python client/cli.py list
python client/cli.py metadata <object_id>
python client/cli.py patch <object_id> '{"project":"demo-v2"}'
python client/cli.py download <object_id> -o copy.png
python client/cli.py delete <object_id>
python client/cli.py status
```

## Run the automated demonstration (all 6 required demos)
```bash
pip install -r demo/requirements.txt
python demo/run_demo.py                # full run, ~3-4 minutes; exit code 0 only if every check passes
python demo/run_demo.py --extras       # + deleted-while-offline tombstone test and all-replicas-corrupt (503) test
python demo/run_demo.py --only 1,2,3,4 # demos that need no docker access
```
Run it from a host that has the `docker` CLI (demos 5 and 6 execute `docker stop/start worker2` and
`docker exec` to corrupt chunk files inside the worker containers). Start from a healthy cluster.

| # | Demo | What `run_demo.py` proves |
|---|------|---------------------------|
| 1 | Object storage | PNG-signature binary upload/download, SHA-256 equality, Content-Type/Disposition, 404; 0-byte, 1 MB, 2 MB+1 edge cases |
| 2 | Chunking & distribution | 3.5 MB → 4 chunks (3×1 MB + 0.5 MB), ordered `chunk_index`, unique ids, spread over workers |
| 3 | Replication | every chunk listed with 2 copies on 2 distinct workers |
| 4 | Metadata | custom metadata, `GET .../metadata`, `PATCH` without re-upload, checksum/bytes unchanged, 400 for bad JSON |
| 5 | Worker failure | state before → `docker stop worker2` → OFFLINE detected → download still works → RF=2 restored on other workers → `docker start worker2` → rejoin, reconcile, excess replicas pruned back to exactly RF=2, balanced |
| 6 | Data integrity | corrupt a chunk file on a worker's disk → download returns original bytes → `CORRUPTION DETECTED`/`RESTORED` logged → disk file hash restored; then the background scrubber heals a second corruption with no download |

### Manual failure / corruption walkthrough
```bash
docker stop worker2        # wait ~10s: status shows worker2 OFFLINE, re-replication follows within ~4s
curl -s localhost:8000/api/v1/cluster/status | python3 -m json.tool
docker start worker2       # re-registers, reconciles, excess replicas pruned

# corrupt a chunk on worker1 (pick an id from the metadata endpoint)
docker exec worker1 python -c "p='/data/chunks/<chunk_id>';b=bytearray(open(p,'rb').read());b[0:16]=bytes(x^255 for x in b[0:16]);open(p,'wb').write(b)"
curl -s -o /dev/null -w '%{http_code}\n' localhost:8000/api/v1/objects/$OID   # 200, correct bytes
docker logs controller 2>&1 | grep CORRUPTION
```

## Design summary
* **Metadata (SQLite, WAL, FKs on)**: `objects`, `chunks`, `replicas`, `workers`, `tombstones`. Upload metadata is committed in one transaction only after all chunks are stored.
* **Upload**: streams 1 MB chunks, computes per-chunk and whole-object SHA-256, places each chunk on the 2 least-loaded distinct healthy workers (falls back to other workers if a write fails). Any failure deletes already-written chunks and persists nothing (`503` if <2 healthy workers, otherwise `500`).
* **Download**: each chunk is fetched from a healthy replica and verified; on mismatch/missing it fails over, logs `CORRUPTION DETECTED`, and read-repairs the bad copy (`CORRUPTION RESTORED`). If no valid replica exists → `503`. The whole object is verified and buffered before sending, so a client never receives corrupted bytes; very large downloads are therefore memory-bound on the controller.
* **Failure detection**: heartbeat every 3 s; monitor runs every 2 s; OFFLINE after >9 s silence. Replication loop every 4 s restores RF=2 from a verified copy onto the least-loaded eligible worker.
* **Rejoin**: worker re-registers; controller lists its disk (`GET /internal/workers/{id}/chunks`), purges tombstoned/orphaned chunks, re-indexes valid ones, restores corrupt ones from peers; the replication loop prunes any chunk with >2 healthy replicas (most-loaded worker first).
* **Scrubber**: every 30 s (`SCRUB_INTERVAL_SECONDS`) each healthy worker's disk checksums are compared with metadata; corrupt replicas are restored, orphans removed.
* **Concurrency**: delete / re-replication / pruning / reconcile / scrub are serialised by one asyncio lock; in-flight upload chunks are excluded from orphan cleanup.
* **Errors**: every error is `{"error": "...", "detail": "..."}`; request-validation errors return `400` (not FastAPI's 422).
* Extra read-only endpoint: `GET /api/v1/cluster/status` (worker health, per-worker chunk counts, under/over-replicated counts).

Timing/size knobs (env vars in `docker-compose.yml`): `CHUNK_SIZE`, `REPLICATION_FACTOR`, `HEARTBEAT_TIMEOUT_SECONDS`, `HEALTH_CHECK_INTERVAL_SECONDS`, `REPLICATION_INTERVAL_SECONDS`, `SCRUB_INTERVAL_SECONDS`, `MAX_OBJECT_SIZE`.
