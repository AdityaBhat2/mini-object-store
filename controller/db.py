"""SQLite (WAL, foreign keys) metadata store.

One shared connection guarded by an RLock (autocommit mode, explicit BEGIN/COMMIT for
multi-statement transactions). Every query is a short local call, so it is safe to call
from the asyncio event loop.
"""
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Dict, List, Optional, Set, Tuple

SCHEMA = """
CREATE TABLE IF NOT EXISTS workers(
  worker_id TEXT PRIMARY KEY, url TEXT NOT NULL, status TEXT NOT NULL,
  last_heartbeat REAL NOT NULL, registered_at REAL NOT NULL,
  chunk_count INTEGER NOT NULL DEFAULT 0, disk_usage INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS objects(
  object_id TEXT PRIMARY KEY, name TEXT NOT NULL, size INTEGER NOT NULL,
  content_type TEXT NOT NULL, created_at TEXT NOT NULL, last_modified_at TEXT NOT NULL,
  checksum TEXT NOT NULL, num_chunks INTEGER NOT NULL,
  custom_metadata TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 1);
CREATE TABLE IF NOT EXISTS chunks(
  chunk_id TEXT PRIMARY KEY,
  object_id TEXT NOT NULL REFERENCES objects(object_id) ON DELETE CASCADE,
  chunk_index INTEGER NOT NULL, size INTEGER NOT NULL, checksum TEXT NOT NULL,
  UNIQUE(object_id, chunk_index));
CREATE TABLE IF NOT EXISTS replicas(
  chunk_id TEXT NOT NULL REFERENCES chunks(chunk_id) ON DELETE CASCADE,
  worker_id TEXT NOT NULL, PRIMARY KEY(chunk_id, worker_id));
CREATE INDEX IF NOT EXISTS idx_replicas_worker ON replicas(worker_id);
CREATE TABLE IF NOT EXISTS tombstones(
  worker_id TEXT NOT NULL, chunk_id TEXT NOT NULL, created_at REAL NOT NULL,
  PRIMARY KEY(worker_id, chunk_id));
"""


class Database:
    def __init__(self, path: str):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)

    def close(self):
        with self._lock:
            self._conn.close()

    # -- helpers ---------------------------------------------------------
    def _q(self, sql: str, params=()) -> List[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def _x(self, sql: str, params=()) -> int:
        with self._lock:
            return self._conn.execute(sql, params).rowcount

    @contextmanager
    def _tx(self):
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # -- workers ---------------------------------------------------------
    def upsert_worker(self, worker_id, url, status, now, chunk_count=0, disk_usage=0):
        self._x(
            """INSERT INTO workers(worker_id,url,status,last_heartbeat,registered_at,chunk_count,disk_usage)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(worker_id) DO UPDATE SET url=excluded.url,status=excluded.status,
                 last_heartbeat=excluded.last_heartbeat,chunk_count=excluded.chunk_count,
                 disk_usage=excluded.disk_usage""",
            (worker_id, url, status, now, now, chunk_count, disk_usage))

    def get_worker(self, worker_id) -> Optional[dict]:
        rows = self._q("SELECT * FROM workers WHERE worker_id=?", (worker_id,))
        return dict(rows[0]) if rows else None

    def list_workers(self) -> List[dict]:
        return [dict(r) for r in self._q("SELECT * FROM workers ORDER BY worker_id")]

    def record_heartbeat(self, worker_id, now, chunk_count, disk_usage) -> Optional[str]:
        """Returns the previous status, or None if the worker is unknown."""
        with self._tx() as c:
            row = c.execute("SELECT status FROM workers WHERE worker_id=?", (worker_id,)).fetchone()
            if not row:
                return None
            c.execute("UPDATE workers SET last_heartbeat=?,chunk_count=?,disk_usage=?,status='HEALTHY' WHERE worker_id=?",
                      (now, chunk_count, disk_usage, worker_id))
            return row["status"]

    def set_worker_status(self, worker_id, status):
        self._x("UPDATE workers SET status=? WHERE worker_id=?", (status, worker_id))

    def mark_all_workers_offline(self):
        self._x("UPDATE workers SET status='OFFLINE'")

    def worker_replica_counts(self) -> Dict[str, int]:
        return {r["worker_id"]: r["n"] for r in
                self._q("SELECT worker_id, COUNT(*) AS n FROM replicas GROUP BY worker_id")}

    # -- objects ---------------------------------------------------------
    @staticmethod
    def _obj(row) -> dict:
        d = dict(row)
        d["custom_metadata"] = json.loads(d["custom_metadata"])
        return d

    def object_exists(self, object_id) -> bool:
        return bool(self._q("SELECT 1 FROM objects WHERE object_id=?", (object_id,)))

    def insert_object(self, obj: dict, chunks: List[dict]):
        """chunks: [{chunk_id, chunk_index, size, checksum, replicas:[worker_id,...]}] (atomic)."""
        with self._tx() as c:
            c.execute(
                """INSERT INTO objects(object_id,name,size,content_type,created_at,last_modified_at,
                   checksum,num_chunks,custom_metadata,version) VALUES(?,?,?,?,?,?,?,?,?,1)""",
                (obj["object_id"], obj["name"], obj["size"], obj["content_type"], obj["created_at"],
                 obj["last_modified_at"], obj["checksum"], obj["num_chunks"],
                 json.dumps(obj["custom_metadata"])))
            for ch in chunks:
                c.execute("INSERT INTO chunks(chunk_id,object_id,chunk_index,size,checksum) VALUES(?,?,?,?,?)",
                          (ch["chunk_id"], obj["object_id"], ch["chunk_index"], ch["size"], ch["checksum"]))
                for w in ch["replicas"]:
                    c.execute("INSERT INTO replicas(chunk_id,worker_id) VALUES(?,?)", (ch["chunk_id"], w))

    def get_object(self, object_id) -> Optional[dict]:
        rows = self._q("SELECT * FROM objects WHERE object_id=?", (object_id,))
        return self._obj(rows[0]) if rows else None

    def get_object_chunks(self, object_id) -> List[dict]:
        chunks = [dict(r) | {"replicas": []} for r in self._q(
            "SELECT chunk_id,chunk_index,size,checksum FROM chunks WHERE object_id=? ORDER BY chunk_index",
            (object_id,))]
        by_id = {c["chunk_id"]: c for c in chunks}
        for r in self._q("""SELECT r.chunk_id, r.worker_id FROM replicas r JOIN chunks c ON c.chunk_id=r.chunk_id
                            WHERE c.object_id=? ORDER BY r.worker_id""", (object_id,)):
            by_id[r["chunk_id"]]["replicas"].append(r["worker_id"])
        return chunks

    def list_objects(self, limit: int, offset: int) -> List[dict]:
        return [dict(r) for r in self._q(
            """SELECT object_id,name,size,content_type,created_at,last_modified_at,checksum,num_chunks
               FROM objects ORDER BY created_at, object_id LIMIT ? OFFSET ?""", (limit, offset))]

    def count_objects(self) -> int:
        return self._q("SELECT COUNT(*) AS n FROM objects")[0]["n"]

    def patch_metadata(self, object_id, updates: dict, now_iso, expected_version=None) -> str:
        """Returns 'ok' | 'notfound' | 'conflict'. A null value deletes the key."""
        with self._tx() as c:
            row = c.execute("SELECT custom_metadata,version FROM objects WHERE object_id=?", (object_id,)).fetchone()
            if not row:
                return "notfound"
            if expected_version is not None and row["version"] != expected_version:
                return "conflict"
            meta = json.loads(row["custom_metadata"])
            for k, v in updates.items():
                if v is None:
                    meta.pop(k, None)
                else:
                    meta[k] = v
            c.execute("UPDATE objects SET custom_metadata=?,last_modified_at=?,version=version+1 WHERE object_id=?",
                      (json.dumps(meta), now_iso, object_id))
            return "ok"

    def delete_object(self, object_id) -> Optional[List[Tuple[str, str]]]:
        """Deletes metadata; returns [(chunk_id, worker_id)] that held its data, or None if unknown."""
        with self._tx() as c:
            if not c.execute("SELECT 1 FROM objects WHERE object_id=?", (object_id,)).fetchone():
                return None
            held = [(r["chunk_id"], r["worker_id"]) for r in c.execute(
                """SELECT r.chunk_id, r.worker_id FROM replicas r JOIN chunks ch ON ch.chunk_id=r.chunk_id
                   WHERE ch.object_id=?""", (object_id,))]
            c.execute("DELETE FROM objects WHERE object_id=?", (object_id,))  # cascades
            return held

    # -- chunks / replicas ----------------------------------------------
    def get_chunk(self, chunk_id) -> Optional[dict]:
        rows = self._q("SELECT chunk_id,object_id,chunk_index,size,checksum FROM chunks WHERE chunk_id=?", (chunk_id,))
        return dict(rows[0]) if rows else None

    def all_chunks_with_replicas(self) -> List[dict]:
        chunks = [dict(r) | {"replicas": []} for r in self._q(
            "SELECT chunk_id,object_id,chunk_index,size,checksum FROM chunks ORDER BY object_id,chunk_index")]
        by_id = {c["chunk_id"]: c for c in chunks}
        for r in self._q("SELECT chunk_id,worker_id FROM replicas ORDER BY worker_id"):
            if r["chunk_id"] in by_id:
                by_id[r["chunk_id"]]["replicas"].append(r["worker_id"])
        return chunks

    def chunk_replicas(self, chunk_id) -> List[str]:
        return [r["worker_id"] for r in self._q("SELECT worker_id FROM replicas WHERE chunk_id=? ORDER BY worker_id", (chunk_id,))]

    def add_replica(self, chunk_id, worker_id):
        self._x("INSERT OR IGNORE INTO replicas(chunk_id,worker_id) VALUES(?,?)", (chunk_id, worker_id))

    def remove_replica(self, chunk_id, worker_id):
        self._x("DELETE FROM replicas WHERE chunk_id=? AND worker_id=?", (chunk_id, worker_id))

    def worker_replica_ids(self, worker_id) -> Set[str]:
        return {r["chunk_id"] for r in self._q("SELECT chunk_id FROM replicas WHERE worker_id=?", (worker_id,))}

    # -- tombstones ------------------------------------------------------
    def add_tombstone(self, worker_id, chunk_id):
        self._x("INSERT OR IGNORE INTO tombstones(worker_id,chunk_id,created_at) VALUES(?,?,?)",
                (worker_id, chunk_id, time.time()))

    def list_tombstones(self, worker_id) -> List[str]:
        return [r["chunk_id"] for r in self._q("SELECT chunk_id FROM tombstones WHERE worker_id=?", (worker_id,))]

    def remove_tombstone(self, worker_id, chunk_id):
        self._x("DELETE FROM tombstones WHERE worker_id=? AND chunk_id=?", (worker_id, chunk_id))
