"""Atomic on-disk chunk storage: /data/chunks/<chunk_id> (raw bytes, nothing else)."""
import hashlib
import os
import re
import shutil
import uuid
from typing import Dict, List, Optional

import config

CHUNK_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")
TMP_DIR = os.path.join(config.STORAGE_DIR, ".tmp")


def init_storage():
    os.makedirs(config.STORAGE_DIR, exist_ok=True)
    os.makedirs(TMP_DIR, exist_ok=True)
    for f in os.listdir(TMP_DIR):          # leftovers of interrupted writes
        try:
            os.remove(os.path.join(TMP_DIR, f))
        except OSError:
            pass


def valid_chunk_id(chunk_id: str) -> bool:
    return bool(CHUNK_ID_RE.match(chunk_id))


def chunk_path(chunk_id: str) -> str:
    return os.path.join(config.STORAGE_DIR, chunk_id)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fsync_dir(path: str):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_chunk(chunk_id: str, data: bytes) -> Dict:
    """temp file + fsync + os.replace (atomic on POSIX) + directory fsync."""
    tmp = os.path.join(TMP_DIR, f"{chunk_id}.{uuid.uuid4().hex}.tmp")
    try:
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, chunk_path(chunk_id))
        _fsync_dir(config.STORAGE_DIR)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    return {"chunk_id": chunk_id, "size": len(data), "checksum": sha256_hex(data)}


def read_chunk(chunk_id: str) -> Optional[bytes]:
    try:
        with open(chunk_path(chunk_id), "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def delete_chunk(chunk_id: str) -> bool:
    try:
        os.remove(chunk_path(chunk_id))
        _fsync_dir(config.STORAGE_DIR)
        return True
    except FileNotFoundError:
        return False


def _file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def list_chunks(with_checksums: bool = True) -> List[Dict]:
    out = []
    with os.scandir(config.STORAGE_DIR) as it:
        for e in sorted(it, key=lambda e: e.name):
            if not e.is_file() or e.name.startswith("."):
                continue
            try:
                item = {"chunk_id": e.name, "size": e.stat().st_size}
                if with_checksums:
                    item["checksum"] = _file_sha256(e.path)   # computed from the bytes on disk, now
                out.append(item)
            except FileNotFoundError:
                continue
    return out


def usage() -> Dict:
    chunks = list_chunks(with_checksums=False)
    free = shutil.disk_usage(config.STORAGE_DIR).free
    return {"chunk_count": len(chunks), "disk_usage": sum(c["size"] for c in chunks), "free_bytes": free}
