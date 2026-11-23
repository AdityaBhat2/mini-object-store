"""Environment-configurable controller settings."""
import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


DB_PATH = os.getenv("DB_PATH", "/data/metadata.db")
CHUNK_SIZE = _int("CHUNK_SIZE", 1024 * 1024)            # 1 MB = 1,048,576 bytes
REPLICATION_FACTOR = _int("REPLICATION_FACTOR", 2)
MAX_OBJECT_SIZE = _int("MAX_OBJECT_SIZE", 1024 * 1024 * 1024)  # 1 GiB upload cap

HEARTBEAT_TIMEOUT = _float("HEARTBEAT_TIMEOUT_SECONDS", 9.0)    # OFFLINE after >9s silence
HEALTH_CHECK_INTERVAL = _float("HEALTH_CHECK_INTERVAL_SECONDS", 2.0)
REPLICATION_INTERVAL = _float("REPLICATION_INTERVAL_SECONDS", 4.0)
SCRUB_INTERVAL = _float("SCRUB_INTERVAL_SECONDS", 30.0)

WORKER_TIMEOUT = _float("WORKER_TIMEOUT_SECONDS", 15.0)
WORKER_CONNECT_TIMEOUT = _float("WORKER_CONNECT_TIMEOUT_SECONDS", 3.0)

# Demo helpers (UI "corrupt chunk" button, event feed). Set DEMO_MODE=0 to disable the corrupt endpoint.
DEMO_MODE = os.getenv("DEMO_MODE", "1") == "1"
