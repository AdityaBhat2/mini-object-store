"""Worker settings (environment driven)."""
import os

WORKER_ID = os.getenv("WORKER_ID", "worker1")
WORKER_URL = os.getenv("WORKER_URL", f"http://{WORKER_ID}:8000").rstrip("/")
CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://controller:8000").rstrip("/")
STORAGE_DIR = os.getenv("STORAGE_DIR", "/data/chunks")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "3"))
DEMO_MODE = os.getenv("DEMO_MODE", "1") == "1"
