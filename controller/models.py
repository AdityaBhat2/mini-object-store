"""Pydantic schemas for internal (worker -> controller) requests."""
from typing import List, Optional

from pydantic import BaseModel, Field


class ChunkInventoryItem(BaseModel):
    chunk_id: str
    size: int = 0


class RegisterRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=64)
    url: str = Field(min_length=1)
    chunks: List[ChunkInventoryItem] = []


class HeartbeatRequest(BaseModel):
    worker_id: str = Field(min_length=1, max_length=64)
    chunk_count: int = 0
    disk_usage: int = 0
    free_bytes: Optional[int] = None
