"""Pydantic request/response schemas for the graph API."""
from pydantic import BaseModel, Field


class InvokeRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=10_000, description="Input text to analyse")
    thread_id: str | None = Field(None, description="Reuse an existing thread for state continuity")


class InvokeResponse(BaseModel):
    thread_id: str
    backend: str
    summary: str
    word_count: int
    char_count: int
    sentence_count: int
    latency_ms: float
    run_id: int


class CheckpointInfo(BaseModel):
    checkpoint_id: str
    step: int | None
    source: str | None
    channel_versions: dict | None


class HistoryResponse(BaseModel):
    thread_id: str
    backend: str
    checkpoints: list[CheckpointInfo]
