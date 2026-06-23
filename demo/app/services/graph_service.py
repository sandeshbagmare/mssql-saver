"""Orchestrates a graph invocation: runs the graph, persists the run record."""
from __future__ import annotations

import asyncio
import time
import uuid

from sqlalchemy.orm import Session

from app.graph.builder import build_graph
from app.managers.run_manager import RunManager
from app.services.checkpointer_factory import Backend, get_checkpointer


def invoke_graph_sync(
    backend: Backend,
    text: str,
    thread_id: str | None,
    db: Session,
) -> dict:
    """Run the graph synchronously and persist a GraphRun record."""
    thread_id = thread_id or str(uuid.uuid4())
    checkpointer = get_checkpointer(backend)
    graph = build_graph(checkpointer)

    config = {"configurable": {"thread_id": thread_id}}

    start = time.perf_counter()
    result = graph.invoke({"text": text, "normalised": "", "word_count": 0,
                           "char_count": 0, "sentence_count": 0, "summary": ""},
                          config)
    latency_ms = (time.perf_counter() - start) * 1000

    manager = RunManager(db)
    run = manager.create(
        thread_id=thread_id,
        backend=backend,
        input_text=text,
        output_summary=result.get("summary"),
        latency_ms=latency_ms,
    )

    return {
        "thread_id": thread_id,
        "backend": backend,
        "summary": result.get("summary"),
        "word_count": result.get("word_count"),
        "char_count": result.get("char_count"),
        "sentence_count": result.get("sentence_count"),
        "latency_ms": round(latency_ms, 3),
        "run_id": run.id,
    }


async def invoke_graph(
    backend: Backend,
    text: str,
    thread_id: str | None,
    db: Session,
) -> dict:
    """Async wrapper — runs the blocking invocation in a thread pool."""
    return await asyncio.to_thread(invoke_graph_sync, backend, text, thread_id, db)


def get_thread_history(backend: Backend, thread_id: str) -> list[dict]:
    """List LangGraph checkpoints (not GraphRun records) for a thread."""
    checkpointer = get_checkpointer(backend)
    config = {"configurable": {"thread_id": thread_id}}
    history = []
    for tup in checkpointer.list(config):
        history.append({
            "checkpoint_id": tup.config["configurable"]["checkpoint_id"],
            "step": tup.metadata.get("step"),
            "source": tup.metadata.get("source"),
            "channel_versions": tup.checkpoint.get("channel_versions"),
        })
    return history
