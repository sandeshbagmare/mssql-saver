"""Repository (manager) for GraphRun persistence.

All DB interaction lives here — the service layer never touches SQLAlchemy directly.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.run import GraphRun


class RunManager:
    def __init__(self, session: Session) -> None:
        self._db = session

    def create(
        self,
        thread_id: str,
        backend: str,
        input_text: str,
        output_summary: str | None,
        latency_ms: float,
    ) -> GraphRun:
        run = GraphRun(
            thread_id=thread_id,
            backend=backend,
            input_text=input_text,
            output_summary=output_summary,
            latency_ms=latency_ms,
        )
        self._db.add(run)
        self._db.commit()
        self._db.refresh(run)
        return run

    def list_for_thread(self, thread_id: str) -> list[GraphRun]:
        return (
            self._db.query(GraphRun)
            .filter(GraphRun.thread_id == thread_id)
            .order_by(GraphRun.created_at.desc())
            .all()
        )

    def list_for_backend(self, backend: str, limit: int = 100) -> list[GraphRun]:
        return (
            self._db.query(GraphRun)
            .filter(GraphRun.backend == backend)
            .order_by(GraphRun.created_at.desc())
            .limit(limit)
            .all()
        )
