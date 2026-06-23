"""ORM model for a single graph invocation run."""
from datetime import datetime, timezone
from sqlalchemy import BigInteger, DateTime, Float, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class GraphRun(Base):
    __tablename__ = "graph_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(String(150), nullable=False, index=True)
    backend: Mapped[str] = mapped_column(String(20), nullable=False)  # "postgres" | "mssql"
    input_text: Mapped[str] = mapped_column(Text, nullable=False)
    output_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    latency_ms: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
