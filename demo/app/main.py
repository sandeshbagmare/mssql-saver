"""FastAPI application entry-point.

Lifespan:
  - Creates the graph_runs ORM table in Postgres (idempotent).
  - Runs setup() for both MssqlSaver and PostgresSaver (idempotent schema migrations).
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_router
from app.core.config import settings
from app.db.base import Base
from app.db.session import pg_engine, mssql_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create graph_runs table in both DBs (ORM layer)
    Base.metadata.create_all(bind=pg_engine)
    try:
        Base.metadata.create_all(bind=mssql_engine)
    except Exception as exc:
        print(f"[warn] MSSQL ORM table creation: {exc}")

    # Run checkpoint schema migrations
    from app.services.checkpointer_factory import get_checkpointer
    for backend in ("postgres", "mssql"):
        try:
            get_checkpointer(backend)
            print(f"[startup] {backend} checkpointer setup OK")
        except Exception as exc:
            print(f"[warn] {backend} checkpointer setup failed: {exc}")

    yield


app = FastAPI(
    title=settings.app_title,
    description=(
        "Demonstrates LangGraph with both PostgreSQL (official) and "
        "MS SQL Server (homegrown) checkpoint savers side-by-side."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(api_router, prefix="/api/v1")


@app.get("/health")
def health():
    return {"status": "ok"}
