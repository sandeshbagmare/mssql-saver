"""MssqlSaver — sync + async LangGraph checkpoint saver for SQL Server.

Async methods delegate to ``asyncio.to_thread`` over the same thread-safe pool.
This deliberately avoids ``aioodbc`` (low release cadence) in favour of the
well-maintained ``pyodbc`` with a thin async shim — a trade-off documented in
``docs/CONFERENCE.md``.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator, Sequence
from typing import Any

from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id,
)

from .base import (
    SQL_DELETE_BLOBS,
    SQL_DELETE_CHECKPOINTS,
    SQL_DELETE_WRITES,
    SQL_GET_BY_ID,
    SQL_GET_LATEST,
    BaseMssqlSaver,
)
from .pool import ConnectionPool


class MssqlSaver(BaseMssqlSaver):
    """LangGraph checkpoint saver backed by Microsoft SQL Server.

    Parameters
    ----------
    conn_str:
        A pyodbc connection string, e.g.::

            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost;DATABASE=langgraph;"
            "UID=sa;PWD=SqlPass123!;"
            "Encrypt=yes;TrustServerCertificate=yes;"

    pool_size:
        Maximum number of open connections (default 10).
    serde:
        Optional custom serialiser; defaults to ``JsonPlusSerializer``.

    Usage
    -----
    .. code-block:: python

        from langgraph_checkpoint_mssql import MssqlSaver

        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=localhost;DATABASE=langgraph;"
            "UID=sa;PWD=SqlPass123!;"
            "Encrypt=yes;TrustServerCertificate=yes;"
        )
        with MssqlSaver(conn_str) as saver:
            saver.setup()
            graph = builder.compile(checkpointer=saver)
            result = graph.invoke(...)
    """

    def __init__(
        self,
        conn_str: str,
        *,
        pool_size: int = 10,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.pool = ConnectionPool(conn_str, pool_size)

    def __enter__(self) -> "MssqlSaver":
        return self

    def __exit__(self, *_: Any) -> None:
        self.pool.close()

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Create / migrate schema idempotently. Call once at startup."""
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._run_setup(cur)

    # ------------------------------------------------------------------
    # Sync interface
    # ------------------------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                if checkpoint_id := get_checkpoint_id(config):
                    cur.execute(SQL_GET_BY_ID, (thread_id, checkpoint_ns, checkpoint_id))
                else:
                    cur.execute(SQL_GET_LATEST, (thread_id, checkpoint_ns))

                row = cur.fetchone()
                if row is None:
                    return None
                return self._row_to_tuple(row, cur)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        sql, params = self._build_list_query(config, filter, before, limit)

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()  # materialise before iterating
                for row in rows:
                    yield self._row_to_tuple(row, cur)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        parent_id: str | None = config["configurable"].get("checkpoint_id")

        typ, blob, meta_json, channel_blobs = self._serialize_checkpoint(
            config, checkpoint, metadata, new_versions
        )

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                self._upsert_blobs(cur, thread_id, checkpoint_ns, channel_blobs)
                self._upsert_checkpoint(
                    cur, thread_id, checkpoint_ns,
                    checkpoint["id"], parent_id, typ, blob, meta_json,
                )

        return {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id: str = config["configurable"]["thread_id"]
        checkpoint_ns: str = config["configurable"].get("checkpoint_ns", "")
        checkpoint_id: str = config["configurable"]["checkpoint_id"]

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                for i, (channel, value) in enumerate(writes):
                    idx = WRITES_IDX_MAP.get(channel, i)
                    typ, raw = self.serde.dumps_typed(value)
                    self._upsert_write(
                        cur, thread_id, checkpoint_ns, checkpoint_id,
                        task_id, idx, channel, typ, raw, task_path,
                    )

    def delete_thread(self, thread_id: str) -> None:
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(SQL_DELETE_WRITES,      (thread_id,))
                cur.execute(SQL_DELETE_BLOBS,       (thread_id,))
                cur.execute(SQL_DELETE_CHECKPOINTS, (thread_id,))

    # ------------------------------------------------------------------
    # Async interface (asyncio.to_thread over the sync pool)
    # ------------------------------------------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return await asyncio.to_thread(self.get_tuple, config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        results = await asyncio.to_thread(
            lambda: list(self.list(config, filter=filter, before=before, limit=limit))
        )
        for item in results:
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return await asyncio.to_thread(
            self.put, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        await asyncio.to_thread(self.put_writes, config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        await asyncio.to_thread(self.delete_thread, thread_id)


# Alias for code that prefers an explicit async name
AsyncMssqlSaver = MssqlSaver
