"""SQL schema, migrations, query constants, and BaseAzureSqlSaver helpers.

Design principles
-----------------
* Mirrors the official langgraph-checkpoint-postgres 3-table split exactly:
  checkpoints / checkpoint_blobs / checkpoint_writes / checkpoint_migrations.
* ALL query parameters use ``?`` placeholders — never string-interpolated values.
  This includes OFFSET/FETCH limits (preventing CVE-2025-67644-class injection).
* Upserts avoid MERGE (documented T-SQL concurrency bugs) in favour of
  UPDATE-then-INSERT with UPDLOCK/HOLDLOCK, or INSERT…WHERE NOT EXISTS.
* Metadata stored as NVARCHAR(MAX) JSON; filtered via JSON_VALUE (SQL Server 2016+).
"""
from __future__ import annotations

import json
import random
from collections.abc import Sequence
from typing import Any

import pyodbc
from langchain_core.runnables import RunnableConfig

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    SerializerProtocol,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

# ---------------------------------------------------------------------------
# Schema migrations  (index == version; applied in order, tracked in DB)
# ---------------------------------------------------------------------------

MIGRATIONS: list[str] = [
    # 0 – version-tracking table (always created first, idempotently)
    """IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='checkpoint_migrations')
    CREATE TABLE checkpoint_migrations (
        v INT NOT NULL,
        CONSTRAINT PK_cm PRIMARY KEY (v)
    )""",
    # 1 – main checkpoints table
    # NOTE: 'checkpoint' is a reserved word in T-SQL; bracket-quote the table name.
    """IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='checkpoints')
    CREATE TABLE [checkpoints] (
        thread_id            NVARCHAR(150)  NOT NULL,
        checkpoint_ns        NVARCHAR(255)  NOT NULL DEFAULT '',
        checkpoint_id        NVARCHAR(150)  NOT NULL,
        parent_checkpoint_id NVARCHAR(150)  NULL,
        type                 NVARCHAR(150)  NULL,
        [checkpoint]         VARBINARY(MAX) NOT NULL,
        metadata             NVARCHAR(MAX)  NOT NULL DEFAULT '{}',
        CONSTRAINT PK_checkpoints PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id)
    )""",
    # 2 – per-channel blobs (one row per channel × version)
    """IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='checkpoint_blobs')
    CREATE TABLE [checkpoint_blobs] (
        thread_id     NVARCHAR(150)  NOT NULL,
        checkpoint_ns NVARCHAR(255)  NOT NULL,
        channel       NVARCHAR(255)  NOT NULL,
        version       NVARCHAR(150)  NOT NULL,
        type          NVARCHAR(150)  NOT NULL,
        blob          VARBINARY(MAX) NULL,
        CONSTRAINT PK_checkpoint_blobs PRIMARY KEY (thread_id, checkpoint_ns, channel, version)
    )""",
    # 3 – pending / intermediate writes
    """IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='checkpoint_writes')
    CREATE TABLE [checkpoint_writes] (
        thread_id     NVARCHAR(150)  NOT NULL,
        checkpoint_ns NVARCHAR(255)  NOT NULL,
        checkpoint_id NVARCHAR(150)  NOT NULL,
        task_id       NVARCHAR(150)  NOT NULL,
        idx           INT            NOT NULL,
        channel       NVARCHAR(255)  NOT NULL,
        type          NVARCHAR(150)  NULL,
        blob          VARBINARY(MAX) NOT NULL,
        task_path     NVARCHAR(MAX)  NOT NULL DEFAULT '',
        CONSTRAINT PK_checkpoint_writes
            PRIMARY KEY (thread_id, checkpoint_ns, checkpoint_id, task_id, idx)
    )""",
    # 4 – thread_id index on checkpoints
    """IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name='IX_checkpoints_tid' AND object_id=OBJECT_ID('[checkpoints]')
    )
    CREATE INDEX IX_checkpoints_tid ON [checkpoints](thread_id)""",
    # 5 – thread_id index on checkpoint_blobs
    """IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name='IX_cb_tid' AND object_id=OBJECT_ID('[checkpoint_blobs]')
    )
    CREATE INDEX IX_cb_tid ON [checkpoint_blobs](thread_id)""",
    # 6 – thread_id index on checkpoint_writes
    """IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name='IX_cw_tid' AND object_id=OBJECT_ID('[checkpoint_writes]')
    )
    CREATE INDEX IX_cw_tid ON [checkpoint_writes](thread_id)""",
]

# ---------------------------------------------------------------------------
# SQL statements (all values passed as ? parameters — never interpolated)
# ---------------------------------------------------------------------------

# -- checkpoints: DO-UPDATE upsert (UPDATE first; INSERT if 0 rows affected) --
# UPDLOCK+HOLDLOCK prevents phantom inserts between the read and the write.
SQL_UPDATE_CHECKPOINT = """\
UPDATE [checkpoints] WITH (UPDLOCK, HOLDLOCK)
SET    [checkpoint] = ?,
       metadata     = ?
WHERE  thread_id = ? AND checkpoint_ns = ? AND checkpoint_id = ?"""

SQL_INSERT_CHECKPOINT = """\
INSERT INTO [checkpoints]
    (thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
     type, [checkpoint], metadata)
VALUES (?, ?, ?, ?, ?, ?, ?)"""

# -- checkpoint_blobs: DO-NOTHING upsert (idempotent – blobs are immutable) --
SQL_INSERT_BLOB_IF_NOT_EXISTS = """\
INSERT INTO [checkpoint_blobs]
    (thread_id, checkpoint_ns, channel, version, type, blob)
SELECT ?, ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM [checkpoint_blobs]
    WHERE thread_id=? AND checkpoint_ns=? AND channel=? AND version=?
)"""

# -- checkpoint_writes: DO-UPDATE for regular writes (idx>=0) --
SQL_UPDATE_WRITE = """\
UPDATE [checkpoint_writes] WITH (UPDLOCK)
SET    channel=?, type=?, blob=?, task_path=?
WHERE  thread_id=? AND checkpoint_ns=? AND checkpoint_id=?
       AND task_id=? AND idx=?"""

SQL_INSERT_WRITE = """\
INSERT INTO [checkpoint_writes]
    (thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
     channel, type, blob, task_path)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""

# -- checkpoint_writes: DO-NOTHING for special writes (idx<0, e.g. ERROR) --
SQL_INSERT_WRITE_IF_NOT_EXISTS = """\
INSERT INTO [checkpoint_writes]
    (thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
     channel, type, blob, task_path)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?
WHERE NOT EXISTS (
    SELECT 1 FROM [checkpoint_writes]
    WHERE thread_id=? AND checkpoint_ns=? AND checkpoint_id=?
          AND task_id=? AND idx=?
)"""

# -- reads --
SQL_GET_BY_ID = """\
SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
       type, [checkpoint], metadata
FROM   [checkpoints]
WHERE  thread_id=? AND checkpoint_ns=? AND checkpoint_id=?"""

SQL_GET_LATEST = """\
SELECT TOP (1)
       thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,
       type, [checkpoint], metadata
FROM   [checkpoints]
WHERE  thread_id=? AND checkpoint_ns=?
ORDER BY checkpoint_id DESC"""

SQL_GET_WRITES = """\
SELECT task_id, idx, channel, type, blob
FROM   [checkpoint_writes]
WHERE  thread_id=? AND checkpoint_ns=? AND checkpoint_id=?
ORDER BY task_id, idx"""

# -- deletes --
SQL_DELETE_WRITES      = "DELETE FROM [checkpoint_writes] WHERE thread_id=?"
SQL_DELETE_BLOBS       = "DELETE FROM [checkpoint_blobs]  WHERE thread_id=?"
SQL_DELETE_CHECKPOINTS = "DELETE FROM [checkpoints]       WHERE thread_id=?"


# ---------------------------------------------------------------------------
# BaseMssqlSaver
# ---------------------------------------------------------------------------

class BaseAzureSqlSaver(BaseCheckpointSaver[str]):
    """Shared migrations, SQL helpers, and row-mapping logic.

    Concrete subclasses provide the connection / pool (``AzureSqlSaver``).
    """

    # ------------------------------------------------------------------
    # Schema setup
    # ------------------------------------------------------------------

    def _run_setup(self, cur: pyodbc.Cursor) -> None:
        """Apply any unapplied migrations idempotently."""
        # Always ensure migration 0 (the tracking table) exists first.
        cur.execute(MIGRATIONS[0])
        cur.connection.commit()

        try:
            cur.execute("SELECT v FROM checkpoint_migrations ORDER BY v")
            applied: set[int] = {row[0] for row in cur.fetchall()}
        except Exception:
            applied = set()

        for version, ddl in enumerate(MIGRATIONS):
            if version in applied:
                continue
            cur.execute(ddl)
            cur.execute(
                "INSERT INTO checkpoint_migrations (v) VALUES (?)", (version,)
            )
            cur.connection.commit()

    # ------------------------------------------------------------------
    # Version generation  (mirrors InMemorySaver exactly)
    # ------------------------------------------------------------------

    def get_next_version(self, current: str | None, channel: None = None) -> str:
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        return f"{current_v + 1:032}.{random.random():016}"

    # ------------------------------------------------------------------
    # Serialization helpers
    # ------------------------------------------------------------------

    def _serialize_checkpoint(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> tuple[str, bytes, str, list[tuple[str, str, str, bytes]]]:
        """Return (type, checkpoint_bytes, metadata_json, channel_blobs).

        channel_blobs is a list of (channel, version, blob_type, blob_bytes).
        channel_values are popped from the checkpoint dict before serialisation,
        matching the InMemorySaver contract.
        """
        c = checkpoint.copy()
        values: dict[str, Any] = c.pop("channel_values")  # type: ignore[misc]
        typ, blob = self.serde.dumps_typed(c)
        meta_json = json.dumps(get_checkpoint_metadata(config, metadata))

        channel_blobs: list[tuple[str, str, str, bytes]] = []
        for channel, version in new_versions.items():
            if channel in values:
                b_type, b_blob = self.serde.dumps_typed(values[channel])
            else:
                b_type, b_blob = "empty", b""
            channel_blobs.append((channel, str(version), b_type, b_blob))

        return typ, blob, meta_json, channel_blobs

    # ------------------------------------------------------------------
    # DB read helpers
    # ------------------------------------------------------------------

    def _load_blobs(
        self,
        conn: pyodbc.Connection,
        thread_id: str,
        checkpoint_ns: str,
        channel_versions: ChannelVersions,
    ) -> dict[str, Any]:
        """Fetch and deserialise blobs for every (channel, version) in the map."""
        if not channel_versions:
            return {}
        pairs = [(ch, str(ver)) for ch, ver in channel_versions.items()]
        # Build a parameterised OR clause — structure is data-driven, values are ?
        placeholders = " OR ".join("(channel=? AND version=?)" for _ in pairs)
        flat = [v for pair in pairs for v in pair]
        with conn.cursor() as cur2:
            cur2.execute(
                f"SELECT channel, type, blob FROM [checkpoint_blobs] "
                f"WHERE thread_id=? AND checkpoint_ns=? AND ({placeholders})",
                [thread_id, checkpoint_ns] + flat,
            )
            rows = cur2.fetchall()
        result: dict[str, Any] = {}
        for ch, typ, raw in rows:
            if typ == "empty":
                continue
            result[ch] = self.serde.loads_typed(
                (typ, bytes(raw) if raw is not None else b"")
            )
        return result

    def _load_writes(
        self,
        conn: pyodbc.Connection,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
    ) -> list[tuple[str, str, Any]]:
        """Fetch and deserialise pending writes for a checkpoint."""
        with conn.cursor() as cur2:
            cur2.execute(SQL_GET_WRITES, (thread_id, checkpoint_ns, checkpoint_id))
            rows = cur2.fetchall()
        out = []
        for task_id, _idx, channel, typ, raw in rows:
            value = self.serde.loads_typed(
                (typ, bytes(raw) if raw is not None else b"")
            )
            out.append((task_id, channel, value))
        return out

    def _row_to_tuple(
        self, row: tuple, conn: pyodbc.Connection
    ) -> CheckpointTuple:
        """Convert one *checkpoints* row + blob/write lookups into a CheckpointTuple."""
        (
            thread_id, checkpoint_ns, checkpoint_id,
            parent_id, typ, raw, metadata_json,
        ) = row

        c: Checkpoint = self.serde.loads_typed(
            (typ, bytes(raw) if raw is not None else b"")
        )
        channel_values = self._load_blobs(
            conn, thread_id, checkpoint_ns, c["channel_versions"]
        )
        pending_writes = self._load_writes(
            conn, thread_id, checkpoint_ns, checkpoint_id
        )
        metadata: CheckpointMetadata = json.loads(metadata_json or "{}")

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": checkpoint_ns,
                    "checkpoint_id": checkpoint_id,
                }
            },
            checkpoint={**c, "channel_values": channel_values},
            metadata=metadata,
            pending_writes=pending_writes,
            parent_config=(
                {
                    "configurable": {
                        "thread_id": thread_id,
                        "checkpoint_ns": checkpoint_ns,
                        "checkpoint_id": parent_id,
                    }
                }
                if parent_id
                else None
            ),
        )

    # ------------------------------------------------------------------
    # DB write helpers
    # ------------------------------------------------------------------

    def _upsert_checkpoint(
        self,
        cur: pyodbc.Cursor,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        parent_id: str | None,
        typ: str,
        blob: bytes,
        meta_json: str,
    ) -> None:
        cur.execute(
            SQL_UPDATE_CHECKPOINT,
            (blob, meta_json, thread_id, checkpoint_ns, checkpoint_id),
        )
        if cur.rowcount == 0:
            cur.execute(
                SQL_INSERT_CHECKPOINT,
                (thread_id, checkpoint_ns, checkpoint_id,
                 parent_id, typ, blob, meta_json),
            )

    def _upsert_blobs(
        self,
        cur: pyodbc.Cursor,
        thread_id: str,
        checkpoint_ns: str,
        channel_blobs: list[tuple[str, str, str, bytes]],
    ) -> None:
        for channel, version, b_type, b_blob in channel_blobs:
            cur.execute(
                SQL_INSERT_BLOB_IF_NOT_EXISTS,
                (thread_id, checkpoint_ns, channel, version, b_type, b_blob,
                 thread_id, checkpoint_ns, channel, version),
            )

    def _upsert_write(
        self,
        cur: pyodbc.Cursor,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        idx: int,
        channel: str,
        typ: str | None,
        blob: bytes,
        task_path: str,
    ) -> None:
        if idx >= 0:
            # Regular write – overwrite if already exists (same semantics as Postgres DO UPDATE)
            cur.execute(
                SQL_UPDATE_WRITE,
                (channel, typ, blob, task_path,
                 thread_id, checkpoint_ns, checkpoint_id, task_id, idx),
            )
            if cur.rowcount == 0:
                cur.execute(
                    SQL_INSERT_WRITE,
                    (thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
                     channel, typ, blob, task_path),
                )
        else:
            # Special write (ERROR / INTERRUPT / etc.) – DO NOTHING if already exists
            cur.execute(
                SQL_INSERT_WRITE_IF_NOT_EXISTS,
                (thread_id, checkpoint_ns, checkpoint_id, task_id, idx,
                 channel, typ, blob, task_path,
                 thread_id, checkpoint_ns, checkpoint_id, task_id, idx),
            )

    # ------------------------------------------------------------------
    # Dynamic query builder for list()
    # ------------------------------------------------------------------

    def _build_list_query(
        self,
        config: RunnableConfig | None,
        filter: dict[str, Any] | None,
        before: RunnableConfig | None,
        limit: int | None,
    ) -> tuple[str, list[Any]]:
        """Return (sql, params) for list(). All values are ? parameters."""
        where: list[str] = []
        params: list[Any] = []

        if config:
            where.append("thread_id=?")
            params.append(config["configurable"]["thread_id"])
            if ns := config["configurable"].get("checkpoint_ns"):
                where.append("checkpoint_ns=?")
                params.append(ns)

        if before and (before_id := get_checkpoint_id(before)):
            where.append("checkpoint_id < ?")
            params.append(before_id)

        if filter:
            for key, value in filter.items():
                # JSON_VALUE returns NULL for missing keys — use IS NULL guard if needed
                where.append("JSON_VALUE(metadata, ?) = ?")
                params.append(f"$.{key}")
                params.append(str(value) if not isinstance(value, str) else value)

        sql = (
            "SELECT thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id,"
            "       type, [checkpoint], metadata\n"
            "FROM   [checkpoints]"
        )
        if where:
            sql += "\nWHERE  " + " AND ".join(where)
        sql += "\nORDER BY checkpoint_id DESC"
        if limit is not None:
            # Parameterised FETCH — never string-concatenated (prevents SQLi)
            sql += "\nOFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"
            params.append(limit)
        return sql, params
