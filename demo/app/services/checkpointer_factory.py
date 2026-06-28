"""Factory that returns the correct checkpointer for a given backend name."""
from __future__ import annotations

from typing import Literal

from app.core.config import settings

Backend = Literal["postgres", "mssql", "azure_sql"]


def get_checkpointer(backend: Backend):
    """Return an initialised + setup checkpointer for *backend*.

    Both savers are synchronous; async usage goes through asyncio.to_thread
    at the service layer.

    The ``azure_sql`` backend uses the **same MssqlSaver class** because
    Azure SQL Database runs the identical T-SQL engine as on-premises
    SQL Server.  The only difference is the connection string (pointing to
    ``*.database.windows.net`` in production, or a separate local database
    for simulation).
    """
    if backend == "postgres":
        import psycopg
        from langgraph.checkpoint.postgres import PostgresSaver

        conn = psycopg.connect(settings.pg_dsn, autocommit=True)
        saver = PostgresSaver(conn)
        saver.setup()
        return saver

    elif backend == "mssql":
        from langgraph_checkpoint_mssql import MssqlSaver

        saver = MssqlSaver(settings.mssql_conn_str, pool_size=settings.pool_size)
        saver.setup()
        return saver

    elif backend == "azure_sql":
        from langgraph_checkpoint_mssql import MssqlSaver

        saver = MssqlSaver(settings.azure_sql_conn_str, pool_size=settings.pool_size)
        saver.setup()
        return saver

    else:
        raise ValueError(
            f"Unknown backend: {backend!r}. "
            "Choose 'postgres', 'mssql', or 'azure_sql'."
        )
