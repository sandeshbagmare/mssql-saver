"""LangGraph checkpoint saver for Microsoft SQL Server.

Quickstart
----------
.. code-block:: python

    from langgraph_checkpoint_mssql import MssqlSaver

    CONN_STR = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;DATABASE=langgraph;"
        "UID=sa;PWD=SqlPass123!;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )

    with MssqlSaver(CONN_STR) as saver:
        saver.setup()          # idempotent schema migration
        graph = builder.compile(checkpointer=saver)
        result = graph.invoke({"text": "hello"}, {"configurable": {"thread_id": "t1"}})
"""
from .saver import AsyncMssqlSaver, MssqlSaver

__version__ = "0.1.0"
__all__ = ["MssqlSaver", "AsyncMssqlSaver"]
