"""LangGraph checkpoint saver for Azure SQL Database.

Quickstart
----------
.. code-block:: python

    from langgraph_checkpoint_azure_sql import AzureSqlSaver

    # Azure SQL Database (production)
    CONN_STR = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=yourserver.database.windows.net;"
        "DATABASE=langgraph;"
        "UID=langgraph_user;PWD=YourPassword!;"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    # Or on-premises SQL Server (also works)
    CONN_STR = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost;DATABASE=langgraph;"
        "Trusted_Connection=yes;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )

    with AzureSqlSaver(CONN_STR) as saver:
        saver.setup()          # idempotent schema migration
        graph = builder.compile(checkpointer=saver)
        result = graph.invoke({"text": "hello"}, {"configurable": {"thread_id": "t1"}})
"""
from .saver import AzureSqlSaver, AsyncAzureSqlSaver

__version__ = "0.1.0"
__all__ = ["AzureSqlSaver", "AsyncAzureSqlSaver"]
