# langgraph-checkpoint-azure-sql

> LangGraph checkpoint saver for **Azure SQL Database** and **SQL Server**.

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## Overview

A production-grade checkpoint saver that enables [LangGraph](https://langchain-ai.github.io/langgraph/) to persist workflow state in Azure SQL Database or any SQL Server instance. Built to the `BaseCheckpointSaver` specification with full conformance testing.

**Key facts:**
- **15/15 conformance tests** passing
- **Zero-change compatibility** with Azure SQL Database & on-premises SQL Server
- Same T-SQL engine under the hood — one library for both
- Sync + async support via `asyncio.to_thread`
- Thread-safe connection pooling with MARS auto-enable

## Installation

```bash
pip install langgraph-checkpoint-azure-sql
```

### Prerequisites

- Python 3.10+
- [Microsoft ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)
- An Azure SQL Database or SQL Server instance

## Quick start

```python
from langgraph_checkpoint_azure_sql import AzureSqlSaver

# Azure SQL Database
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=yourserver.database.windows.net;"
    "DATABASE=langgraph;"
    "UID=langgraph_user;PWD=YourPassword!;"
    "Encrypt=yes;TrustServerCertificate=no;"
)

# Or on-premises SQL Server
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;DATABASE=langgraph;"
    "UID=sa;PWD=SqlPass123!;"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

with AzureSqlSaver(CONN_STR) as saver:
    saver.setup()          # idempotent schema migration
    graph = builder.compile(checkpointer=saver)
    result = graph.invoke(
        {"text": "hello"},
        {"configurable": {"thread_id": "t1"}},
    )
```

## Azure SQL vs SQL Server

This library works identically with both. Azure SQL Database is SQL Server's cloud-managed edition — it runs the same T-SQL engine, supports the same ODBC driver, and uses the same wire protocol.

| Feature | Azure SQL Database | SQL Server (on-prem) |
|---|---|---|
| T-SQL engine | ✅ Same | ✅ Same |
| ODBC Driver 18 | ✅ Supported | ✅ Supported |
| MARS_Connection | ✅ Supported | ✅ Supported |
| Transport | Always TCP | TCP or Named Pipes |
| Authentication | SQL Auth, Azure AD, Managed Identity | SQL Auth, Windows Auth |
| `UPDLOCK/HOLDLOCK` | ✅ Supported | ✅ Supported |
| `JSON_VALUE` | ✅ Supported | ✅ Supported (2016+) |

## Azure AD / Managed Identity

```python
# Azure AD Interactive (for development)
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=yourserver.database.windows.net;"
    "DATABASE=langgraph;"
    "Authentication=ActiveDirectoryInteractive;"
    "Encrypt=yes;"
)

# Managed Identity (for Azure VMs, App Service, AKS)
CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=yourserver.database.windows.net;"
    "DATABASE=langgraph;"
    "Authentication=ActiveDirectoryMsi;"
    "Encrypt=yes;"
)
```

## Schema

Four tables, mirroring the official `langgraph-checkpoint-postgres` design:

| Table | Purpose |
|---|---|
| `checkpoint_migrations` | Migration version tracking |
| `checkpoints` | Core checkpoint snapshots |
| `checkpoint_blobs` | Per-channel value storage (deduplication) |
| `checkpoint_writes` | Pending task outputs |

## Research & benchmarks

See [`docs/AZURE_SQL_GUIDE.md`](docs/AZURE_SQL_GUIDE.md) for the full design walkthrough, benchmarks, and production guidance.

## Running tests

```bash
set AZURE_SQL_TEST_CONN_STR=DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=langgraph_azure_test;Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;
pytest tests/ -v
# Expected: 15 passed
```

## License

MIT
