# langgraph-checkpoint-mssql

A **production-grade, homegrown** Microsoft SQL Server checkpoint saver for
[LangGraph](https://github.com/langchain-ai/langgraph), implementing the official
`BaseCheckpointSaver` interface from `langgraph-checkpoint`.

## Why this library?

LangGraph officially ships checkpoint savers for **Postgres, SQLite, Redis, and
MongoDB** — but not SQL Server. The only publicly available third-party option
(kailashsp/langgraph_azure_sql_db_checkpoint) has 3 commits, 2 stars, no releases,
a naive single-table schema, and uncustomised template placeholders. That is not
a dependency you want in production.

This library takes a different approach:

| Feature | kailashsp/… | **this library** |
|---|---|---|
| Schema design | 1 table, whole blob | 3 tables (mirrors official PG design) |
| Channel blobs | merged into checkpoint | separate `checkpoint_blobs` rows |
| Pending writes | not tracked | `checkpoint_writes` table |
| Upsert strategy | SQLAlchemy ORM | UPDATE+INSERT with UPDLOCK/HOLDLOCK |
| SQL injection safety | unknown | fully parameterised (CVE-2025-67644-safe) |
| Async support | `aioodbc` | `asyncio.to_thread` (stable stdlib) |
| Releases / PyPI | none | 0.1.0 |
| Maintenance | 3 commits, individual | open source, conformance-tested |

## Installation

```bash
pip install langgraph-checkpoint-mssql
# Requires: ODBC Driver 18 for SQL Server on the host
# winget install Microsoft.msodbcsql.18
```

## Quickstart

```python
from langgraph_checkpoint_mssql import MssqlSaver
from langgraph.graph import StateGraph

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost;DATABASE=langgraph;"
    "UID=sa;PWD=SqlPass123!;"
    "Encrypt=yes;TrustServerCertificate=yes;"
)

builder = StateGraph(dict)
# ... add nodes ...

with MssqlSaver(CONN_STR) as saver:
    saver.setup()   # idempotent — call once at startup
    graph = builder.compile(checkpointer=saver)

    result = graph.invoke(
        {"text": "hello"},
        {"configurable": {"thread_id": "my-thread"}},
    )
```

## Schema

Four tables are created automatically by `setup()`:

| Table | Purpose |
|---|---|
| `checkpoint_migrations` | Tracks applied migration versions |
| `checkpoints` | One row per checkpoint (without channel values) |
| `checkpoint_blobs` | One row per `(channel, version)` blob |
| `checkpoint_writes` | Pending / intermediate task writes |

## Security

All SQL parameters are passed as `?` placeholders — never string-interpolated.
This includes `OFFSET/FETCH NEXT ? ROWS ONLY` for the `limit` parameter,
mitigating the class of SQL injection bugs in `CVE-2025-67644`.

## Requirements

- Python ≥ 3.11
- `pyodbc >= 5.0`
- `langgraph-checkpoint >= 4.1.0, < 5.0`
- ODBC Driver 17 or 18 for SQL Server installed on the host

## Running tests

```bash
export MSSQL_TEST_CONN_STR="DRIVER={ODBC Driver 18 for SQL Server};SERVER=...;..."
pip install -e ".[dev]"
pytest tests/ -v
```

## Research & benchmarks

See [`docs/CONFLUENCE.md`](docs/CONFLUENCE.md) for a full design walkthrough,
Postgres↔MSSQL translation table, benchmark results, and the conclusion on whether
SQL Server is a viable LangGraph backend.
