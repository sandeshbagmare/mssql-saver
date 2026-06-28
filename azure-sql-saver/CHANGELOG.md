# Changelog

## 0.1.0 — 2026-06-25

### Added
- `AzureSqlSaver` — sync + async LangGraph checkpoint saver for Azure SQL Database
- `AsyncAzureSqlSaver` — alias for `AzureSqlSaver` (same class supports both)
- Thread-safe `ConnectionPool` with Azure SQL detection (`is_azure` property)
- 4-table schema: `checkpoints`, `checkpoint_blobs`, `checkpoint_writes`, `checkpoint_migrations`
- Upserts via UPDATE-then-INSERT with UPDLOCK/HOLDLOCK (avoids MERGE concurrency bugs)
- Versioned migration system with idempotent DDL
- Support for Azure AD and Managed Identity authentication
- 15 conformance tests passing
- Benchmark suite: MSSQL vs Azure SQL side-by-side comparison
- Full design documentation (`docs/AZURE_SQL_GUIDE.md`)
