# Changelog

## 0.1.0 (2026-06-23)

### Added
- Initial release of `langgraph-checkpoint-mssql`
- `MssqlSaver` / `AsyncMssqlSaver` implementing the `BaseCheckpointSaver` interface
  against `langgraph-checkpoint 4.1.x`
- 3-table schema mirroring `langgraph-checkpoint-postgres`:
  `checkpoints`, `checkpoint_blobs`, `checkpoint_writes`, `checkpoint_migrations`
- Thread-safe `ConnectionPool` backed by `pyodbc` + ODBC Driver 18
- Async support via `asyncio.to_thread` (avoids less-maintained `aioodbc`)
- Fully parameterised SQL (no string-concatenated limits — CVE-2025-67644-safe)
- Upserts via UPDATE-then-INSERT with UPDLOCK/HOLDLOCK (avoids MERGE concurrency bugs)
- Versioned migration system with idempotent DDL
- Conformance test suite + benchmark harness (see `docs/BENCHMARKS.md`)
- Detailed conference/research page (`docs/CONFERENCE.md`)
