"""Benchmark: checkpoint pruning / DELETE performance (MSSQL).

Seeds the DB with N threads × M turns, then measures:
- Bulk DELETE-by-thread latency
- Filtered DELETE (age > threshold) latency
- Table size before and after

Usage:
    python -m benchmarks.pruning
    python -m benchmarks.pruning --threads 200 --turns 20
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings


def _conn():
    import pyodbc
    conn_str = settings.mssql_conn_str
    if "MARS_Connection" not in conn_str and "mars_connection" not in conn_str.lower():
        conn_str = conn_str.rstrip(";") + ";MARS_Connection=yes;"
    return pyodbc.connect(conn_str, autocommit=True)


def _count(cur, table: str, prefix: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM [{table}] WHERE thread_id LIKE ?", (f"{prefix}%",))
    row = cur.fetchone()
    return row[0] if row else 0


def _size_kb(cur, table: str) -> float:
    cur.execute(f"EXEC sp_spaceused '{table}'")
    row = cur.fetchone()
    if row:
        return int(row[2].strip().replace(" KB", ""))
    return 0.0


def seed(cur, threads: int, turns: int, prefix: str) -> list[str]:
    thread_ids = [f"{prefix}-{uuid.uuid4()}" for _ in range(threads)]
    for tid in thread_ids:
        for i in range(turns):
            cur.execute(
                "INSERT INTO [checkpoints] "
                "(thread_id, checkpoint_ns, checkpoint_id, type, [checkpoint], metadata) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (tid, "", f"{i:032}.0", "raw", b"\x80", "{}"),
            )
    return thread_ids


def main():
    parser = argparse.ArgumentParser(description="Pruning / DELETE benchmark")
    parser.add_argument("--threads", type=int, default=100, help="Threads to seed")
    parser.add_argument("--turns", type=int, default=20, help="Checkpoints per thread")
    args = parser.parse_args()

    conn = _conn()
    cur = conn.cursor()
    results: dict = {}
    prefix = "prune"

    total_rows = args.threads * args.turns
    print(f"Seeding {args.threads} threads × {args.turns} turns = {total_rows:,} rows…")
    thread_ids = seed(cur, args.threads, args.turns, prefix)

    before_count = _count(cur, "checkpoints", prefix)
    before_size = _size_kb(cur, "checkpoints")
    results["before"] = {"rows": before_count, "size_kb": before_size}
    print(f"  Seeded: {before_count:,} rows  ({before_size:.0f} KB)")

    # 1. Single-thread DELETE (per-thread cleanup, as used by delete_thread())
    sample_tid = thread_ids[0]
    t0 = time.perf_counter()
    cur.execute("DELETE FROM [checkpoints] WHERE thread_id=?", (sample_tid,))
    single_delete_ms = (time.perf_counter() - t0) * 1000
    results["single_thread_delete_ms"] = round(single_delete_ms, 2)
    print(f"  Single-thread DELETE: {single_delete_ms:.2f} ms")

    # 2. Bulk DELETE — all remaining seeded threads in one batch
    remaining_ids = thread_ids[1:]
    params = ",".join(["?"] * len(remaining_ids))
    t0 = time.perf_counter()
    cur.execute(
        f"DELETE FROM [checkpoints] WHERE thread_id IN ({params})",
        remaining_ids,
    )
    bulk_delete_ms = (time.perf_counter() - t0) * 1000
    results["bulk_delete_ms"] = round(bulk_delete_ms, 2)
    results["bulk_delete_rows"] = len(remaining_ids) * args.turns
    print(f"  Bulk DELETE ({len(remaining_ids) * args.turns:,} rows): {bulk_delete_ms:.2f} ms")

    after_count = _count(cur, "checkpoints", prefix)
    after_size = _size_kb(cur, "checkpoints")
    results["after"] = {"rows": after_count, "size_kb": after_size}
    print(f"  After: {after_count} rows  ({after_size:.0f} KB)")

    # 3. Filtered DELETE — show SQL for age-based pruning (dry run; no actual old rows)
    results["pruning_sql_example"] = (
        "DELETE FROM [checkpoint_writes] WHERE thread_id IN "
        "(SELECT DISTINCT thread_id FROM [checkpoints] "
        " WHERE TRY_CAST(JSON_VALUE(metadata, '$.created_at') AS DATETIME2) "
        "   < DATEADD(DAY, -30, GETUTCDATE()));\n"
        "DELETE FROM [checkpoint_blobs]  WHERE thread_id NOT IN (SELECT DISTINCT thread_id FROM [checkpoints]);\n"
        "DELETE FROM [checkpoints] WHERE TRY_CAST(JSON_VALUE(metadata, '$.created_at') AS DATETIME2) "
        "   < DATEADD(DAY, -30, GETUTCDATE());"
    )

    conn.close()

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"pruning_{ts}.json"
    with open(out_file, "w") as f:
        json.dump({"args": vars(args), "results": results}, f, indent=2)
    print(f"\nResults saved to {out_file}")
    return results


if __name__ == "__main__":
    main()
