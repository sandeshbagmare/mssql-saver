"""Benchmark: checkpoint table growth over time (MSSQL).

Measures how checkpoint counts, table sizes, and query latency change as
the number of threads and per-thread history depth increase.

Usage:
    python -m benchmarks.memory_growth
    python -m benchmarks.memory_growth --threads 200 --turns 50

Results are written to benchmarks/results/memory_growth_<ts>.json
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings


def _mssql_conn():
    import pyodbc
    conn_str = settings.mssql_conn_str
    if "MARS_Connection" not in conn_str and "mars_connection" not in conn_str.lower():
        conn_str = conn_str.rstrip(";") + ";MARS_Connection=yes;"
    return pyodbc.connect(conn_str, autocommit=True)


def _table_row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM [{table}]")
    row = cur.fetchone()
    return row[0] if row else 0


def _table_size_kb(cur, table: str) -> float:
    cur.execute(f"EXEC sp_spaceused '{table}'")
    row = cur.fetchone()
    if row:
        reserved_kb = int(row[2].strip().replace(" KB", ""))
        return reserved_kb
    return 0.0


def _list_latency_ms(cur, thread_id: str) -> float:
    t0 = time.perf_counter()
    cur.execute(
        "SELECT checkpoint_id FROM [checkpoints] WHERE thread_id=? ORDER BY checkpoint_id DESC",
        (thread_id,),
    )
    _ = cur.fetchall()
    return (time.perf_counter() - t0) * 1000


def seed_thread(cur, thread_id: str, turns: int) -> None:
    """Insert `turns` fake checkpoint rows for a thread."""
    for i in range(turns):
        checkpoint_id = f"{i:032}.0"
        cur.execute(
            "INSERT INTO [checkpoints] "
            "(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, "
            "type, [checkpoint], metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                thread_id, "", checkpoint_id,
                f"{i - 1:032}.0" if i > 0 else None,
                "msgpack", b"\x80",
                "{}",
            ),
        )


def main():
    parser = argparse.ArgumentParser(description="Memory / table-growth benchmark")
    parser.add_argument("--threads", type=int, default=100, help="Number of simulated threads")
    parser.add_argument("--turns", type=int, default=20, help="Checkpoints per thread")
    args = parser.parse_args()

    conn = _mssql_conn()
    cur = conn.cursor()

    print(f"Seeding {args.threads} threads × {args.turns} turns "
          f"({args.threads * args.turns:,} checkpoint rows)…")

    thread_ids: list[str] = []
    snapshots: list[dict] = []

    batch = 10
    for b in range(0, args.threads, batch):
        batch_ids = [f"growth-{uuid.uuid4()}" for _ in range(min(batch, args.threads - b))]
        thread_ids.extend(batch_ids)
        for tid in batch_ids:
            seed_thread(cur, tid, args.turns)

        done = min(b + batch, args.threads)
        snap: dict = {"threads": done, "checkpoints": done * args.turns}
        snap["row_counts"] = {
            t: _table_row_count(cur, t)
            for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes")
        }
        snap["sizes_kb"] = {
            t: _table_size_kb(cur, t)
            for t in ("checkpoints", "checkpoint_blobs", "checkpoint_writes")
        }
        # list-query latency for the most recent thread
        snap["list_latency_ms"] = _list_latency_ms(cur, batch_ids[-1])
        snapshots.append(snap)

        print(
            f"  threads={done:>5}  rows={snap['row_counts']['checkpoints']:>7}  "
            f"size={snap['sizes_kb']['checkpoints']:>8.0f} KB  "
            f"list_lat={snap['list_latency_ms']:.2f} ms"
        )

    # Cleanup
    print("\nCleaning up seeded rows…")
    for tid in thread_ids:
        cur.execute("DELETE FROM [checkpoints] WHERE thread_id=?", (tid,))
    conn.close()

    # Persist results
    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"memory_growth_{ts}.json"
    with open(out_file, "w") as f:
        json.dump({"args": vars(args), "snapshots": snapshots}, f, indent=2)
    print(f"Results saved to {out_file}")
    return {"snapshots": snapshots}


if __name__ == "__main__":
    main()
