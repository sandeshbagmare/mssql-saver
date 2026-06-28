"""Benchmark: get_tuple and list() latency vs conversation history depth (MSSQL).

Measures whether query latency grows with the number of checkpoints per thread.
The B-Tree index on checkpoint_id should keep get_tuple at O(log N) regardless.

Usage:
    python -m benchmarks.history_depth
    python -m benchmarks.history_depth --max-turns 200 --n 10
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings

DEPTH_STEPS = [1, 5, 10, 25, 50, 100, 200]


def _conn():
    import pyodbc
    conn_str = settings.mssql_conn_str
    if "MARS_Connection" not in conn_str and "mars_connection" not in conn_str.lower():
        conn_str = conn_str.rstrip(";") + ";MARS_Connection=yes;"
    return pyodbc.connect(conn_str, autocommit=True)


def seed_to_depth(cur, tid: str, depth: int) -> str:
    """Insert `depth` checkpoints for a thread; return the latest checkpoint_id."""
    for i in range(depth):
        cur.execute(
            "INSERT INTO [checkpoints] "
            "(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, "
            "type, [checkpoint], metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                tid, "", f"{i:032}.0",
                f"{i - 1:032}.0" if i > 0 else None,
                "raw", b"\x80", "{}",
            ),
        )
    return f"{depth - 1:032}.0"


def bench_depth(cur, depth: int, n_queries: int) -> dict:
    get_latencies: list[float] = []
    list_latencies: list[float] = []
    tids: list[str] = []

    for _ in range(n_queries):
        tid = f"hist-{uuid.uuid4()}"
        tids.append(tid)
        seed_to_depth(cur, tid, depth)

        # get_tuple equivalent — fetch latest checkpoint
        t0 = time.perf_counter()
        cur.execute(
            "SELECT TOP (1) thread_id, checkpoint_ns, checkpoint_id, [checkpoint] "
            "FROM [checkpoints] WHERE thread_id=? ORDER BY checkpoint_id DESC",
            (tid,),
        )
        _ = cur.fetchone()
        get_latencies.append((time.perf_counter() - t0) * 1000)

        # list() equivalent — scan all checkpoints for thread
        t0 = time.perf_counter()
        cur.execute(
            "SELECT checkpoint_id FROM [checkpoints] WHERE thread_id=? ORDER BY checkpoint_id DESC",
            (tid,),
        )
        _ = cur.fetchall()
        list_latencies.append((time.perf_counter() - t0) * 1000)

    # Cleanup
    for tid in tids:
        cur.execute("DELETE FROM [checkpoints] WHERE thread_id=?", (tid,))

    def pct(lst, p):
        s = sorted(lst)
        return round(s[int(len(s) * p / 100)], 2)

    return {
        "depth": depth,
        "n_queries": n_queries,
        "get_mean_ms": round(statistics.mean(get_latencies), 2),
        "get_p50_ms": round(statistics.median(get_latencies), 2),
        "get_p95_ms": pct(get_latencies, 95),
        "list_mean_ms": round(statistics.mean(list_latencies), 2),
        "list_p50_ms": round(statistics.median(list_latencies), 2),
        "list_p95_ms": pct(list_latencies, 95),
    }


def main():
    parser = argparse.ArgumentParser(description="History depth benchmark")
    parser.add_argument("--max-turns", type=int, default=200,
                        help="Max conversation depth to test")
    parser.add_argument("--n", type=int, default=10,
                        help="Queries per depth step")
    args = parser.parse_args()

    conn = _conn()
    cur = conn.cursor()
    results: list[dict] = []

    steps = [d for d in DEPTH_STEPS if d <= args.max_turns]
    if args.max_turns not in steps:
        steps.append(args.max_turns)

    print(f"\n{'Depth':>8} {'get p50':>10} {'get p95':>10} {'list p50':>10} {'list p95':>10}")
    print("-" * 55)

    for depth in steps:
        r = bench_depth(cur, depth, args.n)
        results.append(r)
        print(
            f"{depth:>8} "
            f"{r['get_p50_ms']:>9.2f}ms "
            f"{r['get_p95_ms']:>9.2f}ms "
            f"{r['list_p50_ms']:>9.2f}ms "
            f"{r['list_p95_ms']:>9.2f}ms"
        )

    conn.close()

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"history_depth_{ts}.json"
    with open(out_file, "w") as f:
        json.dump({"args": vars(args), "results": results}, f, indent=2)
    print(f"\nResults saved to {out_file}")
    return results


if __name__ == "__main__":
    main()
