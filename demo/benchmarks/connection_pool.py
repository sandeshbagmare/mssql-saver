"""Benchmark: connection pool size vs throughput (MSSQL).

Measures throughput at different pool sizes (1, 5, 10, 20, 50)
under fixed concurrent load (20 worker threads, 200 total ops).

Usage:
    python -m benchmarks.connection_pool
    python -m benchmarks.connection_pool --workers 40 --ops 400
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings

POOL_SIZES = [1, 5, 10, 20, 50]


def _make_pool(pool_size: int):
    from mssql_saver.src.langgraph_checkpoint_mssql.pool import ConnectionPool
    return ConnectionPool(settings.mssql_conn_str, pool_size=pool_size)


def bench_pool_size(pool_size: int, workers: int, total_ops: int) -> dict:
    """Run total_ops across workers threads with a pool of given size."""
    import pyodbc

    conn_str = settings.mssql_conn_str
    if "MARS_Connection" not in conn_str and "mars_connection" not in conn_str.lower():
        conn_str = conn_str.rstrip(";") + ";MARS_Connection=yes;"

    # Build a simple pool
    pool: list = []
    lock = threading.Lock()
    sem = threading.Semaphore(pool_size)

    def acquire():
        sem.acquire()
        with lock:
            if pool:
                return pool.pop()
        return pyodbc.connect(conn_str, autocommit=False)

    def release(conn):
        try:
            conn.commit()
        except Exception:
            pass
        with lock:
            if len(pool) < pool_size:
                pool.append(conn)
                sem.release()
                return
        conn.close()
        sem.release()

    latencies: list[float] = []
    errors: list[str] = []
    op_lock = threading.Lock()
    ops_remaining = [total_ops]

    def worker():
        while True:
            with op_lock:
                if ops_remaining[0] <= 0:
                    return
                ops_remaining[0] -= 1

            tid = f"pool-{uuid.uuid4()}"
            conn = acquire()
            try:
                t0 = time.perf_counter()
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO [checkpoints] "
                    "(thread_id, checkpoint_ns, checkpoint_id, type, [checkpoint], metadata) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (tid, "", f"{0:032}.0", "raw", b"\x80", "{}"),
                )
                conn.commit()
                cur.execute("DELETE FROM [checkpoints] WHERE thread_id=?", (tid,))
                conn.commit()
                lat = (time.perf_counter() - t0) * 1000
                with op_lock:
                    latencies.append(lat)
            except Exception as e:
                with op_lock:
                    errors.append(str(e))
                try:
                    conn.rollback()
                except Exception:
                    pass
            finally:
                release(conn)

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time = time.perf_counter() - t_start

    # Close pool
    with lock:
        for c in pool:
            try:
                c.close()
            except Exception:
                pass
        pool.clear()

    if not latencies:
        return {"pool_size": pool_size, "workers": workers, "ops": total_ops, "errors": len(errors)}

    s = sorted(latencies)
    n = len(s)
    return {
        "pool_size": pool_size,
        "workers": workers,
        "ops": n,
        "errors": len(errors),
        "mean_ms": round(statistics.mean(s), 2),
        "p50_ms": round(s[int(n * 0.50)], 2),
        "p95_ms": round(s[int(n * 0.95)], 2),
        "p99_ms": round(s[int(n * 0.99)], 2),
        "max_ms": round(s[-1], 2),
        "throughput_rps": round(n / total_time, 1),
        "total_time_s": round(total_time, 2),
    }


def main():
    parser = argparse.ArgumentParser(description="Connection pool size benchmark")
    parser.add_argument("--workers", type=int, default=20, help="Concurrent worker threads")
    parser.add_argument("--ops", type=int, default=200, help="Total operations")
    args = parser.parse_args()

    results: list[dict] = []

    print(f"\nConnection pool benchmark: {args.workers} workers, {args.ops} ops\n")
    print(f"{'Pool size':>10} {'ops':>6} {'p50 ms':>8} {'p95 ms':>8} {'rps':>8} {'errors':>8}")
    print("-" * 60)

    for ps in POOL_SIZES:
        r = bench_pool_size(ps, args.workers, args.ops)
        results.append(r)
        print(
            f"{ps:>10} "
            f"{r.get('ops', 0):>6} "
            f"{r.get('p50_ms', 0):>7.1f}ms "
            f"{r.get('p95_ms', 0):>7.1f}ms "
            f"{r.get('throughput_rps', 0):>7.1f} "
            f"{r.get('errors', 0):>8}"
        )

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"connection_pool_{ts}.json"
    with open(out_file, "w") as f:
        json.dump({"args": vars(args), "results": results}, f, indent=2)
    print(f"\nResults saved to {out_file}")
    return results


if __name__ == "__main__":
    main()
