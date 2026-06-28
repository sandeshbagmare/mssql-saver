"""Benchmark: serialization cost vs payload size (MSSQL).

Measures how checkpoint put/get latency scales with payload size.
Tests payloads: 1 KB, 10 KB, 50 KB, 100 KB, 500 KB, 1 MB.

Usage:
    python -m benchmarks.serialization
    python -m benchmarks.serialization --n 20
"""
from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import string
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings


PAYLOAD_SIZES_KB = [1, 10, 50, 100, 500, 1024]


def _mssql_conn():
    import pyodbc
    conn_str = settings.mssql_conn_str
    if "MARS_Connection" not in conn_str and "mars_connection" not in conn_str.lower():
        conn_str = conn_str.rstrip(";") + ";MARS_Connection=yes;"
    return pyodbc.connect(conn_str, autocommit=False)


def bench_payload(conn, payload_kb: int, n: int) -> dict:
    """Measure put (INSERT) latency for a given payload size."""
    payload = "".join(random.choices(string.ascii_letters + " \n", k=payload_kb * 1024))
    blob = payload.encode("utf-8")

    put_latencies: list[float] = []
    get_latencies: list[float] = []
    thread_ids: list[str] = []

    cur = conn.cursor()

    for i in range(n):
        tid = f"serde-{uuid.uuid4()}"
        thread_ids.append(tid)
        checkpoint_id = f"{i:032}.0"

        # PUT
        t0 = time.perf_counter()
        cur.execute(
            "INSERT INTO [checkpoints] "
            "(thread_id, checkpoint_ns, checkpoint_id, parent_checkpoint_id, "
            "type, [checkpoint], metadata) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (tid, "", checkpoint_id, None, "raw", blob, "{}"),
        )
        conn.commit()
        put_latencies.append((time.perf_counter() - t0) * 1000)

        # GET
        t0 = time.perf_counter()
        cur.execute(
            "SELECT TOP (1) [checkpoint] FROM [checkpoints] "
            "WHERE thread_id=? ORDER BY checkpoint_id DESC",
            (tid,),
        )
        row = cur.fetchone()
        _ = bytes(row[0]) if row else b""
        get_latencies.append((time.perf_counter() - t0) * 1000)

    # Cleanup
    for tid in thread_ids:
        cur.execute("DELETE FROM [checkpoints] WHERE thread_id=?", (tid,))
    conn.commit()

    def pct(lst, p):
        s = sorted(lst)
        return round(s[int(len(s) * p / 100)], 2)

    return {
        "payload_kb": payload_kb,
        "n": n,
        "put_mean_ms": round(statistics.mean(put_latencies), 2),
        "put_p50_ms": round(statistics.median(put_latencies), 2),
        "put_p95_ms": pct(put_latencies, 95),
        "put_max_ms": round(max(put_latencies), 2),
        "get_mean_ms": round(statistics.mean(get_latencies), 2),
        "get_p50_ms": round(statistics.median(get_latencies), 2),
        "get_p95_ms": pct(get_latencies, 95),
        "get_max_ms": round(max(get_latencies), 2),
        "throughput_rps": round(n / (sum(put_latencies) / 1000), 1),
    }


def main():
    parser = argparse.ArgumentParser(description="Serialization / payload size benchmark")
    parser.add_argument("--n", type=int, default=20, help="Iterations per payload size")
    args = parser.parse_args()

    conn = _mssql_conn()
    results: list[dict] = []

    print(f"\n{'Payload':>10} {'put p50':>10} {'put p95':>10} {'get p50':>10} {'get p95':>10} {'rps':>8}")
    print("-" * 65)

    for kb in PAYLOAD_SIZES_KB:
        r = bench_payload(conn, kb, args.n)
        results.append(r)
        print(
            f"{kb:>8} KB "
            f"{r['put_p50_ms']:>9.2f}ms "
            f"{r['put_p95_ms']:>9.2f}ms "
            f"{r['get_p50_ms']:>9.2f}ms "
            f"{r['get_p95_ms']:>9.2f}ms "
            f"{r['throughput_rps']:>7.1f}"
        )

    conn.close()

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"serialization_{ts}.json"
    with open(out_file, "w") as f:
        json.dump({"args": vars(args), "results": results}, f, indent=2)
    print(f"\nResults saved to {out_file}")
    return results


if __name__ == "__main__":
    main()
