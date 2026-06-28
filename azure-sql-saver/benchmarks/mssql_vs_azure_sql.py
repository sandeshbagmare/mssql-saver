"""Benchmark: MSSQL vs Azure SQL (simulated) — side-by-side comparison.

Runs identical workloads against both the mssql-saver and azure-sql-saver
libraries to demonstrate that they produce identical results (since they
target the same T-SQL engine).

Usage:
    python benchmarks/mssql_vs_azure_sql.py
"""
from __future__ import annotations

import json
import os
import statistics
import time
import uuid
import threading
import random
import string

from langgraph.checkpoint.base import empty_checkpoint

MSSQL_CONN_STR = os.environ.get(
    "MSSQL_CONN_STR",
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=.;DATABASE=langgraph;"
    "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    "MARS_Connection=yes;",
)

AZURE_SQL_CONN_STR = os.environ.get(
    "AZURE_SQL_CONN_STR",
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=.;DATABASE=langgraph_azure;"
    "Trusted_Connection=yes;Encrypt=yes;TrustServerCertificate=yes;"
    "MARS_Connection=yes;",
)


def _checkpoint(idx: int = 0) -> dict:
    c = empty_checkpoint()
    c["channel_values"] = {"counter": idx, "data": f"value_{idx}"}
    c["channel_versions"] = {"counter": f"{idx + 1:032}.0", "data": f"{idx + 1:032}.1"}
    return c


def _config(tid: str) -> dict:
    return {"configurable": {"thread_id": tid, "checkpoint_ns": ""}}


def run_sequential_benchmark(saver, label: str, n: int = 200):
    """Sequential put+get for n invocations."""
    latencies = []
    tid = f"seq-{label}-{uuid.uuid4()}"
    config = _config(tid)

    for i in range(n):
        ckpt = _checkpoint(i)
        t0 = time.perf_counter()
        cfg = saver.put(config, ckpt, {"step": i, "source": "loop"}, ckpt["channel_versions"])
        result = saver.get_tuple(cfg)
        latencies.append((time.perf_counter() - t0) * 1000)

    saver.delete_thread(tid)

    return {
        "label": label,
        "type": "sequential",
        "n": n,
        "mean_ms": round(statistics.mean(latencies), 2),
        "p50_ms": round(statistics.median(latencies), 2),
        "p95_ms": round(sorted(latencies)[int(n * 0.95)], 2),
        "max_ms": round(max(latencies), 2),
        "rps": round(n / (sum(latencies) / 1000), 1),
    }


def run_concurrent_benchmark(saver, label: str, n: int = 100, workers: int = 10):
    """Concurrent writes with multiple threads."""
    latencies = []
    errors = []
    lock = threading.Lock()

    def worker():
        tid = f"conc-{label}-{uuid.uuid4()}"
        config = _config(tid)
        per_worker = n // workers
        for i in range(per_worker):
            ckpt = _checkpoint(i)
            try:
                t0 = time.perf_counter()
                saver.put(config, ckpt, {"step": i}, ckpt["channel_versions"])
                lat = (time.perf_counter() - t0) * 1000
                with lock:
                    latencies.append(lat)
            except Exception as e:
                with lock:
                    errors.append(str(e))
        try:
            saver.delete_thread(tid)
        except Exception:
            pass

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    t_start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_time = time.perf_counter() - t_start

    return {
        "label": label,
        "type": "concurrent",
        "n": n,
        "workers": workers,
        "mean_ms": round(statistics.mean(latencies), 2) if latencies else 0,
        "p50_ms": round(statistics.median(latencies), 2) if latencies else 0,
        "p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 2) if latencies else 0,
        "max_ms": round(max(latencies), 2) if latencies else 0,
        "rps": round(len(latencies) / total_time, 1),
        "errors": len(errors),
    }


def run_large_payload_benchmark(saver, label: str, payload_kb: int = 50, n: int = 20):
    """Benchmark with large payloads."""
    large_text = "".join(random.choices(string.ascii_letters + " ", k=payload_kb * 1024))
    latencies = []
    tid = f"payload-{label}-{uuid.uuid4()}"
    config = _config(tid)

    for i in range(n):
        c = empty_checkpoint()
        c["channel_values"] = {"text": large_text, "counter": i}
        c["channel_versions"] = {"text": f"{i + 1:032}.0", "counter": f"{i + 1:032}.1"}
        t0 = time.perf_counter()
        saver.put(config, c, {"step": i}, c["channel_versions"])
        latencies.append((time.perf_counter() - t0) * 1000)

    saver.delete_thread(tid)

    return {
        "label": label,
        "type": "large_payload",
        "payload_kb": payload_kb,
        "n": n,
        "mean_ms": round(statistics.mean(latencies), 2),
        "p50_ms": round(statistics.median(latencies), 2),
        "max_ms": round(max(latencies), 2),
    }


def run_history_benchmark(saver, label: str, turns: int = 50):
    """Benchmark get_tuple and list performance as history grows."""
    tid = f"history-{label}-{uuid.uuid4()}"
    config = _config(tid)
    put_latencies = []
    get_latencies = []

    for i in range(turns):
        ckpt = _checkpoint(i)
        t0 = time.perf_counter()
        cfg = saver.put(config, ckpt, {"step": i}, ckpt["channel_versions"])
        put_latencies.append((time.perf_counter() - t0) * 1000)

        t0 = time.perf_counter()
        saver.get_tuple(cfg)
        get_latencies.append((time.perf_counter() - t0) * 1000)

    # List all checkpoints
    t0 = time.perf_counter()
    all_cps = list(saver.list(config))
    list_time_ms = (time.perf_counter() - t0) * 1000

    saver.delete_thread(tid)

    return {
        "label": label,
        "type": "history",
        "turns": turns,
        "put_p50_ms": round(statistics.median(put_latencies), 2),
        "get_p50_first10": round(statistics.median(get_latencies[:10]), 2),
        "get_p50_last10": round(statistics.median(get_latencies[-10:]), 2),
        "list_all_ms": round(list_time_ms, 2),
        "total_checkpoints": len(all_cps),
    }


def main():
    from langgraph_checkpoint_mssql import MssqlSaver
    from langgraph_checkpoint_azure_sql import AzureSqlSaver

    print("=" * 70)
    print("BENCHMARK: MSSQL vs Azure SQL (Local Simulation)")
    print("=" * 70)

    mssql_saver = MssqlSaver(MSSQL_CONN_STR, pool_size=20)
    mssql_saver.setup()

    azure_saver = AzureSqlSaver(AZURE_SQL_CONN_STR, pool_size=20)
    azure_saver.setup()

    results = []

    # --- Sequential ---
    print("\n[1/4] Sequential benchmark (n=500)...")
    for saver, label in [(mssql_saver, "MSSQL"), (azure_saver, "AzureSQL")]:
        r = run_sequential_benchmark(saver, label, n=500)
        results.append(r)
        print(f"  {label}: p50={r['p50_ms']}ms  rps={r['rps']}")

    # --- Concurrent ---
    print("\n[2/4] Concurrent benchmark (n=200, w=10)...")
    for saver, label in [(mssql_saver, "MSSQL"), (azure_saver, "AzureSQL")]:
        r = run_concurrent_benchmark(saver, label, n=200, workers=10)
        results.append(r)
        print(f"  {label}: p50={r['p50_ms']}ms  rps={r['rps']}  errors={r['errors']}")

    # --- Large Payload ---
    print("\n[3/4] Large payload benchmark (50KB, n=20)...")
    for saver, label in [(mssql_saver, "MSSQL"), (azure_saver, "AzureSQL")]:
        r = run_large_payload_benchmark(saver, label, payload_kb=50, n=20)
        results.append(r)
        print(f"  {label}: p50={r['p50_ms']}ms  max={r['max_ms']}ms")

    # --- History depth ---
    print("\n[4/4] History depth benchmark (50 turns)...")
    for saver, label in [(mssql_saver, "MSSQL"), (azure_saver, "AzureSQL")]:
        r = run_history_benchmark(saver, label, turns=50)
        results.append(r)
        print(f"  {label}: get_first10={r['get_p50_first10']}ms  get_last10={r['get_p50_last10']}ms  list_all={r['list_all_ms']}ms")

    # Save results
    out_dir = os.path.join(os.path.dirname(__file__), "results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "mssql_vs_azure_sql.json")
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {out_path}")

    # Summary table
    print("\n" + "=" * 70)
    print("SUMMARY TABLE")
    print("=" * 70)
    print(f"{'Scenario':<30} {'MSSQL p50':>12} {'Azure p50':>12} {'Diff':>10}")
    print("-" * 70)

    seq_results = [r for r in results if r["type"] == "sequential"]
    if len(seq_results) == 2:
        m, a = seq_results
        diff = ((a["p50_ms"] - m["p50_ms"]) / m["p50_ms"] * 100) if m["p50_ms"] else 0
        print(f"{'Sequential (n=500)':<30} {m['p50_ms']:>10}ms {a['p50_ms']:>10}ms {diff:>+8.1f}%")

    conc_results = [r for r in results if r["type"] == "concurrent"]
    if len(conc_results) == 2:
        m, a = conc_results
        diff = ((a["p50_ms"] - m["p50_ms"]) / m["p50_ms"] * 100) if m["p50_ms"] else 0
        print(f"{'Concurrent (n=200,w=10)':<30} {m['p50_ms']:>10}ms {a['p50_ms']:>10}ms {diff:>+8.1f}%")

    pay_results = [r for r in results if r["type"] == "large_payload"]
    if len(pay_results) == 2:
        m, a = pay_results
        diff = ((a["p50_ms"] - m["p50_ms"]) / m["p50_ms"] * 100) if m["p50_ms"] else 0
        print(f"{'50KB Payload (n=20)':<30} {m['p50_ms']:>10}ms {a['p50_ms']:>10}ms {diff:>+8.1f}%")

    hist_results = [r for r in results if r["type"] == "history"]
    if len(hist_results) == 2:
        m, a = hist_results
        diff = ((a["get_p50_last10"] - m["get_p50_last10"]) / m["get_p50_last10"] * 100) if m["get_p50_last10"] else 0
        print(f"{'History (50 turns, get)':<30} {m['get_p50_last10']:>10}ms {a['get_p50_last10']:>10}ms {diff:>+8.1f}%")

    print()
    mssql_saver.pool.close()
    azure_saver.pool.close()


if __name__ == "__main__":
    main()
