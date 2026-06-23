"""Stress / latency benchmark: PG vs MSSQL checkpointer.

Usage (from repo root, after both DBs are running):
    python -m benchmarks.stress
    python -m benchmarks.stress --n 100 --workers 10
    python -m benchmarks.stress --n 1000 --workers 20

Results are written to benchmarks/results/stress_<timestamp>.json
and printed as a markdown table to stdout.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import threading
import uuid
from datetime import datetime
from pathlib import Path

# Allow running from repo root without installing the app package
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.graph.builder import build_graph
from app.services.checkpointer_factory import get_checkpointer


SAMPLE_TEXT = (
    "LangGraph is a library for building stateful, multi-actor applications with LLMs. "
    "It extends LangChain by providing a graph-based orchestration framework. "
    "Each node in the graph represents a computation step. "
    "The checkpointer persists state between steps, enabling resumable workflows."
)


def run_single(backend: str, checkpointer, text: str, thread_id: str) -> float:
    """Run one graph invocation; return wall-clock latency in ms."""
    graph = build_graph(checkpointer)
    config = {"configurable": {"thread_id": thread_id}}
    initial_state = {
        "text": text, "normalised": "", "word_count": 0,
        "char_count": 0, "sentence_count": 0, "summary": "",
    }
    t0 = time.perf_counter()
    graph.invoke(initial_state, config)
    return (time.perf_counter() - t0) * 1000


def bench_sequential(backend: str, n: int) -> list[float]:
    """n sequential invocations, each on its own thread_id."""
    checkpointer = get_checkpointer(backend)
    latencies = []
    for i in range(n):
        tid = f"seq-{uuid.uuid4()}"
        latencies.append(run_single(backend, checkpointer, SAMPLE_TEXT, tid))
    return latencies


def bench_concurrent(backend: str, n: int, workers: int) -> list[float]:
    """n invocations spread across *workers* threads."""
    checkpointer = get_checkpointer(backend)
    latencies: list[float] = [0.0] * n
    errors: list[Exception] = []
    lock = threading.Lock()
    indices = list(range(n))
    idx_iter = iter(indices)

    def worker():
        while True:
            with lock:
                try:
                    i = next(idx_iter)
                except StopIteration:
                    return
            tid = f"conc-{uuid.uuid4()}"
            try:
                latencies[i] = run_single(backend, checkpointer, SAMPLE_TEXT, tid)
            except Exception as e:
                errors.append(e)
                latencies[i] = -1.0

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        print(f"  [warn] {len(errors)} errors during concurrent bench: {errors[:3]}")

    return [l for l in latencies if l >= 0]


def stats(latencies: list[float]) -> dict:
    if not latencies:
        return {}
    s = sorted(latencies)
    n = len(s)
    return {
        "n": n,
        "mean":  round(statistics.mean(s), 2),
        "p50":   round(s[int(n * 0.50)], 2),
        "p95":   round(s[int(n * 0.95)], 2),
        "p99":   round(s[int(n * 0.99)], 2),
        "max":   round(s[-1], 2),
        "throughput_rps": round(n / (sum(latencies) / 1000), 1),
    }


def print_table(results: dict) -> None:
    header = f"{'Scenario':<35} {'n':>6} {'mean':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8} {'rps':>8}"
    print("\n" + header)
    print("-" * len(header))
    for label, s in results.items():
        print(
            f"{label:<35} {s['n']:>6} {s['mean']:>8.1f} {s['p50']:>8.1f} "
            f"{s['p95']:>8.1f} {s['p99']:>8.1f} {s['max']:>8.1f} {s['throughput_rps']:>8.1f}"
        )
    print("(all latencies in ms)\n")


def main():
    parser = argparse.ArgumentParser(description="Checkpoint saver stress benchmark")
    parser.add_argument("--n", type=int, default=100, help="Number of invocations per scenario")
    parser.add_argument("--workers", type=int, default=10, help="Concurrent worker threads")
    args = parser.parse_args()

    results = {}
    backends = ["postgres", "mssql"]

    for backend in backends:
        print(f"\n{'='*50}")
        print(f" Backend: {backend.upper()}")
        print(f"{'='*50}")

        print(f"  Sequential n={args.n}...")
        lats = bench_sequential(backend, args.n)
        results[f"{backend} sequential n={args.n}"] = stats(lats)

        print(f"  Concurrent n={args.n} workers={args.workers}...")
        lats = bench_concurrent(backend, args.n, args.workers)
        results[f"{backend} concurrent n={args.n} w={args.workers}"] = stats(lats)

        if args.n >= 100:
            big_n = min(args.n * 10, 1000)
            print(f"  Sequential n={big_n}...")
            lats = bench_sequential(backend, big_n)
            results[f"{backend} sequential n={big_n}"] = stats(lats)

            print(f"  Concurrent n={big_n} workers={args.workers * 2}...")
            lats = bench_concurrent(backend, big_n, args.workers * 2)
            results[f"{backend} concurrent n={big_n} w={args.workers * 2}"] = stats(lats)

    print_table(results)

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = out_dir / f"stress_{ts}.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Results saved to {out_file}")
    return results


if __name__ == "__main__":
    main()
