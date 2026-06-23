"""Aggregate all benchmark results into markdown tables.

Usage:
    python -m benchmarks.report
    python -m benchmarks.report --latest   (use only most recent result files)
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


RESULTS_DIR = Path(__file__).parent / "results"


def load_latest(pattern: str) -> dict | None:
    files = sorted(RESULTS_DIR.glob(pattern))
    if not files:
        return None
    with open(files[-1]) as f:
        return json.load(f)


def markdown_stress(data: dict) -> str:
    lines = [
        "## Latency Benchmark Results\n",
        "All latencies in **milliseconds**. Throughput in **requests/second**.\n",
        "| Scenario | n | mean | p50 | p95 | p99 | max | rps |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for label, s in data.items():
        lines.append(
            f"| {label} | {s['n']} | {s['mean']} | {s['p50']} | "
            f"{s['p95']} | {s['p99']} | {s['max']} | {s['throughput_rps']} |"
        )
    return "\n".join(lines)


def markdown_sizes(data: dict) -> str:
    lines = [
        "## Database Size Comparison\n",
        "| Table | PG rows | PG size | MSSQL rows | MSSQL size |",
        "|---|---|---|---|---|",
    ]
    all_tables = sorted({t for backend in data.values() for t in backend})
    for table in all_tables:
        pg = data.get("postgres", {}).get(table, {})
        ms = data.get("mssql", {}).get(table, {})

        def fmt_size(b):
            if b is None:
                return "—"
            if b < 1_048_576:
                return f"{b/1024:.1f} KB"
            return f"{b/1_048_576:.2f} MB"

        lines.append(
            f"| {table} | {pg.get('row_count', '—')} | {fmt_size(pg.get('size_bytes'))} "
            f"| {ms.get('row_count', '—')} | {fmt_size(ms.get('size_bytes'))} |"
        )
    return "\n".join(lines)


def markdown_correctness(data: dict) -> str:
    lines = [
        "## Correctness / Concurrency Verification\n",
        "| Backend | Threads | Invocations | Result |",
        "|---|---|---|---|",
    ]
    for backend, r in data.items():
        status = "✅ PASS" if r["passed"] else f"❌ FAIL ({len(r['errors'])} errors)"
        lines.append(
            f"| {backend} | {r['threads']} | {r['total_invocations']} | {status} |"
        )
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(RESULTS_DIR / "REPORT.md"))
    args = parser.parse_args()

    sections = ["# Benchmark Report\n"]

    stress = load_latest("stress_*.json")
    if stress:
        sections.append(markdown_stress(stress))
    else:
        sections.append("*No stress results found. Run `python -m benchmarks.stress` first.*\n")

    sizes = load_latest("db_size_*.json")
    if sizes:
        sections.append(markdown_sizes(sizes))
    else:
        sections.append("*No size results found. Run `python -m benchmarks.db_size` first.*\n")

    correctness = load_latest("correctness_*.json")
    if correctness:
        sections.append(markdown_correctness(correctness))
    else:
        sections.append("*No correctness results found.*\n")

    report = "\n\n".join(sections)
    print(report)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as f:
        f.write(report)
    print(f"\nReport written to {args.out}")


if __name__ == "__main__":
    main()
