"""Master benchmark runner: run every suite and produce a single JSON summary.

Usage:
    python -m benchmarks.run_all                    # default settings
    python -m benchmarks.run_all --quick            # fast smoke-test (small N)
    python -m benchmarks.run_all --stress           # full stress (large N)
    python -m benchmarks.run_all --out results.json

Suites run:
  1. stress          (latency + throughput, PG vs MSSQL)
  2. db_size         (table sizes)
  3. correctness     (concurrent write verification)
  4. serialization   (payload size scaling)
  5. history_depth   (get/list latency vs turn depth)
  6. memory_growth   (table growth, list latency scaling)
  7. pruning         (DELETE performance)

Results are written per-suite to benchmarks/results/ AND aggregated into
the --out file (default: benchmarks/results/FULL_REPORT_<ts>.json).
"""
from __future__ import annotations

import argparse
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()


def run_suite(name: str, fn, *args, **kwargs) -> dict:
    print(f"\n{'='*60}")
    print(f"  Running: {name}")
    print(f"{'='*60}")
    try:
        result = fn(*args, **kwargs)
        return {"suite": name, "status": "ok", "result": result}
    except Exception as e:
        traceback.print_exc()
        return {"suite": name, "status": "error", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Run all benchmark suites")
    parser.add_argument("--quick", action="store_true", help="Small N for fast runs")
    parser.add_argument("--stress", action="store_true", help="Large N for thorough runs")
    parser.add_argument("--out", default="", help="Output path for combined JSON")
    args = parser.parse_args()

    # Import suite mains after path setup
    from benchmarks.stress import main as stress_main
    from benchmarks.db_size import main as db_size_main
    from benchmarks.correctness import main as correctness_main
    from benchmarks.serialization import main as serialization_main
    from benchmarks.history_depth import main as history_depth_main
    from benchmarks.memory_growth import main as memory_growth_main
    from benchmarks.pruning import main as pruning_main

    # Determine scale
    if args.quick:
        stress_n, stress_w = 20, 4
        ser_n = 5
        hist_max, hist_n = 50, 5
        growth_threads, growth_turns = 20, 5
        prune_threads, prune_turns = 20, 5
    elif args.stress:
        stress_n, stress_w = 500, 30
        ser_n = 50
        hist_max, hist_n = 200, 20
        growth_threads, growth_turns = 500, 50
        prune_threads, prune_turns = 500, 20
    else:
        stress_n, stress_w = 100, 10
        ser_n = 20
        hist_max, hist_n = 100, 10
        growth_threads, growth_turns = 100, 20
        prune_threads, prune_turns = 100, 20

    # Monkey-patch argparse for sub-mains that use it
    import sys as _sys

    def _patch_argv(*argv):
        _sys.argv = ["_"] + list(argv)

    suite_results = []

    # 1. Stress (latency + throughput)
    _patch_argv(f"--n={stress_n}", f"--workers={stress_w}")
    suite_results.append(run_suite("stress", stress_main))

    # 2. DB size
    suite_results.append(run_suite("db_size", db_size_main))

    # 3. Correctness
    suite_results.append(run_suite("correctness", correctness_main))

    # 4. Serialization
    _patch_argv(f"--n={ser_n}")
    suite_results.append(run_suite("serialization", serialization_main))

    # 5. History depth
    _patch_argv(f"--max-turns={hist_max}", f"--n={hist_n}")
    suite_results.append(run_suite("history_depth", history_depth_main))

    # 6. Memory growth
    _patch_argv(f"--threads={growth_threads}", f"--turns={growth_turns}")
    suite_results.append(run_suite("memory_growth", memory_growth_main))

    # 7. Pruning
    _patch_argv(f"--threads={prune_threads}", f"--turns={prune_turns}")
    suite_results.append(run_suite("pruning", pruning_main))

    # Aggregate summary
    passed = [s for s in suite_results if s["status"] == "ok"]
    failed = [s for s in suite_results if s["status"] == "error"]

    print(f"\n{'='*60}")
    print(f"  SUMMARY: {len(passed)}/{len(suite_results)} suites passed")
    for s in failed:
        print(f"  FAILED: {s['suite']} — {s['error']}")
    print(f"{'='*60}")

    combined = {
        "generated_at": datetime.now().isoformat(),
        "suites": suite_results,
        "summary": {
            "total": len(suite_results),
            "passed": len(passed),
            "failed": len(failed),
        },
    }

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = args.out or str(out_dir / f"FULL_REPORT_{ts}.json")
    with open(out_path, "w") as f:
        json.dump(combined, f, indent=2)
    print(f"\nCombined report: {out_path}")
    return combined


if __name__ == "__main__":
    main()
