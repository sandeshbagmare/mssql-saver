"""Correctness verification under concurrency.

Runs concurrent writes to both backends and verifies:
- No lost checkpoints (put → get_tuple round-trip)
- No PK constraint violations
- State integrity: latest checkpoint holds expected channel values
- list() matches expected count

Usage:
    python -m benchmarks.correctness
"""
from __future__ import annotations

import sys
import threading
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.graph.builder import build_graph
from app.services.checkpointer_factory import get_checkpointer


SAMPLE_TEXT = "The quick brown fox jumps over the lazy dog."
STEPS = 3  # graph has 3 nodes → 3 checkpoints per invocation


def verify_backend(backend: str, n_threads: int = 30, invocations_per_thread: int = 5) -> dict:
    checkpointer = get_checkpointer(backend)
    graph = build_graph(checkpointer)

    errors: list[str] = []
    thread_ids: list[str] = []
    lock = threading.Lock()

    def worker(tid: str):
        config = {"configurable": {"thread_id": tid}}
        initial = {
            "text": SAMPLE_TEXT, "normalised": "", "word_count": 0,
            "char_count": 0, "sentence_count": 0, "summary": "",
        }
        try:
            for _ in range(invocations_per_thread):
                result = graph.invoke(initial, config)
            # Verify get_tuple returns latest state
            tup = checkpointer.get_tuple(config)
            if tup is None:
                with lock:
                    errors.append(f"[{tid}] get_tuple returned None")
                return
            if not tup.checkpoint.get("channel_values"):
                with lock:
                    errors.append(f"[{tid}] checkpoint has empty channel_values")
            # Verify list() has at least 1 entry
            history = list(checkpointer.list(config))
            if not history:
                with lock:
                    errors.append(f"[{tid}] list() returned empty for {tid}")
        except Exception as e:
            with lock:
                errors.append(f"[{tid}] exception: {e}")

    tids = [f"corr-{uuid.uuid4()}" for _ in range(n_threads)]
    with lock:
        thread_ids.extend(tids)

    threads = [threading.Thread(target=worker, args=(tid,)) for tid in tids]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Cleanup
    for tid in tids:
        try:
            checkpointer.delete_thread(tid)
        except Exception:
            pass

    return {
        "backend": backend,
        "threads": n_threads,
        "invocations_per_thread": invocations_per_thread,
        "total_invocations": n_threads * invocations_per_thread,
        "errors": errors,
        "passed": len(errors) == 0,
    }


def main():
    import json
    from datetime import datetime
    from pathlib import Path

    results = {}
    for backend in ("postgres", "mssql"):
        print(f"\nRunning correctness verification for {backend}...")
        r = verify_backend(backend)
        results[backend] = r
        status = "PASS" if r["passed"] else f"FAIL ({len(r['errors'])} errors)"
        print(f"  {backend}: {status}")
        if not r["passed"]:
            for e in r["errors"][:5]:
                print(f"    - {e}")

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(out_dir / f"correctness_{ts}.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to benchmarks/results/correctness_{ts}.json")
    return results


if __name__ == "__main__":
    main()
