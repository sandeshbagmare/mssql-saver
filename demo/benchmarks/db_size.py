"""Measure checkpoint table sizes in both Postgres and SQL Server.

Usage:
    python -m benchmarks.db_size
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from app.core.config import settings


def pg_table_sizes() -> dict[str, dict]:
    import psycopg
    tables = ["checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations"]
    conn = psycopg.connect(settings.pg_dsn, autocommit=True)
    cur = conn.cursor()
    rows = {}
    for t in tables:
        cur.execute(
            "SELECT pg_total_relation_size(quote_ident(%s)), "
            "       (SELECT COUNT(*) FROM " + t + ")",
            (t,),
        )
        row = cur.fetchone()
        if row:
            rows[t] = {"size_bytes": row[0], "row_count": row[1]}
    cur.execute("SELECT pg_database_size(current_database())")
    row = cur.fetchone()
    rows["_total_db"] = {"size_bytes": row[0] if row else 0, "row_count": None}
    conn.close()
    return rows


def mssql_table_sizes() -> dict[str, dict]:
    import pyodbc
    tables = ["checkpoints", "checkpoint_blobs", "checkpoint_writes", "checkpoint_migrations"]
    conn = pyodbc.connect(settings.mssql_conn_str, autocommit=True)
    cur = conn.cursor()
    rows = {}
    for t in tables:
        # sp_spaceused per table
        cur.execute(f"EXEC sp_spaceused '{t}'")
        row = cur.fetchone()
        if row:
            # row: (name, rows, reserved, data, index_size, unused)
            reserved_kb = int(row[2].strip().replace(" KB", ""))
            rows[t] = {"size_bytes": reserved_kb * 1024, "row_count": int(row[1])}
        else:
            rows[t] = {"size_bytes": 0, "row_count": 0}
    # Total DB size
    cur.execute(
        "SELECT SUM(size * 8 * 1024) FROM sys.database_files WHERE type_desc='ROWS'"
    )
    row = cur.fetchone()
    rows["_total_db"] = {"size_bytes": int(row[0]) if row and row[0] else 0, "row_count": None}
    conn.close()
    return rows


def print_sizes(backend: str, sizes: dict[str, dict]) -> None:
    print(f"\n{'='*55}")
    print(f"  {backend.upper()} table sizes")
    print(f"{'='*55}")
    header = f"{'Table':<30} {'Rows':>10} {'Size':>12}"
    print(header)
    print("-" * len(header))
    for table, info in sizes.items():
        sz = info["size_bytes"]
        rc = info["row_count"]
        sz_str = f"{sz / 1024:.1f} KB" if sz < 1_048_576 else f"{sz / 1_048_576:.2f} MB"
        rc_str = str(rc) if rc is not None else "—"
        print(f"{table:<30} {rc_str:>10} {sz_str:>12}")


def main():
    import json
    from pathlib import Path
    from datetime import datetime

    print("Measuring table sizes...")
    pg_sizes = pg_table_sizes()
    mssql_sizes = mssql_table_sizes()

    print_sizes("postgres", pg_sizes)
    print_sizes("mssql", mssql_sizes)

    out_dir = Path(__file__).parent / "results"
    out_dir.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = {"postgres": pg_sizes, "mssql": mssql_sizes}
    with open(out_dir / f"db_size_{ts}.json", "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved to benchmarks/results/db_size_{ts}.json")
    return out


if __name__ == "__main__":
    main()
