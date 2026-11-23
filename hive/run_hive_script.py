"""Hive HQL Execution Engine (Local Parquet Lakehouse Runner).

Executes Apache Hive external table DDLs and SQL analytical queries against
the local Parquet Lakehouse files without requiring a native Hadoop/Hive daemon.
Maps 'hdfs://namenode:9000/fitguard/...' directly to './lakehouse/fitguard/...'.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
from pathlib import Path
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_HQL = ROOT_DIR / "hive" / "create_external_tables.hql"
LAKEHOUSE_ROOT = ROOT_DIR / "lakehouse" / "fitguard"


def parse_and_register_tables(conn: sqlite3.Connection, hql_content: str) -> dict[str, int]:
    """Parse CREATE EXTERNAL TABLE statements and load underlying Parquet files."""
    table_pattern = re.compile(
        r"CREATE\s+EXTERNAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\((.*?)\)\s*STORED\s+AS\s+PARQUET\s+LOCATION\s+['\"]hdfs://namenode:9000/fitguard/([^'\"]+)['\"];",
        re.DOTALL | re.IGNORECASE,
    )

    registered = {}
    for match in table_pattern.finditer(hql_content):
        table_name = match.group(1).lower()
        subpath = match.group(3).strip("/ ")

        target_dir = LAKEHOUSE_ROOT / subpath
        parquet_files = list(target_dir.glob("*.parquet"))
        if not parquet_files and (LAKEHOUSE_ROOT / f"{subpath}.parquet").exists():
            parquet_files = [LAKEHOUSE_ROOT / f"{subpath}.parquet"]
        if not parquet_files and (LAKEHOUSE_ROOT / f"{subpath}.snappy.parquet").exists():
            parquet_files = [LAKEHOUSE_ROOT / f"{subpath}.snappy.parquet"]

        if not parquet_files:
            print(f"[Warning] No Parquet file found at: {target_dir}")
            continue

        dfs = [pd.read_parquet(pf) for pf in parquet_files]
        df = pd.concat(dfs, ignore_index=True)

        # Convert timestamp or complex types for SQLite
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

        df.to_sql(table_name, conn, if_exists="replace", index=False)
        registered[table_name] = len(df)

    return registered


def run_sql(conn: sqlite3.Connection, query: str, title: str | None = None) -> None:
    if title:
        print(f"\n{'=' * 75}\n{title}\n{'=' * 75}")
    print(f"HIVE-QL> {query.strip()}\n")
    try:
        res = pd.read_sql_query(query, conn)
        if res.empty:
            print("(0 rows returned)")
        else:
            print(res.to_string(index=False))
            print(f"\n({len(res)} rows)")
    except Exception as e:
        print(f"[Execution Error]: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Hive HQL script locally on Parquet Lakehouse")
    parser.add_argument("hql_file", nargs="?", default=str(DEFAULT_HQL), help="Path to .hql script")
    parser.add_argument("-q", "--query", help="Custom SQL query to execute")
    parser.add_argument("-i", "--interactive", action="store_true", help="Launch interactive Hive SQL prompt")
    args = parser.parse_args()

    hql_path = Path(args.hql_file)
    if not hql_path.exists():
        print(f"Error: HQL file not found: {hql_path}")
        sys.exit(1)

    print("=" * 75)
    print("  SmartProtein / FitGuard Apache Hive HQL Execution Engine (Local Parquet)")
    print("=" * 75)
    print(f"Reading HQL: {hql_path}")
    print(f"Lakehouse  : {LAKEHOUSE_ROOT}\n")

    conn = sqlite3.connect(":memory:")
    with open(hql_path, "r", encoding="utf-8") as fh:
        hql_content = fh.read()

    registered = parse_and_register_tables(conn, hql_content)
    print("Mounted Lakehouse Parquet Tables:")
    for tbl, cnt in registered.items():
        print(f"  * {tbl:32s} -> {cnt:6,} rows loaded")

    if args.query:
        run_sql(conn, args.query, "Custom Query Result")
        return

    # Run default analytical queries embedded in the script
    run_sql(
        conn,
        """
        SELECT dq_rule_id, dq_dimension, COUNT(*) AS quarantined_rows
        FROM quarantine_protein_foods
        GROUP BY dq_rule_id, dq_dimension
        ORDER BY quarantined_rows DESC;
        """,
        "Query 1: Quarantine Audit (Protein Foods Violations by Rule)",
    )

    run_sql(
        conn,
        """
        SELECT food_name, vendor_name, protein_per_100g, cost_per_100g_inr, protein_per_100_inr
        FROM silver_protein_foods
        WHERE city = 'Bengaluru' AND diet_tag = 'Vegan' AND cost_per_100g_inr <= 50
        ORDER BY protein_per_100_inr DESC
        LIMIT 5;
        """,
        "Query 2: Best Value Vegan Protein Foods in Bengaluru (<= Rs. 50 / 100g)",
    )

    run_sql(
        conn,
        """
        SELECT user_id, location_city, fitness_goal, daily_protein_target_g, food_name, protein_per_100g, cost_per_100g_inr
        FROM gold_protein_recommendations
        WHERE rank = 1
        LIMIT 5;
        """,
        "Query 3: Top Personalized Recommendation (Gold Zone Rank 1 per User)",
    )

    if args.interactive:
        print("\nEntering Interactive Hive CLI (type 'exit;' or 'quit;' to leave):")
        while True:
            try:
                line = input("hive> ")
                if line.strip().lower() in ("exit;", "quit;", "exit", "quit"):
                    break
                if line.strip():
                    run_sql(conn, line)
            except (KeyboardInterrupt, EOFError):
                break


if __name__ == "__main__":
    main()
