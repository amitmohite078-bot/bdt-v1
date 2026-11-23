"""Seed OLTP Postgres (sp-postgres-stores) with store_locations & store_prices for Sqoop demo."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT_DIR / "data" / "raw"


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed PostgreSQL OLTP store tables for Sqoop import")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--db", default="smartprotein_oltp")
    parser.add_argument("--user", default="sp_user")
    parser.add_argument("--password", default="sp_pass")
    parser.add_argument("--limit", type=int, default=5000, help="Max store_prices rows to seed")
    args = parser.parse_args()

    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError as exc:
        raise SystemExit("Install psycopg2-binary first: pip install psycopg2-binary") from exc

    loc_csv = RAW_DIR / "store_locations" / "store_locations.csv"
    price_csvs = sorted((RAW_DIR / "store_prices").rglob("store_prices_raw.csv"))
    if not loc_csv.exists() or not price_csvs:
        raise SystemExit("Generate Stage 3 raw datasets first via generate_smartprotein_data.py")

    loc_df = pd.read_csv(loc_csv)
    price_df = pd.read_csv(price_csvs[-1]).head(args.limit)

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.db, user=args.user, password=args.password
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS store_locations (
            store_id VARCHAR(32) PRIMARY KEY,
            store_name VARCHAR(128),
            region VARCHAR(64),
            city VARCHAR(64),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            geohash_6 VARCHAR(12),
            store_type VARCHAR(64)
        );
        CREATE TABLE IF NOT EXISTS store_prices (
            offer_id VARCHAR(32) PRIMARY KEY,
            store_id VARCHAR(32),
            food_name_raw VARCHAR(128),
            canonical_food_hint VARCHAR(128),
            pack_weight_g DOUBLE PRECISION,
            price_value DOUBLE PRECISION,
            currency VARCHAR(8),
            in_stock BOOLEAN,
            stock_qty INT,
            updated_at VARCHAR(64)
        );
        TRUNCATE TABLE store_locations, store_prices;
        """
    )

    execute_values(
        cur,
        "INSERT INTO store_locations VALUES %s",
        [tuple(x) for x in loc_df.to_numpy()],
    )
    execute_values(
        cur,
        "INSERT INTO store_prices VALUES %s",
        [tuple(x) for x in price_df.to_numpy()],
    )
    cur.close()
    conn.close()
    print(f"[Sqoop Seed] Loaded {len(loc_df)} stores and {len(price_df)} store price offers into PostgreSQL.")


if __name__ == "__main__":
    main()

