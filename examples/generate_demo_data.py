"""Generate a deterministic demo shop dataset (Parquet for DuckDB, CSV for PostgreSQL).

Usage:
    uv run python examples/generate_demo_data.py [output_dir]

The default output directory is examples/demo_data/. Values come from hashes of the
row number, not a random generator, so every run produces the same files.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

DEFAULT_OUTPUT = Path(__file__).resolve().parent / "demo_data"
TABLES = ("customers", "products", "orders", "order_items")

# Pseudo-random in [0, 1) from a key: deterministic across runs and thread counts.
_RAND = "((hash({key}) % 1000000)::DOUBLE / 1000000)"


def _rand(key: str) -> str:
    return _RAND.format(key=key)


CUSTOMERS = f"""
CREATE TABLE customers AS
SELECT
    i AS id,
    'Customer ' || lpad(i::VARCHAR, 4, '0') AS name,
    (['North', 'South', 'East', 'West', 'Central'])[1 + (hash(i, 'region') % 5)::INTEGER] AS region,
    CASE
        WHEN {_rand("i, 'segment'")} < 0.6 THEN 'Consumer'
        WHEN {_rand("i, 'segment'")} < 0.9 THEN 'SMB'
        ELSE 'Enterprise'
    END AS segment,
    DATE '2024-01-01' + (hash(i, 'signup') % 640)::INTEGER AS signup_date
FROM range(1, 401) t(i)
"""

PRODUCTS = """
CREATE TABLE products AS
SELECT
    i AS id,
    category || ' item ' || i AS name,
    category,
    round(price_base * (0.6 + (hash(i, 'price') % 900)::DOUBLE / 1000), 2)::DECIMAL(10, 2)
        AS unit_price
FROM (
    SELECT
        i,
        (['Electronics', 'Home', 'Office', 'Outdoor', 'Toys'])[1 + ((i - 1) % 5)] AS category,
        ([320.0, 85.0, 40.0, 120.0, 30.0])[1 + ((i - 1) % 5)] AS price_base
    FROM range(1, 41) t(i)
)
"""

# Orders over 22 months, with more orders in November and December.
ORDERS = f"""
CREATE TABLE orders AS
WITH days AS (
    SELECT d::DATE AS day FROM range(DATE '2025-01-01', DATE '2026-11-01', INTERVAL 1 DAY) t(d)
),
weighted AS (
    SELECT day, CASE WHEN month(day) IN (11, 12) THEN 14 ELSE 8 END
        + (hash(day, 'volume') % 5)::INTEGER AS n
    FROM days
),
expanded AS (
    SELECT day, unnest(range(n)) AS k FROM weighted
)
SELECT
    row_number() OVER (ORDER BY day, k) AS id,
    1 + (hash(day, k, 'customer') % 400)::INTEGER AS customer_id,
    day AS order_date,
    CASE
        WHEN {_rand("day, k, 'status'")} < 0.88 THEN 'paid'
        WHEN {_rand("day, k, 'status'")} < 0.96 THEN 'refunded'
        ELSE 'cancelled'
    END AS status,
    (['web', 'web', 'app', 'store'])[1 + (hash(day, k, 'channel') % 4)::INTEGER] AS channel
FROM expanded
"""

ORDER_ITEMS = """
CREATE TABLE order_items AS
WITH lines AS (
    SELECT o.id AS order_id, unnest(range(1 + (hash(o.id, 'lines') % 4)::INTEGER)) AS line
    FROM orders o
)
SELECT
    l.order_id,
    l.line + 1 AS line_no,
    p.id AS product_id,
    1 + (hash(l.order_id, l.line, 'qty') % 3)::INTEGER AS quantity,
    p.unit_price,
    CASE WHEN hash(l.order_id, l.line, 'discount') % 10 = 0 THEN 0.1 ELSE 0 END::DECIMAL(4, 2)
        AS discount
FROM lines l
JOIN products p ON p.id = 1 + (hash(l.order_id, l.line, 'product') % 40)::INTEGER
"""


def generate(output: Path = DEFAULT_OUTPUT) -> dict[str, Path]:
    """Write each table as Parquet and CSV into `output`; return the Parquet paths."""
    output.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(":memory:")
    try:
        for statement in (CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS):
            con.execute(statement)
        written: dict[str, Path] = {}
        for table in TABLES:
            ordering = "order_id, line_no" if table == "order_items" else "id"
            parquet = output / f"{table}.parquet"
            csv = output / f"{table}.csv"
            query = f"SELECT * FROM {table} ORDER BY {ordering}"
            con.execute(f"COPY ({query}) TO '{parquet.as_posix()}' (FORMAT parquet)")
            con.execute(f"COPY ({query}) TO '{csv.as_posix()}' (HEADER, DELIMITER ',')")
            written[table] = parquet
        return written
    finally:
        con.close()


def main(argv: list[str]) -> None:
    output = Path(argv[1]) if len(argv) > 1 else DEFAULT_OUTPUT
    for table, path in generate(output).items():
        print(f"{table:12} -> {path}")


if __name__ == "__main__":
    main(sys.argv)
