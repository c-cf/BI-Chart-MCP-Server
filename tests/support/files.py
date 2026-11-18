"""Build small, deterministic data files for DuckDB-backed tests."""

from __future__ import annotations

from pathlib import Path

import duckdb


def write_sample_files(directory: Path) -> dict[str, Path]:
    """Write orders.parquet, customers.csv and an unconfigured secret.csv."""
    directory.mkdir(parents=True, exist_ok=True)
    orders = directory / "orders.parquet"
    customers = directory / "customers.csv"
    secret = directory / "secret.csv"
    duckdb.sql(
        f"""
        COPY (
            SELECT
                i AS id,
                (i % 3) + 1 AS customer_id,
                CASE WHEN i % 4 = 0 THEN 'refunded' ELSE 'paid' END AS status,
                round(10 + i * 2.5, 2)::DECIMAL(10, 2) AS amount,
                DATE '2026-01-01' + (i * 5)::INTEGER AS order_date
            FROM range(1, 41) t(i)
        ) TO '{orders.as_posix()}' (FORMAT parquet)
        """
    )
    customers.write_text("id,name,region\n1,Ann,North\n2,Bob,South\n3,Cid,North\n", "utf-8")
    secret.write_text("key,value\napi_key,do-not-leak\n", "utf-8")
    return {"orders": orders, "customers": customers, "secret": secret}
