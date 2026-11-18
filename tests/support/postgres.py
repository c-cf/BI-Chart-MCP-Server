"""PostgreSQL test data and helpers for the integration suite."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[2]
READER_ROLE = "bi_reader"
READER_PASSWORD = "reader-pass"  # noqa: S105 - throwaway test container
DATABASE = "test"

SEED = """
CREATE SCHEMA sales;
CREATE SCHEMA hr;

CREATE TABLE sales.customers (
    id serial PRIMARY KEY,
    name text NOT NULL,
    region text NOT NULL
);
COMMENT ON TABLE sales.customers IS 'One row per customer';

CREATE TABLE sales.orders (
    id serial PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES sales.customers (id),
    status text NOT NULL,
    amount numeric(10, 2) NOT NULL,
    created_at timestamptz NOT NULL
);
COMMENT ON TABLE sales.orders IS 'One row per order';
COMMENT ON COLUMN sales.orders.amount IS 'Order total in USD';

INSERT INTO sales.customers (name, region)
VALUES ('Ann', 'North'), ('Bob', 'South'), ('Cid', 'North');

INSERT INTO sales.orders (customer_id, status, amount, created_at)
SELECT (i % 3) + 1,
       CASE WHEN i % 4 = 0 THEN 'refunded' ELSE 'paid' END,
       round((10 + i * 2.5)::numeric, 2),
       timestamptz '2026-01-01 00:00+00' + make_interval(days => i * 5)
FROM generate_series(1, 40) AS s(i);

CREATE TABLE hr.salaries (employee text, amount integer);
INSERT INTO hr.salaries VALUES ('ceo', 1000000);
ANALYZE;
"""


@dataclass(frozen=True, slots=True)
class PostgresTarget:
    admin_dsn: str
    reader_dsn: str


def with_credentials(dsn: str, user: str, password: str) -> str:
    parts = urlsplit(dsn)
    host = parts.hostname or "localhost"
    netloc = f"{user}:{password}@{host}:{parts.port}"
    return urlunsplit(parts._replace(netloc=netloc))


def render_role_script() -> str:
    script = (ROOT / "scripts" / "create_readonly_role.sql").read_text(encoding="utf-8")
    replacements = {
        ':"role"': f'"{READER_ROLE}"',
        ":'password'": f"'{READER_PASSWORD}'",
        ':"database"': f'"{DATABASE}"',
        ':"schema"': '"sales"',
    }
    for variable, value in replacements.items():
        script = script.replace(variable, value)
    return script
