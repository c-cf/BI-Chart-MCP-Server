"""Tests for the DuckDB file source, including its lockdown (no guard involved)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from bi_chart_mcp.errors import ConfigError, QueryError
from bi_chart_mcp.safety.sql_guard import GuardedQuery, guard_sql
from bi_chart_mcp.settings import DuckDBSourceConfig, DuckDBTableConfig, Limits
from bi_chart_mcp.sources.duckdb_source import DuckDBSource
from support.corpus import deny_cases
from support.files import write_sample_files


@pytest.fixture
def files(tmp_path: Path) -> dict[str, Path]:
    return write_sample_files(tmp_path / "data")


def _config(files: dict[str, Path], **overrides: object) -> DuckDBSourceConfig:
    return DuckDBSourceConfig(
        type="duckdb",
        name="files",
        description="Sample files",
        tables=(
            DuckDBTableConfig(name="orders", path=files["orders"], description="Orders"),
            DuckDBTableConfig(name="customers", path=files["customers"]),
        ),
        **overrides,  # type: ignore[arg-type]
    )


@pytest.fixture
def source(files: dict[str, Path]) -> Iterator[DuckDBSource]:
    src = DuckDBSource(_config(files), Limits(row_cap=10, statement_timeout_ms=2000))
    yield src
    src.close()


def _raw(sql: str, row_cap: int = 10) -> GuardedQuery:
    """Bypass the guard to test the database layer on its own."""
    return GuardedQuery(sql=sql, display_sql=sql, row_cap=row_cap, tables=())


def test_guarded_query_returns_rows_and_types(source: DuckDBSource) -> None:
    query = guard_sql(
        "SELECT status, count(*) AS n, sum(amount) AS total "
        "FROM orders GROUP BY status ORDER BY status",
        source.policy,
    )
    result = source.execute(query)
    assert [c.name for c in result.columns] == ["status", "n", "total"]
    assert result.columns[1].db_type == "BIGINT"
    assert result.rows[0][0] == "paid"
    assert result.truncated is False
    assert result.elapsed_ms >= 0


def test_results_are_capped_and_flagged(source: DuckDBSource) -> None:
    result = source.execute(guard_sql("SELECT * FROM orders", source.policy))
    assert len(result.rows) == 10
    assert result.truncated is True


def test_csv_view_and_join(source: DuckDBSource) -> None:
    query = guard_sql(
        "SELECT c.region, count(*) FROM orders o JOIN customers c ON c.id = o.customer_id "
        "GROUP BY 1 ORDER BY 1",
        source.policy,
    )
    assert [r[0] for r in source.execute(query).rows] == ["North", "South"]


def test_timeout_interrupts_long_queries(files: dict[str, Path]) -> None:
    slow = DuckDBSource(_config(files), Limits(statement_timeout_ms=200))
    try:
        with pytest.raises(QueryError) as info:
            slow.execute(_raw("SELECT count(*) FROM range(300000000) a, range(1000) b"))
        assert info.value.code == "timeout"
    finally:
        slow.close()


def test_errors_are_sanitized(source: DuckDBSource, files: dict[str, Path]) -> None:
    with pytest.raises(QueryError) as info:
        source.execute(_raw(f"SELECT * FROM read_csv('{files['secret'].as_posix()}')"))
    message = str(info.value)
    assert files["secret"].parent.as_posix() not in message
    assert str(files["secret"].parent) not in message
    assert "<path>" in message


def test_list_and_describe_tables(source: DuckDBSource) -> None:
    tables = source.list_tables()
    assert [t.name for t in tables] == ["orders", "customers"]
    assert tables[0].description == "Orders"
    detail = source.describe_table("orders")
    assert [c.name for c in detail.columns] == [
        "id",
        "customer_id",
        "status",
        "amount",
        "order_date",
    ]
    assert source.describe_table("main.customers").table.name == "customers"


def test_describe_unknown_table(source: DuckDBSource) -> None:
    with pytest.raises(QueryError, match="Available tables: customers, orders") as info:
        source.describe_table("payroll")
    assert info.value.code == "not_found"


def test_top_values(source: DuckDBSource) -> None:
    values = source.top_values("orders", "status", 5)
    assert values[0].value == "paid"
    assert values[0].count == 30
    with pytest.raises(QueryError, match="does not exist"):
        source.top_values("orders", "nope", 5)


def test_missing_file_is_a_config_error(tmp_path: Path) -> None:
    config = DuckDBSourceConfig(
        type="duckdb",
        name="files",
        tables=(DuckDBTableConfig(name="t", path=tmp_path / "missing.csv"),),
    )
    with pytest.raises(ConfigError, match="file not found"):
        DuckDBSource(config, Limits())


# Attacks the database layer must stop even if the guard were bypassed.
DUCKDB_LAYER_BLOCKS = {
    "replacement_scan_csv",
    "replacement_scan_parquet",
    "read_csv",
    "read_csv_auto",
    "read_parquet",
    "read_text",
    "read_json",
    "parquet_metadata",
    "glob",
    "sniff_csv",
    "function_in_subquery",
    "attach",
    "install",
    "load",
    "copy_out",
    "export_database",
    "set",
    "pragma",
    "create_view",
}


def test_database_layer_blocks_file_and_config_attacks(
    source: DuckDBSource, files: dict[str, Path]
) -> None:
    """Two-layer matrix: replay the deny corpus with the guard disabled."""
    leaked = []
    for case in deny_cases("duckdb"):
        if case.id not in DUCKDB_LAYER_BLOCKS:
            continue
        try:
            source.execute(_raw(case.sql))
        except QueryError:
            continue
        leaked.append(case.id)
    assert leaked == []
    # And the locked connection still serves the configured views.
    assert source.execute(_raw("SELECT count(*) FROM orders")).rows == ((40,),)
    assert files["customers"].read_text("utf-8").startswith("id,name,region")
