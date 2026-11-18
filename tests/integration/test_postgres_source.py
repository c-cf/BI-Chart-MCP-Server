"""PostgreSQL source against a real database, as the read-only role."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from bi_chart_mcp.errors import QueryError
from bi_chart_mcp.safety.sql_guard import GuardedQuery, guard_sql
from bi_chart_mcp.settings import Limits, PostgresSourceConfig
from bi_chart_mcp.sources.postgres_source import PostgresSource
from support.corpus import deny_cases
from support.postgres import PostgresTarget

pytestmark = pytest.mark.integration


def _config(**overrides: object) -> PostgresSourceConfig:
    values: dict[str, object] = {
        "type": "postgres",
        "name": "warehouse",
        "dsn_env": "UNUSED",
        "schemas": ("sales",),
    }
    values.update(overrides)
    return PostgresSourceConfig.model_validate(values)


def _raw(sql: str, row_cap: int = 10) -> GuardedQuery:
    """Bypass the guard to test the database layer on its own."""
    return GuardedQuery(sql=sql, display_sql=sql, row_cap=row_cap, tables=())


@pytest.fixture
def source(postgres: PostgresTarget) -> Iterator[PostgresSource]:
    src = PostgresSource(
        _config(), Limits(row_cap=10, statement_timeout_ms=1000), postgres.reader_dsn
    )
    yield src
    src.close()


def test_guarded_query_returns_rows_and_types(source: PostgresSource) -> None:
    query = guard_sql(
        "SELECT status, count(*) AS n, sum(amount) AS total, max(created_at) AS last "
        "FROM orders GROUP BY status ORDER BY status",
        source.policy,
    )
    result = source.execute(query)
    assert [c.name for c in result.columns] == ["status", "n", "total", "last"]
    assert [c.db_type for c in result.columns] == ["text", "int8", "numeric", "timestamptz"]
    assert result.rows[0][:2] == ("paid", 30)
    assert result.truncated is False


def test_results_are_capped_and_flagged(source: PostgresSource) -> None:
    result = source.execute(guard_sql("SELECT * FROM orders ORDER BY id", source.policy))
    assert len(result.rows) == 10
    assert result.truncated is True


def test_statement_timeout(source: PostgresSource) -> None:
    with pytest.raises(QueryError) as info:
        source.execute(_raw("SELECT pg_sleep(5)"))
    assert info.value.code == "timeout"


def test_list_tables_only_shows_configured_schemas(source: PostgresSource) -> None:
    tables = source.list_tables()
    assert [t.qualified_name for t in tables] == ["sales.customers", "sales.orders"]
    orders = tables[1]
    assert orders.description == "One row per order"
    assert orders.row_estimate == 40


def test_describe_table(source: PostgresSource) -> None:
    detail = source.describe_table("orders")
    assert detail.table.qualified_name == "sales.orders"
    amount = next(c for c in detail.columns if c.name == "amount")
    assert amount.db_type == "numeric(10,2)"
    assert amount.nullable is False
    assert amount.description == "Order total in USD"
    (fk,) = detail.foreign_keys
    assert fk.columns == ("customer_id",)
    assert fk.references_table == "customers"
    assert fk.references_columns == ("id",)
    assert source.describe_table("sales.customers").table.name == "customers"


@pytest.mark.parametrize("table", ["hr.salaries", "salaries", "nope", "public.orders"])
def test_describe_hidden_or_missing_tables(source: PostgresSource, table: str) -> None:
    with pytest.raises(QueryError) as info:
        source.describe_table(table)
    assert info.value.code == "not_found"


def test_top_values(source: PostgresSource) -> None:
    values = source.top_values("orders", "status", 5)
    assert [(v.value, v.count) for v in values] == [("paid", 30), ("refunded", 10)]
    with pytest.raises(QueryError, match="does not exist"):
        source.top_values("orders", "nope", 5)


def test_connection_errors_do_not_leak_the_dsn() -> None:
    dsn = "postgresql://bi_reader:super-secret@127.0.0.1:1/test"
    src = PostgresSource(_config(), Limits(), dsn)
    try:
        with pytest.raises(QueryError) as info:
            src.execute(_raw("SELECT 1"))
        assert info.value.code == "connection_error"
        assert "super-secret" not in str(info.value)
    finally:
        src.close()


def test_session_state_does_not_leak_between_queries(postgres: PostgresTarget) -> None:
    single = PostgresSource(_config(pool_max_size=1), Limits(), postgres.reader_dsn)
    try:
        single.execute(_raw("SELECT pg_advisory_lock(42)"))
        single.execute(_raw("SELECT set_config('application_name', 'pwned', false)"))
        locks = single.execute(_raw("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'"))
        name = single.execute(_raw("SELECT current_setting('application_name')"))
    finally:
        single.close()
    assert locks.rows == ((0,),)
    assert name.rows != (("pwned",),)


# Attacks PostgreSQL itself must stop for the read-only role, even with the guard bypassed.
POSTGRES_LAYER_BLOCKS = {
    "delete",
    "update",
    "insert",
    "drop_table",
    "truncate",
    "create_table",
    "create_table_as",
    "alter_role",
    "grant",
    "copy_to_program",
    "vacuum",
    "explain_analyze",
    "do_block",
    "lock_table",
    "stacked_drop",
    "data_modifying_cte_delete",
    "data_modifying_cte_insert",
    "data_modifying_cte_update",
    "select_into",
    "for_update",
    "for_share",
    "nextval",
    "setval",
    "pg_read_file",
    "pg_ls_dir",
    "dblink_lateral",
    "lo_import",
    "lo_export",
    "query_to_xml",
    "ts_stat",
    "pg_catalog_authid",
    "pg_shadow",
    "schema_outside_allowlist",
}


def test_database_layer_blocks_writes_and_privileged_access(source: PostgresSource) -> None:
    """Two-layer matrix: replay the deny corpus with the guard disabled."""
    leaked = []
    for case in deny_cases("postgres"):
        if case.id not in POSTGRES_LAYER_BLOCKS:
            continue
        try:
            source.execute(_raw(case.sql))
        except QueryError:
            continue
        leaked.append(case.id)
    assert leaked == []
    # Nothing was modified.
    assert source.execute(_raw("SELECT count(*) FROM orders")).rows == ((40,),)
