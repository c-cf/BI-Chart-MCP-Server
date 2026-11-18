"""Tests for the sqlglot-based SQL guard (second line of defence after the DB role)."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot.expressions.query import Union

from bi_chart_mcp.safety.sql_guard import (
    Dialect,
    GuardPolicy,
    GuardViolation,
    ViolationCode,
    guard_sql,
)
from support.corpus import CorpusCase, allow_cases, deny_cases

ROW_CAP = 100

PG_POLICY = GuardPolicy(
    dialect=Dialect.POSTGRES,
    row_cap=ROW_CAP,
    allowed_schemas=frozenset({"public", "analytics"}),
)
DUCK_POLICY = GuardPolicy(
    dialect=Dialect.DUCKDB,
    row_cap=ROW_CAP,
    allowed_tables=frozenset({"orders", "customers"}),
)
POLICIES = {"postgres": PG_POLICY, "duckdb": DUCK_POLICY}


def _ids(cases: list[CorpusCase]) -> list[str]:
    return [c.id for c in cases]


# ---------------------------------------------------------------- corpus: allow


@pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
def test_allow_corpus_passes(dialect: str) -> None:
    policy = POLICIES[dialect]
    failures = []
    for case in allow_cases(dialect):  # type: ignore[arg-type]
        try:
            guard_sql(case.sql, policy)
        except GuardViolation as exc:
            failures.append(f"{case.id}: {exc.code} {exc}")
    assert failures == []


@pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
def test_guarded_sql_is_a_fixed_point(dialect: str) -> None:
    """Guarding the canonical output again must not change it (parse/generate round-trip)."""
    policy = POLICIES[dialect]
    for case in allow_cases(dialect):  # type: ignore[arg-type]
        first = guard_sql(case.sql, policy)
        second = guard_sql(first.sql, policy)
        assert second.sql == first.sql, case.id


@pytest.mark.parametrize("dialect", ["postgres", "duckdb"])
def test_guarded_sql_is_single_statement(dialect: str) -> None:
    policy = POLICIES[dialect]
    for case in allow_cases(dialect):  # type: ignore[arg-type]
        guarded = guard_sql(case.sql, policy)
        statements = [s for s in sqlglot.parse(guarded.sql, dialect=dialect) if s is not None]
        assert len(statements) == 1, case.id


# ---------------------------------------------------------------- corpus: deny


@pytest.mark.parametrize("case", deny_cases("postgres"), ids=_ids(deny_cases("postgres")))
def test_postgres_deny_corpus(case: CorpusCase) -> None:
    with pytest.raises(GuardViolation) as info:
        guard_sql(case.sql, PG_POLICY)
    assert info.value.code == ViolationCode(case.code)


@pytest.mark.parametrize("case", deny_cases("duckdb"), ids=_ids(deny_cases("duckdb")))
def test_duckdb_deny_corpus(case: CorpusCase) -> None:
    with pytest.raises(GuardViolation) as info:
        guard_sql(case.sql, DUCK_POLICY)
    assert info.value.code == ViolationCode(case.code)


def test_violation_messages_are_actionable() -> None:
    with pytest.raises(GuardViolation) as info:
        guard_sql("DELETE FROM orders", PG_POLICY)
    message = str(info.value)
    assert "SELECT" in message
    assert "DELETE" in message.upper()


def test_too_long_sql_is_rejected() -> None:
    policy = GuardPolicy(dialect=Dialect.POSTGRES, row_cap=10, max_sql_length=50)
    with pytest.raises(GuardViolation) as info:
        guard_sql("SELECT " + ", ".join(["1"] * 40), policy)
    assert info.value.code is ViolationCode.TOO_LONG


# ---------------------------------------------------------------- row cap


def _limit_of(sql: str, dialect: str = "postgres") -> int:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    limit = tree.args["limit"]
    return int(limit.expression.this)


def test_row_cap_added_when_missing() -> None:
    guarded = guard_sql("SELECT * FROM orders", PG_POLICY)
    assert _limit_of(guarded.sql) == ROW_CAP + 1
    assert guarded.row_cap == ROW_CAP


def test_smaller_user_limit_is_kept() -> None:
    guarded = guard_sql("SELECT * FROM orders LIMIT 7", PG_POLICY)
    assert _limit_of(guarded.sql) == 7


def test_larger_user_limit_is_capped() -> None:
    guarded = guard_sql("SELECT * FROM orders LIMIT 100000", PG_POLICY)
    assert _limit_of(guarded.sql) == ROW_CAP + 1


def test_limit_all_is_capped() -> None:
    guarded = guard_sql("SELECT * FROM orders LIMIT ALL", PG_POLICY)
    assert _limit_of(guarded.sql) == ROW_CAP + 1


def test_offset_is_preserved() -> None:
    guarded = guard_sql("SELECT * FROM orders ORDER BY id LIMIT 5 OFFSET 20", PG_POLICY)
    assert "OFFSET 20" in guarded.sql
    assert _limit_of(guarded.sql) == 5


def test_fetch_first_becomes_limit() -> None:
    guarded = guard_sql("SELECT * FROM orders ORDER BY id FETCH FIRST 5 ROWS ONLY", PG_POLICY)
    assert _limit_of(guarded.sql) == 5


def test_union_gets_cap_on_the_whole_result() -> None:
    guarded = guard_sql("SELECT id FROM orders UNION SELECT id FROM refunds", PG_POLICY)
    tree = sqlglot.parse_one(guarded.sql, dialect="postgres")
    assert isinstance(tree, Union)
    assert _limit_of(guarded.sql) == ROW_CAP + 1


def test_duckdb_from_first_is_capped() -> None:
    guarded = guard_sql("FROM orders SELECT id", DUCK_POLICY)
    assert _limit_of(guarded.sql, "duckdb") == ROW_CAP + 1


# ---------------------------------------------------------------- output shape


def test_quoted_function_names_keep_their_case() -> None:
    guarded = guard_sql('SELECT "MyFunc"(1), lower(name) FROM orders', PG_POLICY)
    assert '"MyFunc"(1)' in guarded.sql


def test_comments_are_stripped_from_executed_sql() -> None:
    guarded = guard_sql("SELECT /* note */ 1 -- trailing", PG_POLICY)
    assert "note" not in guarded.sql
    assert "trailing" not in guarded.sql


def test_display_sql_is_pretty_printed() -> None:
    guarded = guard_sql("SELECT a, b FROM orders WHERE a > 1", PG_POLICY)
    assert "\n" in guarded.display_sql
    assert guarded.display_sql.startswith("SELECT")


def test_referenced_tables_exclude_cte_names() -> None:
    guarded = guard_sql(
        "WITH r AS (SELECT * FROM orders) SELECT * FROM r JOIN customers c ON true",
        PG_POLICY,
    )
    assert guarded.tables == ("customers", "orders")


def test_policy_rejects_non_positive_row_cap() -> None:
    with pytest.raises(ValueError, match="row_cap"):
        GuardPolicy(dialect=Dialect.POSTGRES, row_cap=0)
