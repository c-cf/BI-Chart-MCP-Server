"""SQL guard: accept exactly one read-only query, reject everything else.

This is the second line of defence. The database is the first: PostgreSQL runs
every query in a READ ONLY transaction as a dedicated read-only role, and DuckDB
connections are locked to configured files. The guard catches what those layers
do not (sleeping, session-level settings, file-reading table functions, catalog
snooping), gives the model actionable error messages, and enforces a row cap.

The caller must execute `GuardedQuery.sql`, the SQL regenerated from the parsed
tree, never the original text: that removes parser-differential tricks.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError, TokenError
from sqlglot.expressions.core import Expr
from sqlglot.expressions.ddl import DDL
from sqlglot.expressions.dml import DML

from bi_chart_mcp.errors import BiChartError
from bi_chart_mcp.safety.denylists import DUCKDB_RULES, POSTGRES_RULES, DialectRules


class Dialect(StrEnum):
    POSTGRES = "postgres"
    DUCKDB = "duckdb"


class ViolationCode(StrEnum):
    EMPTY = "empty"
    TOO_LONG = "too_long"
    PARSE_ERROR = "parse_error"
    MULTIPLE_STATEMENTS = "multiple_statements"
    NOT_A_QUERY = "not_a_query"
    FORBIDDEN_CLAUSE = "forbidden_clause"
    FORBIDDEN_FUNCTION = "forbidden_function"
    FORBIDDEN_TABLE = "forbidden_table"


class GuardViolation(BiChartError):  # noqa: N818 - a violation, not a crash
    """The SQL was rejected. The message tells the model how to fix it."""

    def __init__(self, code: ViolationCode, message: str) -> None:
        super().__init__(message, code=code)
        self._violation = code

    @property
    def code(self) -> ViolationCode:
        return self._violation


@dataclass(frozen=True, slots=True)
class GuardPolicy:
    """What the guard allows for one data source."""

    dialect: Dialect
    row_cap: int = 5000
    # None: any table the database role can read. Otherwise only these names
    # (unqualified, or "schema.name"); used for DuckDB file-backed views.
    allowed_tables: frozenset[str] | None = None
    # None: any schema. Otherwise schema-qualified references must use one of these.
    allowed_schemas: frozenset[str] | None = None
    max_sql_length: int = 20_000

    def __post_init__(self) -> None:
        if self.row_cap < 1:
            raise ValueError(f"row_cap must be >= 1, got {self.row_cap}")
        if self.max_sql_length < 1:
            raise ValueError(f"max_sql_length must be >= 1, got {self.max_sql_length}")


@dataclass(frozen=True, slots=True)
class GuardedQuery:
    """A query that passed the guard, ready to execute."""

    sql: str  # canonical single-line SQL with the row cap applied; execute this
    display_sql: str  # the same query, pretty-printed for people
    row_cap: int  # keep at most this many rows; the query may return one more to signal truncation
    tables: tuple[str, ...]  # base tables referenced (CTE names excluded), sorted


_RULES: Final[dict[Dialect, DialectRules]] = {
    Dialect.POSTGRES: POSTGRES_RULES,
    Dialect.DUCKDB: DUCKDB_RULES,
}

_QUERY_ROOTS: Final = (exp.Select, exp.SetOperation, exp.Subquery)

_FORBIDDEN_NODES: Final = (
    DML,
    DDL,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Merge,
    exp.Copy,
    exp.Create,
    exp.Drop,
    exp.Alter,
    exp.TruncateTable,
    exp.Command,
    exp.Into,
    exp.Lock,
    exp.Set,
    exp.SetItem,
    exp.Pragma,
    exp.Use,
    exp.Transaction,
    exp.Commit,
    exp.Rollback,
    exp.Attach,
    exp.Detach,
    exp.Install,
    exp.Kill,
    exp.Execute,
    exp.Grant,
    exp.Revoke,
    exp.Export,
    exp.LoadData,
    exp.Analyze,
    exp.Describe,
    exp.Show,
    exp.Comment,
    exp.Declare,
    exp.Cache,
    exp.Uncache,
    exp.Refresh,
)

# Read-only statements we still refuse, so that they get a not_a_query message.
_OTHER_STATEMENTS: Final = (exp.Values, exp.Pivot, exp.Summarize)

_CLAUSE_HINTS: Final[dict[type[Expr], str]] = {
    exp.Into: "SELECT ... INTO creates a table",
    exp.Lock: "FOR UPDATE / FOR SHARE takes row locks",
}

_DUCKDB_DEFAULT_SCHEMA: Final = "main"

_ONLY_SELECT: Final = "Only a single read-only SELECT query (optionally with WITH) is allowed"


def guard_sql(sql: str, policy: GuardPolicy) -> GuardedQuery:
    """Validate `sql` against `policy` and return the canonical query to execute.

    Raises:
        GuardViolation: The SQL is not a single read-only query allowed by the policy.
    """
    if not sql.strip():
        raise GuardViolation(ViolationCode.EMPTY, "The SQL is empty.")
    if len(sql) > policy.max_sql_length:
        raise GuardViolation(
            ViolationCode.TOO_LONG,
            f"The SQL is {len(sql)} characters; the limit is {policy.max_sql_length}.",
        )

    root = _parse_single(sql, policy.dialect)
    _check_root(root)
    _check_clauses(root)
    rules = _RULES[policy.dialect]
    _check_functions(root, rules)
    tables = _check_tables(root, policy, rules)

    capped = _apply_row_cap(root, policy.row_cap + 1)
    dialect = policy.dialect.value
    return GuardedQuery(
        sql=capped.sql(dialect=dialect, comments=False, normalize_functions=False),
        display_sql=capped.sql(
            dialect=dialect, pretty=True, comments=False, normalize_functions=False
        ),
        row_cap=policy.row_cap,
        tables=tables,
    )


# ---------------------------------------------------------------- parsing


def _parse_single(sql: str, dialect: Dialect) -> Expr:
    try:
        parsed = sqlglot.parse(sql, dialect=dialect.value)
    except (ParseError, TokenError) as exc:
        raise GuardViolation(
            ViolationCode.PARSE_ERROR,
            f"The SQL could not be parsed as {dialect.value} SQL: {_first_line(exc)}",
        ) from None

    statements = [statement for statement in parsed if statement is not None]
    if not statements:
        raise GuardViolation(ViolationCode.EMPTY, "The SQL contains no statement.")
    if len(statements) > 1:
        raise GuardViolation(
            ViolationCode.MULTIPLE_STATEMENTS,
            f"{_ONLY_SELECT}; got {len(statements)} statements. Send one query per call.",
        )
    return _unwrap_parentheses(statements[0])


def _unwrap_parentheses(node: Expr) -> Expr:
    """`(SELECT 1)` -> `SELECT 1`, as long as the parentheses carry nothing else."""
    while isinstance(node, exp.Subquery) and set(_present_args(node)) == {"this"}:
        node = node.this
    return node


def _present_args(node: Expr) -> Iterator[str]:
    return (key for key, value in node.args.items() if value is not None and value != [])


def _first_line(exc: Exception) -> str:
    return str(exc).strip().splitlines()[0][:200]


# ---------------------------------------------------------------- statement shape


def _describe(node: Expr) -> str:
    if isinstance(node, exp.Command):
        return str(node.this).upper()
    return node.key.upper()


def _check_root(root: Expr) -> None:
    if isinstance(root, _QUERY_ROOTS):
        return
    if isinstance(root, _FORBIDDEN_NODES + _OTHER_STATEMENTS):
        raise GuardViolation(ViolationCode.NOT_A_QUERY, f"{_ONLY_SELECT}; got {_describe(root)}.")
    # A bare expression such as `SELEC * FRM t` parsed leniently: not a statement at all.
    raise GuardViolation(
        ViolationCode.PARSE_ERROR,
        f"The SQL does not form a valid statement. {_ONLY_SELECT}.",
    )


def _check_clauses(root: Expr) -> None:
    for node in root.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            hint = _CLAUSE_HINTS.get(type(node), f"{_describe(node)} modifies data or state")
            raise GuardViolation(
                ViolationCode.FORBIDDEN_CLAUSE,
                f"{hint}, which is not allowed. {_ONLY_SELECT}.",
            )


# ---------------------------------------------------------------- functions


def _function_names(func: exp.Func) -> set[str]:
    if isinstance(func, exp.Anonymous | exp.AnonymousAggFunc):
        return {func.name.lower()}
    return {type(func).sql_name().lower(), func.key.lower()}


def _check_functions(root: Expr, rules: DialectRules) -> None:
    for func in root.find_all(exp.Func):
        for name in _function_names(func):
            if rules.is_function_denied(name):
                raise GuardViolation(
                    ViolationCode.FORBIDDEN_FUNCTION,
                    f"The function {name}() is not allowed: it can read files, change "
                    "settings, block, or run SQL outside this query. Use plain SQL over "
                    "the tables listed by search_catalog instead.",
                )


# ---------------------------------------------------------------- tables


def _check_tables(root: Expr, policy: GuardPolicy, rules: DialectRules) -> tuple[str, ...]:
    cte_names = {cte.alias_or_name.lower() for cte in root.find_all(exp.CTE)}
    referenced: set[str] = set()
    for table in root.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):
            continue  # a table function such as range(10); checked by _check_functions
        name, schema, catalog = table.name.lower(), table.db.lower(), table.catalog.lower()
        if not schema and not catalog and name in cte_names:
            continue
        _check_table(name, schema, catalog, policy, rules)
        referenced.add(f"{schema}.{name}" if schema else name)
    return tuple(sorted(referenced))


def _forbidden_table(qualified: str, reason: str) -> GuardViolation:
    return GuardViolation(
        ViolationCode.FORBIDDEN_TABLE,
        f"The table {qualified} is not available: {reason}. "
        "Use search_catalog to list the tables you can query.",
    )


def _check_table(
    name: str, schema: str, catalog: str, policy: GuardPolicy, rules: DialectRules
) -> None:
    qualified = ".".join(part for part in (catalog, schema, name) if part)
    if schema in rules.denied_schemas or name.startswith(rules.denied_table_prefixes):
        raise _forbidden_table(qualified, "system catalogs are not exposed")
    if policy.allowed_schemas is not None and schema and schema not in policy.allowed_schemas:
        raise _forbidden_table(qualified, "its schema is not configured for this source")
    if policy.allowed_tables is None:
        return
    is_default_schema = schema == "" or (
        policy.dialect is Dialect.DUCKDB and schema == _DUCKDB_DEFAULT_SCHEMA
    )
    key = name if is_default_schema else f"{schema}.{name}"
    if catalog or key not in policy.allowed_tables:
        raise _forbidden_table(qualified, "it is not configured for this source")


# ---------------------------------------------------------------- row cap


def _literal_int(node: Expr | None) -> int | None:
    if isinstance(node, exp.Literal) and not node.is_string:
        try:
            return int(str(node.this))
        except ValueError:
            return None
    return None


def _existing_limit(node: Expr | None) -> int | None:
    if isinstance(node, exp.Limit):
        return _literal_int(node.expression)
    if isinstance(node, exp.Fetch):
        options = node.args.get("limit_options")
        if isinstance(options, exp.LimitOptions) and options.args.get("percent"):
            return None
        return _literal_int(node.args.get("count"))
    return None


def _apply_row_cap(root: Expr, limit: int) -> Expr:
    capped = root.copy()
    current = _existing_limit(capped.args.get("limit"))
    effective = limit if current is None else min(current, limit)
    capped.set("limit", exp.Limit(expression=exp.Literal.number(effective)))
    return capped
