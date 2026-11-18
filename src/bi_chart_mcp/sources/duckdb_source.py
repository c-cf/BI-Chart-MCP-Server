"""DuckDB source over configured CSV / Parquet / JSON files.

Each configured file becomes a view. The connection is then locked down:
- only the configured files are reachable (`allowed_paths`),
- all other file system and network access is disabled,
- extensions cannot be installed or auto-loaded,
- configuration is locked, so a query cannot undo any of this.

DuckDB has no statement timeout, so queries are interrupted from a timer.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from typing import Final, cast

import duckdb

from bi_chart_mcp.errors import ConfigError, QueryError
from bi_chart_mcp.logs import get_logger
from bi_chart_mcp.safety.sql_guard import Dialect, GuardedQuery, GuardPolicy
from bi_chart_mcp.settings import DuckDBSourceConfig, DuckDBTableConfig, Limits
from bi_chart_mcp.sources._common import (
    NOT_FOUND_CODE,
    interrupt_after,
    sanitize_message,
    timeout_error,
)
from bi_chart_mcp.sources.base import (
    Column,
    ColumnInfo,
    ColumnMatch,
    ResultSet,
    TableDetail,
    TableInfo,
    ValueCount,
)

_log = get_logger("sources.duckdb")

_COLUMNS_CACHE_TTL_S: Final = 60.0

_READERS: Final[dict[str, str]] = {
    ".parquet": "read_parquet",
    ".csv": "read_csv",
    ".tsv": "read_csv",
    ".json": "read_json",
}


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class DuckDBSource:
    """Read-only access to a fixed set of local data files."""

    def __init__(self, config: DuckDBSourceConfig, limits: Limits) -> None:
        self._config = config
        self._tables: dict[str, DuckDBTableConfig] = {t.name: t for t in config.tables}
        self._timeout_ms = config.statement_timeout_ms or limits.statement_timeout_ms
        self._policy = GuardPolicy(
            dialect=Dialect.DUCKDB,
            row_cap=limits.row_cap,
            allowed_tables=frozenset(self._tables),
        )
        self._secrets = tuple(
            form for t in config.tables for form in (str(t.path), t.path.as_posix())
        )
        self._lock = threading.Lock()
        self._columns_cache: tuple[float, dict[str, tuple[ColumnInfo, ...]]] | None = None
        self._con = self._connect()

    # ------------------------------------------------------------ Source protocol

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def dialect(self) -> Dialect:
        return Dialect.DUCKDB

    @property
    def description(self) -> str | None:
        return self._config.description

    @property
    def policy(self) -> GuardPolicy:
        return self._policy

    def execute(self, query: GuardedQuery) -> ResultSet:
        started = time.perf_counter()
        cursor = self._cursor()
        try:
            with interrupt_after(self._timeout_ms, cursor.interrupt) as timed_out:
                try:
                    cursor.execute(query.sql)
                    rows = cursor.fetchmany(query.row_cap + 1)
                except duckdb.Error as exc:
                    if timed_out() or isinstance(exc, duckdb.InterruptException):
                        raise timeout_error(self._timeout_ms) from None
                    raise self._query_error(exc) from None
            columns = tuple(Column(str(d[0]), str(d[1])) for d in cursor.description or ())
        finally:
            cursor.close()
        return ResultSet(
            columns=columns,
            rows=tuple(tuple(row) for row in rows[: query.row_cap]),
            truncated=len(rows) > query.row_cap,
            elapsed_ms=round((time.perf_counter() - started) * 1000),
        )

    def list_tables(self) -> tuple[TableInfo, ...]:
        return tuple(
            TableInfo(schema="", name=t.name, kind="file", description=t.description)
            for t in self._config.tables
        )

    def describe_table(self, table: str) -> TableDetail:
        config = self._table(table)
        rows = self._internal(f"DESCRIBE {_quote_identifier(config.name)}")
        columns = tuple(
            ColumnInfo(name=str(r[0]), db_type=str(r[1]), nullable=str(r[2]).upper() == "YES")
            for r in rows
        )
        info = TableInfo(schema="", name=config.name, kind="file", description=config.description)
        return TableDetail(table=info, columns=columns)

    def top_values(self, table: str, column: str, limit: int) -> tuple[ValueCount, ...]:
        detail = self.describe_table(table)
        if column not in {c.name for c in detail.columns}:
            raise QueryError(
                f"Column {column!r} does not exist in {detail.table.name}.", code=NOT_FOUND_CODE
            )
        # Identifiers come from DESCRIBE and are quoted; limit is an int.
        sql = (
            f"SELECT {_quote_identifier(column)} AS value, count(*) AS n "  # noqa: S608
            f"FROM {_quote_identifier(detail.table.name)} "
            f"GROUP BY 1 ORDER BY 2 DESC, 1 NULLS LAST LIMIT {int(limit)}"
        )
        rows = self._internal(sql)
        return tuple(ValueCount(value=r[0], count=int(cast(int, r[1]))) for r in rows)

    def find_columns(self, terms: Sequence[str], limit: int) -> tuple[ColumnMatch, ...]:
        lowered = [t.lower() for t in terms if t]
        matches: list[ColumnMatch] = []
        for name, columns in self._all_columns().items():
            for column in columns:
                if any(t in column.name.lower() for t in lowered):
                    matches.append(
                        ColumnMatch(table=name, column=column.name, db_type=column.db_type)
                    )
        return tuple(matches[:limit])

    def close(self) -> None:
        with self._lock:
            self._con.close()

    def _all_columns(self) -> dict[str, tuple[ColumnInfo, ...]]:
        """Columns of every view, cached briefly: DESCRIBE re-sniffs CSV files each time."""
        now = time.monotonic()
        cached = self._columns_cache
        if cached is not None and now - cached[0] < _COLUMNS_CACHE_TTL_S:
            return cached[1]
        columns = {name: self.describe_table(name).columns for name in self._tables}
        self._columns_cache = (now, columns)
        return columns

    # ------------------------------------------------------------ internals

    def _connect(self) -> duckdb.DuckDBPyConnection:
        for table in self._config.tables:
            if not table.path.is_file():
                raise ConfigError(
                    f"Source {self._config.name!r}, table {table.name!r}: file not found: "
                    f"{table.path}"
                )
        con = duckdb.connect(
            ":memory:",
            config={
                "autoinstall_known_extensions": False,
                "autoload_known_extensions": False,
                "memory_limit": self._config.memory_limit,
            },
        )
        try:
            for table in self._config.tables:
                reader = _READERS[table.path.suffix.lower()]
                con.execute(  # names are validated identifiers; paths are quoted literals
                    f"CREATE VIEW {_quote_identifier(table.name)} AS "  # noqa: S608
                    f"SELECT * FROM {reader}({_quote_literal(table.path.as_posix())})"
                )
            allowed = ", ".join(_quote_literal(t.path.as_posix()) for t in self._config.tables)
            # Order matters: the allow-list must be set before external access is disabled,
            # and locking the configuration must come last.
            con.execute(f"SET allowed_paths = [{allowed}]")
            con.execute("SET enable_external_access = false")
            con.execute("SET allow_community_extensions = false")
            con.execute("SET lock_configuration = true")
        except duckdb.Error as exc:
            con.close()
            raise ConfigError(
                f"Source {self._config.name!r}: could not open the configured files: "
                f"{sanitize_message(str(exc))}"
            ) from None
        _log.info("duckdb source ready", extra={"source": self.name, "tables": len(self._tables)})
        return con

    def _cursor(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            return self._con.cursor()

    def _table(self, table: str) -> DuckDBTableConfig:
        name = table.removeprefix("main.")
        config = self._tables.get(name)
        if config is None:
            available = ", ".join(sorted(self._tables))
            raise QueryError(
                f"Table {table!r} is not configured in source {self.name!r}. "
                f"Available tables: {available}.",
                code=NOT_FOUND_CODE,
            )
        return config

    def _internal(self, sql: str) -> list[tuple[object, ...]]:
        """Run a trusted, server-built statement (no guard) with the same timeout."""
        cursor = self._cursor()
        try:
            with interrupt_after(self._timeout_ms, cursor.interrupt) as timed_out:
                try:
                    return [tuple(row) for row in cursor.execute(sql).fetchall()]
                except duckdb.Error as exc:
                    if timed_out() or isinstance(exc, duckdb.InterruptException):
                        raise timeout_error(self._timeout_ms) from None
                    raise self._query_error(exc) from None
        finally:
            cursor.close()

    def _query_error(self, exc: duckdb.Error) -> QueryError:
        message = sanitize_message(str(exc), self._secrets)
        _log.info("duckdb query failed", extra={"source": self.name, "error": message})
        return QueryError(message)
