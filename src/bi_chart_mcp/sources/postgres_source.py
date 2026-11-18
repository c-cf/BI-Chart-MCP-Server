"""PostgreSQL source.

Defences, outermost first:
1. The DSN should use a dedicated read-only role (see scripts/create_readonly_role.sql).
2. Every query runs in a `READ ONLY` transaction that is always rolled back, with
   `statement_timeout`, `lock_timeout` and `search_path` set with `SET LOCAL`.
3. When a connection returns to the pool, `DISCARD ALL` drops anything a query may
   have left behind at session level (advisory locks, prepared statements, settings).
4. The SQL guard (applied by the caller) rejects everything else it can recognise.
"""

from __future__ import annotations

import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from typing import Any, Final, LiteralString, cast

import psycopg
from psycopg import errors as pg_errors
from psycopg import sql
from psycopg_pool import ConnectionPool, PoolTimeout

from bi_chart_mcp.errors import QueryError
from bi_chart_mcp.logs import get_logger
from bi_chart_mcp.safety.sql_guard import Dialect, GuardedQuery, GuardPolicy
from bi_chart_mcp.settings import Limits, PostgresSourceConfig
from bi_chart_mcp.sources._common import (
    CONNECTION_CODE,
    NOT_FOUND_CODE,
    sanitize_message,
    timeout_error,
)
from bi_chart_mcp.sources.base import (
    Column,
    ColumnInfo,
    ForeignKey,
    ResultSet,
    TableDetail,
    TableInfo,
    TableKind,
    ValueCount,
)

_log = get_logger("sources.postgres")

_LOCK_TIMEOUT: Final = "2s"
_POOL_TIMEOUT_S: Final = 10.0
_KINDS: Final[dict[str, TableKind]] = {
    "r": "table",
    "p": "table",
    "v": "view",
    "m": "materialized_view",
    "f": "foreign_table",
}

_LIST_TABLES: Final = """
SELECT n.nspname, c.relname, c.relkind::text, c.reltuples::bigint,
       obj_description(c.oid, 'pg_class')
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY(%(schemas)s)
  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND has_table_privilege(c.oid, 'SELECT')
ORDER BY array_position(%(schemas)s, n.nspname::text), c.relname
"""

_FIND_TABLE: Final = """
SELECT c.oid, n.nspname, c.relname, c.relkind::text, c.reltuples::bigint,
       obj_description(c.oid, 'pg_class')
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = %(name)s
  AND n.nspname = ANY(%(schemas)s)
  AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND has_table_privilege(c.oid, 'SELECT')
ORDER BY array_position(%(schemas)s, n.nspname::text)
LIMIT 1
"""

_COLUMNS: Final = """
SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull,
       col_description(a.attrelid, a.attnum)
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = %(oid)s AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""

_FOREIGN_KEYS: Final = """
SELECT
  ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(attnum, ord)
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
        ORDER BY k.ord),
  c.confrelid::regclass::text,
  ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(attnum, ord)
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.attnum
        ORDER BY k.ord)
FROM pg_catalog.pg_constraint c
WHERE c.conrelid = %(oid)s AND c.contype = 'f'
ORDER BY c.conname
"""

_TYPE_NAMES: Final = (
    "SELECT oid, format_type(oid, NULL) FROM pg_catalog.pg_type WHERE oid = ANY(%s)"
)


def _configure(conn: psycopg.Connection[Any]) -> None:
    conn.read_only = True  # every transaction starts with BEGIN READ ONLY


def _reset(conn: psycopg.Connection[Any]) -> None:
    conn.autocommit = True
    try:
        conn.execute("DISCARD ALL")
    finally:
        conn.autocommit = False


class PostgresSource:
    """Read-only access to one PostgreSQL database through a connection pool."""

    def __init__(self, config: PostgresSourceConfig, limits: Limits, dsn: str) -> None:
        self._config = config
        self._timeout_ms = config.statement_timeout_ms or limits.statement_timeout_ms
        self._policy = GuardPolicy(
            dialect=Dialect.POSTGRES,
            row_cap=limits.row_cap,
            allowed_schemas=frozenset(config.schemas),
        )
        self._secrets = (dsn,)
        # min_size=0: the server starts even when the database is down; the first
        # query then reports a connection error the user can act on.
        self._pool: ConnectionPool[psycopg.Connection[Any]] = ConnectionPool(
            dsn,
            min_size=0,
            max_size=config.pool_max_size,
            open=False,
            configure=_configure,
            reset=_reset,
            check=ConnectionPool.check_connection,
            name=f"bi-chart-{config.name}",
            timeout=_POOL_TIMEOUT_S,
            kwargs={"application_name": "bi-chart-mcp", "connect_timeout": 10},
        )
        self._pool.open(wait=False)

    # ------------------------------------------------------------ Source protocol

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def dialect(self) -> Dialect:
        return Dialect.POSTGRES

    @property
    def description(self) -> str | None:
        return self._config.description

    @property
    def policy(self) -> GuardPolicy:
        return self._policy

    def execute(self, query: GuardedQuery) -> ResultSet:
        started = time.perf_counter()
        # The guard regenerated this SQL from its parse tree; psycopg types want a literal.
        statement = cast(LiteralString, query.sql)
        with self._session() as conn, conn.cursor() as cur:
            cur.execute(statement, prepare=False)
            rows = cur.fetchmany(query.row_cap + 1) if cur.description else []
            columns = self._columns(conn, cur.description or ())
        return ResultSet(
            columns=columns,
            rows=tuple(tuple(row) for row in rows[: query.row_cap]),
            truncated=len(rows) > query.row_cap,
            elapsed_ms=round((time.perf_counter() - started) * 1000),
        )

    def list_tables(self) -> tuple[TableInfo, ...]:
        rows = self._internal(_LIST_TABLES, {"schemas": list(self._config.schemas)})
        return tuple(self._table_info(r[0], r[1], r[2], r[3], r[4]) for r in rows)

    def describe_table(self, table: str) -> TableDetail:
        schema, _, name = table.rpartition(".")
        if schema and schema not in self._config.schemas:
            raise self._not_found(table)
        schemas = [schema] if schema else list(self._config.schemas)
        found = self._internal(_FIND_TABLE, {"name": name, "schemas": schemas})
        if not found:
            raise self._not_found(table)
        oid, nspname, relname, relkind, reltuples, comment = found[0]
        info = self._table_info(nspname, relname, relkind, reltuples, comment)
        columns = tuple(
            ColumnInfo(name=str(r[0]), db_type=str(r[1]), nullable=bool(r[2]), description=r[3])
            for r in self._internal(_COLUMNS, {"oid": oid})
        )
        foreign_keys = tuple(
            ForeignKey(
                columns=tuple(r[0]), references_table=str(r[1]), references_columns=tuple(r[2])
            )
            for r in self._internal(_FOREIGN_KEYS, {"oid": oid})
        )
        return TableDetail(table=info, columns=columns, foreign_keys=foreign_keys)

    def top_values(self, table: str, column: str, limit: int) -> tuple[ValueCount, ...]:
        detail = self.describe_table(table)
        if column not in {c.name for c in detail.columns}:
            raise QueryError(
                f"Column {column!r} does not exist in {detail.table.qualified_name}.",
                code=NOT_FOUND_CODE,
            )
        statement = sql.SQL(
            "SELECT {col} AS value, count(*) AS n FROM {schema}.{table} "
            "GROUP BY 1 ORDER BY 2 DESC, 1 NULLS LAST LIMIT {limit}"
        ).format(
            col=sql.Identifier(column),
            schema=sql.Identifier(detail.table.schema),
            table=sql.Identifier(detail.table.name),
            limit=sql.Literal(int(limit)),
        )
        return tuple(ValueCount(value=r[0], count=int(r[1])) for r in self._internal(statement))

    def close(self) -> None:
        self._pool.close()

    # ------------------------------------------------------------ internals

    @contextmanager
    def _session(self) -> Generator[psycopg.Connection[Any]]:
        """A pooled connection inside a READ ONLY transaction that always rolls back."""
        try:
            with self._pool.connection() as conn, conn.transaction(force_rollback=True):
                self._set_local_limits(conn)
                yield conn
        except PoolTimeout:
            raise self._connection_unavailable() from None
        except psycopg.Error as exc:
            raise self._query_error(exc) from None

    def _set_local_limits(self, conn: psycopg.Connection[Any]) -> None:
        conn.execute(
            sql.SQL("SET LOCAL statement_timeout = {}").format(sql.Literal(f"{self._timeout_ms}ms"))
        )
        conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(sql.Literal(_LOCK_TIMEOUT)))
        conn.execute(
            sql.SQL("SET LOCAL search_path TO {}").format(
                sql.SQL(", ").join(sql.Identifier(s) for s in self._config.schemas)
            )
        )

    def _internal(
        self, statement: LiteralString | sql.Composed, params: dict[str, Any] | None = None
    ) -> list[tuple[Any, ...]]:
        """Run a trusted, server-built statement (no guard) in a read-only session."""
        with self._session() as conn, conn.cursor() as cur:
            cur.execute(statement, params, prepare=False)
            return [tuple(row) for row in cur.fetchall()]

    def _columns(
        self, conn: psycopg.Connection[Any], description: Sequence[Any]
    ) -> tuple[Column, ...]:
        names: dict[int, str] = {}
        unknown: list[int] = []
        for col in description:
            info = conn.adapters.types.get(col.type_code)
            if info is None:
                unknown.append(col.type_code)
            else:
                names[col.type_code] = info.name
        if unknown:
            for oid, type_name in conn.execute(_TYPE_NAMES, [unknown]).fetchall():
                names[int(oid)] = str(type_name)
        return tuple(
            Column(str(col.name), names.get(col.type_code, "unknown")) for col in description
        )

    @staticmethod
    def _table_info(
        schema: object, name: object, relkind: object, reltuples: object, comment: object
    ) -> TableInfo:
        estimate = int(cast(int, reltuples)) if reltuples is not None else None
        return TableInfo(
            schema=str(schema),
            name=str(name),
            kind=_KINDS.get(str(relkind), "table"),
            row_estimate=estimate if estimate is not None and estimate >= 0 else None,
            description=str(comment) if comment is not None else None,
        )

    def _not_found(self, table: str) -> QueryError:
        return QueryError(
            f"Table {table!r} was not found in the schemas configured for source {self.name!r} "
            f"({', '.join(self._config.schemas)}), or this role cannot read it.",
            code=NOT_FOUND_CODE,
        )

    def _query_error(self, exc: psycopg.Error) -> QueryError:
        if isinstance(exc, pg_errors.QueryCanceled):
            return timeout_error(self._timeout_ms)
        if isinstance(exc, psycopg.OperationalError) and exc.sqlstate is None:
            _log.warning(
                "postgres connection failed",
                extra={"source": self.name, "error": sanitize_message(str(exc), self._secrets)},
            )
            return QueryError(
                f"Could not connect to source {self.name!r}. Check that the database is "
                "running and the DSN environment variable is correct.",
                code=CONNECTION_CODE,
            )
        primary = exc.diag.message_primary or str(exc)
        message = sanitize_message(primary, self._secrets)
        if exc.diag.message_hint:
            message = f"{message} (hint: {sanitize_message(exc.diag.message_hint, self._secrets)})"
        _log.info("postgres query failed", extra={"source": self.name, "sqlstate": exc.sqlstate})
        return QueryError(message)

    def _connection_unavailable(self) -> QueryError:
        return QueryError(
            f"Source {self.name!r} is busy or unreachable; try again shortly.",
            code=CONNECTION_CODE,
        )
