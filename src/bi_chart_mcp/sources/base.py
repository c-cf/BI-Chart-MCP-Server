"""The data-source contract shared by PostgreSQL and DuckDB.

Sources are synchronous; the query service runs them in worker threads. Every
method returns immutable values.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

from bi_chart_mcp.safety.sql_guard import Dialect, GuardedQuery, GuardPolicy

TableKind = Literal["table", "view", "materialized_view", "foreign_table", "file"]


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    db_type: str


@dataclass(frozen=True, slots=True)
class ResultSet:
    columns: tuple[Column, ...]
    rows: tuple[tuple[object, ...], ...]  # at most `row_cap` rows
    truncated: bool  # the database had more rows than `row_cap`
    elapsed_ms: int


@dataclass(frozen=True, slots=True)
class TableInfo:
    schema: str
    name: str
    kind: TableKind
    row_estimate: int | None = None
    description: str | None = None

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name


@dataclass(frozen=True, slots=True)
class ColumnInfo:
    name: str
    db_type: str
    nullable: bool
    description: str | None = None


@dataclass(frozen=True, slots=True)
class ForeignKey:
    columns: tuple[str, ...]
    references_table: str
    references_columns: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TableDetail:
    table: TableInfo
    columns: tuple[ColumnInfo, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()


@dataclass(frozen=True, slots=True)
class ValueCount:
    value: object
    count: int


class Source(Protocol):
    """A configured database the server can query read-only."""

    @property
    def name(self) -> str: ...

    @property
    def dialect(self) -> Dialect: ...

    @property
    def description(self) -> str | None: ...

    @property
    def policy(self) -> GuardPolicy:
        """Guard rules for SQL sent to this source."""
        ...

    def execute(self, query: GuardedQuery) -> ResultSet:
        """Run a guarded query; raise `QueryError` on database errors."""
        ...

    def list_tables(self) -> tuple[TableInfo, ...]: ...

    def describe_table(self, table: str) -> TableDetail:
        """`table` is `name` or `schema.name`; raise `QueryError` if it is not visible."""
        ...

    def top_values(self, table: str, column: str, limit: int) -> tuple[ValueCount, ...]: ...

    def close(self) -> None: ...
