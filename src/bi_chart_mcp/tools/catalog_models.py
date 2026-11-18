"""Structured output of the catalog tools, and how to build it from catalog results."""

from __future__ import annotations

from typing import Final

from pydantic import BaseModel, Field, JsonValue

from bi_chart_mcp.catalog.service import SourceSummary, TableMatch, TableProfile
from bi_chart_mcp.query.values import ColumnKind, column_kind, to_json_value
from bi_chart_mcp.sources.base import ForeignKey, TableInfo

OVERVIEW_TABLE_LIMIT: Final = 100


class TableSummary(BaseModel):
    name: str = Field(description="Table name to use in SQL (schema-qualified for PostgreSQL).")
    kind: str
    description: str | None = None
    row_estimate: int | None = None


class SourceOverview(BaseModel):
    name: str
    dialect: str = Field(description="SQL dialect to write for this source.")
    description: str | None = None
    table_count: int
    tables: list[TableSummary]
    tables_truncated: bool = Field(description="True when only the first tables are listed.")


class TableMatchOut(BaseModel):
    source: str
    table: str
    kind: str
    description: str | None = None
    matched_columns: list[str]


class SearchCatalogOutput(BaseModel):
    sources: list[SourceOverview] | None = Field(
        default=None, description="Overview of every source (when no search text was given)."
    )
    matches: list[TableMatchOut] | None = Field(
        default=None, description="Tables matching the search text, best first."
    )
    hint: str


class ValueCountOut(BaseModel):
    value: JsonValue
    count: int


class ColumnOut(BaseModel):
    name: str
    db_type: str
    kind: ColumnKind
    nullable: bool
    description: str | None = None
    top_values: list[ValueCountOut] | None = Field(
        default=None, description="Most frequent values; only for columns in sample_columns."
    )


class ForeignKeyOut(BaseModel):
    columns: list[str]
    references_table: str
    references_columns: list[str]


class DescribeTableOutput(BaseModel):
    source: str
    table: str
    kind: str
    description: str | None = None
    row_estimate: int | None = None
    columns: list[ColumnOut]
    foreign_keys: list[ForeignKeyOut] = Field(description="Declared by the database.")
    inferred_joins: list[ForeignKeyOut] = Field(
        default_factory=list[ForeignKeyOut],
        description="Likely joins guessed from column names (e.g. customer_id -> customers.id).",
    )


def table_summary(table: TableInfo) -> TableSummary:
    return TableSummary(
        name=table.qualified_name,
        kind=table.kind,
        description=table.description,
        row_estimate=table.row_estimate,
    )


def source_overview(summary: SourceSummary) -> SourceOverview:
    tables = summary.tables[:OVERVIEW_TABLE_LIMIT]
    return SourceOverview(
        name=summary.name,
        dialect=summary.dialect,
        description=summary.description,
        table_count=len(summary.tables),
        tables=[table_summary(t) for t in tables],
        tables_truncated=len(summary.tables) > len(tables),
    )


def table_match(match: TableMatch) -> TableMatchOut:
    return TableMatchOut(
        source=match.source,
        table=match.table.qualified_name,
        kind=match.table.kind,
        description=match.table.description,
        matched_columns=list(match.matched_columns),
    )


def _foreign_key(fk: ForeignKey) -> ForeignKeyOut:
    return ForeignKeyOut(
        columns=list(fk.columns),
        references_table=fk.references_table,
        references_columns=list(fk.references_columns),
    )


def describe_output(profile: TableProfile, max_chars: int) -> DescribeTableOutput:
    detail = profile.detail
    columns = [
        ColumnOut(
            name=c.name,
            db_type=c.db_type,
            kind=column_kind(c.db_type),
            nullable=c.nullable,
            description=c.description,
            top_values=(
                [
                    ValueCountOut(value=to_json_value(v.value, max_chars), count=v.count)
                    for v in profile.top_values[c.name]
                ]
                if c.name in profile.top_values
                else None
            ),
        )
        for c in detail.columns
    ]
    return DescribeTableOutput(
        source=profile.source,
        table=detail.table.qualified_name,
        kind=detail.table.kind,
        description=detail.table.description,
        row_estimate=detail.table.row_estimate,
        columns=columns,
        foreign_keys=[_foreign_key(fk) for fk in detail.foreign_keys],
        inferred_joins=[_foreign_key(fk) for fk in profile.inferred_joins],
    )
