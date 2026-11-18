"""Tools that describe what can be queried: search_catalog and describe_table."""

from __future__ import annotations

from typing import Annotated, Final

from mcp.server import MCPServer
from pydantic import BaseModel, Field, JsonValue

from bi_chart_mcp.catalog.service import SourceSummary, TableMatch, TableProfile
from bi_chart_mcp.query.values import ColumnKind, column_kind, to_json_value
from bi_chart_mcp.services import Services
from bi_chart_mcp.sources.base import TableInfo
from bi_chart_mcp.tools.common import READ_ONLY, tool_errors

_OVERVIEW_TABLE_LIMIT: Final = 100


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
    foreign_keys: list[ForeignKeyOut]


def _table_summary(table: TableInfo) -> TableSummary:
    return TableSummary(
        name=table.qualified_name,
        kind=table.kind,
        description=table.description,
        row_estimate=table.row_estimate,
    )


def _overview(summary: SourceSummary) -> SourceOverview:
    tables = summary.tables[:_OVERVIEW_TABLE_LIMIT]
    return SourceOverview(
        name=summary.name,
        dialect=summary.dialect,
        description=summary.description,
        table_count=len(summary.tables),
        tables=[_table_summary(t) for t in tables],
        tables_truncated=len(summary.tables) > len(tables),
    )


def _match(match: TableMatch) -> TableMatchOut:
    return TableMatchOut(
        source=match.source,
        table=match.table.qualified_name,
        kind=match.table.kind,
        description=match.table.description,
        matched_columns=list(match.matched_columns),
    )


def _describe_output(profile: TableProfile, max_chars: int) -> DescribeTableOutput:
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
        foreign_keys=[
            ForeignKeyOut(
                columns=list(fk.columns),
                references_table=fk.references_table,
                references_columns=list(fk.references_columns),
            )
            for fk in detail.foreign_keys
        ],
    )


def register_catalog_tools(server: MCPServer, services: Services) -> None:
    @server.tool(annotations=READ_ONLY)
    async def search_catalog(
        query: Annotated[
            str | None,
            Field(description="Words to look for in table names, descriptions and column names."),
        ] = None,
        source: Annotated[str | None, Field(description="Limit to one source.")] = None,
    ) -> SearchCatalogOutput:
        """Find the tables you can query.

        Without `query`: an overview of every source (its SQL dialect and tables).
        With `query`: tables whose name, description or columns match, best first.
        Call this before writing SQL, then use describe_table for column details.
        """
        with tool_errors():
            if query and query.strip():
                matches = await services.catalog.search(query, source)
                return SearchCatalogOutput(
                    matches=[_match(m) for m in matches],
                    hint=(
                        "Use describe_table on a match to see its columns."
                        if matches
                        else "No match. Call search_catalog without a query for an overview."
                    ),
                )
            overview = await services.catalog.overview(source)
            return SearchCatalogOutput(
                sources=[_overview(s) for s in overview],
                hint="Use describe_table for columns, then query or visualize.",
            )

    @server.tool(annotations=READ_ONLY)
    async def describe_table(
        source: Annotated[str, Field(description="Source name from search_catalog.")],
        table: Annotated[str, Field(description="Table name as listed by search_catalog.")],
        sample_columns: Annotated[
            list[str] | None,
            Field(
                description=(
                    "Columns to return their most frequent values for (e.g. category or "
                    "status columns, to learn valid filter values). Avoid personal data."
                ),
                max_length=10,
            ),
        ] = None,
    ) -> DescribeTableOutput:
        """Columns, types, descriptions and foreign keys of one table.

        Optionally the most frequent values of selected columns.
        """
        with tool_errors():
            profile = await services.catalog.describe(source, table, sample_columns or ())
            return _describe_output(profile, services.limits.max_cell_chars)
