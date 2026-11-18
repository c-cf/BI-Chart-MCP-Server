"""Tools that describe what can be queried: search_catalog and describe_table."""

from __future__ import annotations

from typing import Annotated

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, Field

from bi_chart_mcp.services import Services
from bi_chart_mcp.tools.catalog_models import (
    DescribeTableOutput,
    SearchCatalogOutput,
    describe_output,
    source_overview,
    table_match,
)
from bi_chart_mcp.tools.catalog_text import describe_text, search_text
from bi_chart_mcp.tools.common import READ_ONLY, tool_errors

_HINT_OVERVIEW = "Next: describe_table for columns, then query or visualize."
_HINT_MATCHES = "Next: describe_table on a match to see its columns."
_HINT_NO_MATCH = "No match. Call search_catalog without a query for an overview."


def _result(text: str, output: BaseModel) -> CallToolResult:
    """Compact text for the model; structured data without nulls for hosts and the UI."""
    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        structured_content=output.model_dump(mode="json", exclude_none=True),
    )


async def search_catalog_output(
    services: Services, query: str | None, source: str | None
) -> SearchCatalogOutput:
    if query and query.strip():
        matches = await services.catalog.search(query, source)
        return SearchCatalogOutput(
            matches=[table_match(m) for m in matches],
            hint=_HINT_MATCHES if matches else _HINT_NO_MATCH,
        )
    overview = await services.catalog.overview(source)
    return SearchCatalogOutput(sources=[source_overview(s) for s in overview], hint=_HINT_OVERVIEW)


def register_catalog_tools(server: MCPServer, services: Services) -> None:
    @server.tool(annotations=READ_ONLY)
    async def search_catalog(
        query: Annotated[
            str | None,
            Field(description="Words to look for in table names, descriptions and column names."),
        ] = None,
        source: Annotated[str | None, Field(description="Limit to one source.")] = None,
    ) -> Annotated[CallToolResult, SearchCatalogOutput]:
        """Find the tables you can query.

        Without `query`: an overview of every source (its SQL dialect and tables).
        With `query`: tables whose name, description or columns match, best first.
        Call this before writing SQL, then use describe_table for column details.
        """
        with tool_errors():
            output = await search_catalog_output(services, query, source)
        return _result(search_text(output), output)

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
    ) -> Annotated[CallToolResult, DescribeTableOutput]:
        """Columns, types, descriptions and joins of one table.

        Shows declared foreign keys and joins guessed from column names. Optionally the
        most frequent values of selected columns.
        """
        with tool_errors():
            profile = await services.catalog.describe(source, table, sample_columns or ())
        output = describe_output(profile, services.limits.max_cell_chars)
        return _result(describe_text(output), output)
