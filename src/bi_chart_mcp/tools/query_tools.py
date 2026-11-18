"""The query tool: run one read-only SQL query and preview the result."""

from __future__ import annotations

from typing import Annotated

from mcp.server import MCPServer
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, Field, JsonValue

from bi_chart_mcp.query.handles import QueryRecord
from bi_chart_mcp.query.table_text import markdown_table
from bi_chart_mcp.query.values import ColumnKind, column_kind, row_to_json
from bi_chart_mcp.services import Services
from bi_chart_mcp.settings import Limits
from bi_chart_mcp.tools.common import READ_ONLY, UNTRUSTED_NOTE, tool_errors


class ColumnMeta(BaseModel):
    name: str
    db_type: str
    kind: ColumnKind


class QueryOutput(BaseModel):
    handle: str = Field(description="Pass to visualize to chart this result without re-running.")
    source: str
    sql: str = Field(description="The SQL that was actually executed (row cap applied).")
    executed_at: str
    elapsed_ms: int
    row_count: int = Field(description="Rows fetched, at most the row cap.")
    truncated: bool = Field(description="True when the database had more rows than the cap.")
    columns: list[ColumnMeta]
    rows: list[list[JsonValue]] = Field(description="The first rows of the result.")


def query_output(record: QueryRecord, limits: Limits) -> QueryOutput:
    result = record.result
    preview = result.rows[: limits.preview_rows]
    return QueryOutput(
        handle=record.handle,
        source=record.source,
        sql=record.sql,
        executed_at=record.executed_at,
        elapsed_ms=result.elapsed_ms,
        row_count=len(result.rows),
        truncated=result.truncated,
        columns=[
            ColumnMeta(name=c.name, db_type=c.db_type, kind=column_kind(c.db_type))
            for c in result.columns
        ],
        rows=[row_to_json(r, limits.max_cell_chars) for r in preview],
    )


def summary_line(output: QueryOutput) -> str:
    more = "+" if output.truncated else ""
    return (
        f"{output.row_count}{more} rows from {output.source} in {output.elapsed_ms} ms "
        f"(handle {output.handle})."
    )


def query_text(output: QueryOutput, limits: Limits) -> str:
    lines = [summary_line(output)]
    if output.truncated:
        lines.append(
            f"The result hit the {limits.row_cap}-row cap; aggregate or filter in SQL instead."
        )
    if len(output.rows) < output.row_count:
        lines.append(f"Showing the first {len(output.rows)} rows.")
    if output.columns:
        lines += [UNTRUSTED_NOTE, "", markdown_table([c.name for c in output.columns], output.rows)]
    return "\n".join(lines)


def register_query_tools(server: MCPServer, services: Services) -> None:
    limits = services.limits

    @server.tool(annotations=READ_ONLY)
    async def query(
        source: Annotated[str, Field(description="Source name from search_catalog.")],
        sql: Annotated[
            str,
            Field(
                description=(
                    "One read-only SELECT (WITH allowed) in the source's dialect. "
                    "Aggregate in SQL rather than fetching raw rows."
                )
            ),
        ],
    ) -> Annotated[CallToolResult, QueryOutput]:
        """Run one read-only SQL query and preview the rows.

        Returns the first rows, column types and a handle. To chart the result, call
        visualize with the handle (or call visualize directly with the SQL).
        Writes, DDL, multiple statements, system catalogs and file/network functions
        are rejected with an explanation.
        """
        with tool_errors():
            record = await services.queries.run(source, sql)
        output = query_output(record, limits)
        return CallToolResult(
            content=[TextContent(type="text", text=query_text(output, limits))],
            structured_content=output.model_dump(mode="json"),
        )
