"""The visualize tool: an interactive chart rendered by the MCP Apps view."""

from __future__ import annotations

import json
from importlib import resources
from typing import Annotated, Any, Final

from mcp.server.apps import Apps, client_supports_apps
from mcp.server.mcpserver.context import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, TextContent
from pydantic import BaseModel, Field, JsonValue

from bi_chart_mcp.charts.compile import compile_chart
from bi_chart_mcp.charts.intent import ChartIntent
from bi_chart_mcp.charts.recommend import complete_intent, profile_fields
from bi_chart_mcp.errors import ConfigError
from bi_chart_mcp.query.handles import QueryRecord
from bi_chart_mcp.query.table_text import markdown_table
from bi_chart_mcp.query.values import ColumnKind, column_kind, to_json_value
from bi_chart_mcp.services import Services
from bi_chart_mcp.settings import Limits
from bi_chart_mcp.tools.common import READ_ONLY, UNTRUSTED_NOTE, tool_errors
from bi_chart_mcp.tools.query_tools import ColumnMeta

CHART_RESOURCE_URI: Final = "ui://bi-chart/chart.html"
# Claude stops rendering MCP Apps whose tool result exceeds ~150,000 characters.
MAX_PAYLOAD_CHARS: Final = 100_000

_MARK_NAMES: Final[dict[str, str]] = {
    "bar": "Bar chart",
    "line": "Line chart",
    "area": "Area chart",
    "point": "Scatter plot",
    "arc": "Pie chart",
    "rect": "Heatmap",
    "text": "Single value",
}


class ChartPayload(BaseModel):
    """What the chart view receives. Mirrors ChartPayload in ui/src/payload.ts."""

    handle: str
    source: str
    sql: str
    display_sql: str
    executed_at: str
    elapsed_ms: int
    row_count: int = Field(description="Rows returned by the query (at most the row cap).")
    truncated: bool = Field(description="The query had more rows than the row cap.")
    chart_rows: int = Field(description="Rows included in `data` and drawn in the chart.")
    columns: list[ColumnMeta]
    data: dict[str, list[JsonValue]] = Field(description="Column-oriented chart data.")
    spec: dict[str, Any] = Field(description="Vega-Lite spec; reads the dataset named 'table'.")
    chart: ChartIntent = Field(description="The chart that was drawn, with defaults filled in.")
    summary: str
    notes: list[str]


def load_chart_html() -> str:
    """The built single-file view shipped inside the package."""
    path = resources.files("bi_chart_mcp").joinpath("ui", "chart.html")
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError(
            "The chart view is missing (src/bi_chart_mcp/ui/chart.html). "
            "Build it with: cd ui && npm ci && npm run build"
        ) from None


def _columnar(
    names: list[str], rows: tuple[tuple[object, ...], ...], max_chars: int
) -> dict[str, list[JsonValue]]:
    return {
        name: [to_json_value(row[i], max_chars) for row in rows] for i, name in enumerate(names)
    }


def _fit_rows(
    names: list[str], rows: tuple[tuple[object, ...], ...], limit: int, max_chars: int
) -> tuple[int, dict[str, list[JsonValue]]]:
    """The largest row count up to `limit` whose data stays under the payload budget."""
    count = min(limit, len(rows))
    data = _columnar(names, rows[:count], max_chars)
    while count > 1 and len(json.dumps(data, ensure_ascii=False)) > MAX_PAYLOAD_CHARS:
        count //= 2
        data = _columnar(names, rows[:count], max_chars)
    return count, data


def describe_chart(intent: ChartIntent) -> str:
    kind = _MARK_NAMES.get(intent.mark or "", "Chart")
    measure = f"{intent.aggregate}({intent.y})" if intent.aggregate and intent.y else intent.y
    measure = measure or "count"
    if intent.mark == "text":
        return f"{kind}: {measure}"
    text = f"{kind}: {measure} by {intent.x}"
    return f"{text}, split by {intent.color}" if intent.color else text


def build_payload(record: QueryRecord, chart: ChartIntent, limits: Limits) -> ChartPayload:
    result = record.result
    names = [c.name for c in result.columns]
    kinds: list[ColumnKind] = [column_kind(c.db_type) for c in result.columns]
    chart_rows, data = _fit_rows(names, result.rows, limits.chart_rows, limits.max_cell_chars)
    fields = profile_fields(list(zip(names, kinds, strict=True)), data)
    intent = complete_intent(chart, fields, chart_rows)
    spec = compile_chart(intent, {f.name: f for f in fields})
    notes: list[str] = []
    if not result.rows:
        notes.append("The query returned no rows.")
    if chart_rows < len(result.rows):
        notes.append(f"The chart shows the first {chart_rows} of {len(result.rows)} rows.")
    if result.truncated:
        notes.append(
            f"The query hit the {limits.row_cap}-row cap, so totals may be incomplete; "
            "aggregate in SQL instead."
        )
    return ChartPayload(
        handle=record.handle,
        source=record.source,
        sql=record.sql,
        display_sql=record.display_sql,
        executed_at=record.executed_at,
        elapsed_ms=result.elapsed_ms,
        row_count=len(result.rows),
        truncated=result.truncated,
        chart_rows=chart_rows,
        columns=[
            ColumnMeta(name=n, db_type=c.db_type, kind=k)
            for n, c, k in zip(names, result.columns, kinds, strict=True)
        ],
        data=data,
        spec=spec,
        chart=intent,
        summary=intent.title or describe_chart(intent),
        notes=notes,
    )


def chart_text(payload: ChartPayload, *, rendered: bool, limits: Limits) -> str:
    rows = f"{payload.row_count}{'+' if payload.truncated else ''} rows"
    head = (
        f"Showed the user an interactive chart ({payload.summary}) from {payload.source}, "
        f"{rows}, handle {payload.handle}. Discuss what it shows; no need to repeat every value."
        if rendered
        else f"{payload.summary} from {payload.source}, {rows} (handle {payload.handle}). "
        "This client cannot display interactive charts, so the data follows."
    )
    lines = [head, *payload.notes]
    names = [c.name for c in payload.columns]
    preview = min(limits.preview_rows, payload.chart_rows)
    if names and preview:
        table_rows = [[payload.data[n][i] for n in names] for i in range(preview)]
        lines += [UNTRUSTED_NOTE, "", markdown_table(names, table_rows)]
        if preview < payload.chart_rows:
            lines.append(f"(first {preview} of {payload.chart_rows} charted rows)")
    return "\n".join(lines)


def register_chart_tools(apps: Apps, services: Services) -> None:
    limits = services.limits
    apps.add_html_resource(
        CHART_RESOURCE_URI,
        load_chart_html(),
        name="chart",
        title="BI chart",
        description="Interactive Vega-Lite chart of a query result.",
        prefers_border=True,
    )

    @apps.tool(resource_uri=CHART_RESOURCE_URI, annotations=READ_ONLY, title="Visualize")
    async def visualize(
        ctx: Context,
        handle: Annotated[
            str | None, Field(description="Handle from a previous query call (reuses its rows).")
        ] = None,
        source: Annotated[
            str | None, Field(description="Source to query when no handle is given.")
        ] = None,
        sql: Annotated[
            str | None,
            Field(description="Read-only SELECT to run when no handle is given; aggregate in SQL."),
        ] = None,
        chart: Annotated[
            ChartIntent | None,
            Field(description="What to plot. Omit any part to let the server choose."),
        ] = None,
        title: Annotated[str | None, Field(description="Chart title.", max_length=200)] = None,
    ) -> Annotated[CallToolResult, ChartPayload]:
        """Show the user an interactive chart of a query result.

        Pass either `handle` (from query) or `source` + `sql`. The chart type and
        axes are chosen automatically unless you set them in `chart`. The user sees
        the chart, a table view and the SQL; you get a short summary and preview.
        """
        if handle and (source or sql):
            raise ToolError("[invalid_arguments] Pass either handle, or source and sql; not both.")
        if not handle and not (source and sql):
            raise ToolError("[invalid_arguments] Pass handle, or both source and sql.")
        intent = chart or ChartIntent()
        if title:
            intent = intent.model_copy(update={"title": title})
        with tool_errors():
            if handle:
                record = services.queries.record(handle)
            else:
                record = await services.queries.run(source or "", sql or "")
            payload = build_payload(record, intent, limits)
        text = chart_text(payload, rendered=client_supports_apps(ctx), limits=limits)
        return CallToolResult(
            content=[TextContent(type="text", text=text)],
            structured_content=payload.model_dump(mode="json"),
        )
