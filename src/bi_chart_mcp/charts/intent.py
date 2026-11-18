"""ChartIntent: the small, validated chart description the model writes.

The model never writes Vega-Lite directly; `compile.py` turns an intent into a spec.
Every field is optional: missing parts are filled in by `recommend.py`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from bi_chart_mcp.errors import BiChartError

Mark = Literal["bar", "line", "area", "point", "arc", "rect", "text"]
Aggregate = Literal["sum", "mean", "median", "min", "max", "count", "distinct"]
TimeUnit = Literal["year", "quarter", "month", "week", "date", "hour", "yearquarter", "yearmonth"]
Sort = Literal["data", "x", "-x", "y", "-y"]
Stack = Literal["stacked", "grouped", "normalized"]

_FieldName = Annotated[str, Field(min_length=1, max_length=200)]


class ChartError(BiChartError):
    """The requested chart cannot be built from this result."""

    default_code = "chart_error"


class ChartIntent(BaseModel):
    """What to plot. Field names refer to result columns."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    mark: Mark | None = Field(
        default=None,
        description=(
            "bar, line, area, point (scatter), arc (pie/donut), rect (heatmap) or text "
            "(single big number). Omit to let the server choose."
        ),
    )
    x: _FieldName | None = Field(default=None, description="Column for the x axis (or slices).")
    y: _FieldName | None = Field(default=None, description="Column for the y axis (or values).")
    color: _FieldName | None = Field(
        default=None, description="Column for colour: series, stacks or heatmap intensity."
    )
    size: _FieldName | None = Field(default=None, description="Column for point size (scatter).")
    aggregate: Aggregate | None = Field(
        default=None,
        description="Aggregate y in the chart. Prefer aggregating in SQL; use for raw rows.",
    )
    time_unit: TimeUnit | None = Field(
        default=None, description="Bucket a date x axis, e.g. month or yearmonth."
    )
    sort: Sort | None = Field(
        default=None,
        description="Category order: data (keep SQL ORDER BY), x, -x, y or -y (descending).",
    )
    stack: Stack | None = Field(
        default=None, description="With color on bar/area: stacked (default), grouped, normalized."
    )
    horizontal: bool | None = Field(
        default=None, description="Horizontal bars; chosen automatically for many categories."
    )
    donut: bool = Field(default=False, description="Arc only: draw a donut instead of a pie.")
    title: Annotated[str, Field(max_length=200)] | None = None
