"""Compile a completed ChartIntent into a Vega-Lite v6 spec.

The spec never carries data or URLs: it reads the named dataset "table", which the
chart view fills from the tool result.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

from bi_chart_mcp.charts.intent import ChartError, ChartIntent
from bi_chart_mcp.charts.recommend import FieldProfile
from bi_chart_mcp.query.values import ColumnKind

VEGA_LITE_SCHEMA: Final = "https://vega.github.io/schema/vega-lite/v6.json"
DATASET_NAME: Final = "table"

_VL_TYPE: Final[dict[ColumnKind, str]] = {
    "number": "quantitative",
    "temporal": "temporal",
    "text": "nominal",
    "boolean": "nominal",
    "other": "nominal",
}
_HEIGHT: Final = 320
_KPI_HEIGHT: Final = 120
_BAR_STEP: Final = 22
_LINE_POINTS_MAX: Final = 40
_COUNT_AGGREGATES: Final = frozenset({"count", "distinct"})

Spec = dict[str, Any]


def vl_type(kind: ColumnKind) -> str:
    return _VL_TYPE[kind]


def compile_chart(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    """`intent` must already be completed by `recommend.complete_intent`."""
    if intent.mark is None:
        raise ChartError("Internal error: chart mark was not decided.")
    builders = {
        "bar": _bar,
        "line": _line_or_area,
        "area": _line_or_area,
        "point": _point,
        "arc": _arc,
        "rect": _rect,
        "text": _text,
    }
    spec: Spec = {
        "$schema": VEGA_LITE_SCHEMA,
        "data": {"name": DATASET_NAME},
        "width": "container",
        "height": _HEIGHT,
        "autosize": {"type": "fit", "contains": "padding"},
    }
    spec.update(builders[intent.mark](intent, fields))
    if intent.title:
        spec["title"] = intent.title
    return spec


# ---------------------------------------------------------------- channels


def _field(
    name: str, fields: Mapping[str, FieldProfile], *, time_unit: str | None = None
) -> dict[str, Any]:
    profile = fields[name]
    channel: dict[str, Any] = {"field": name, "type": vl_type(profile.kind), "title": name}
    if profile.kind == "temporal":
        channel["scale"] = {"type": "utc"}  # ISO strings parse as UTC; don't shift by time zone
        if time_unit:
            channel["timeUnit"] = f"utc{time_unit}"
    return channel


def _value(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> dict[str, Any]:
    """The measured channel: y with its aggregate, or a row count when there is no y."""
    if intent.y is None:
        return {"aggregate": "count", "type": "quantitative", "title": "count"}
    profile = fields[intent.y]
    if profile.kind != "number" and intent.aggregate not in _COUNT_AGGREGATES:
        raise ChartError(
            f"Column {intent.y!r} is {profile.kind}, not numeric, so it cannot be the measured "
            "value. Pick a numeric column for y, or use aggregate count/distinct."
        )
    channel = {"field": intent.y, "type": "quantitative", "title": intent.y}
    if intent.aggregate:
        channel["aggregate"] = intent.aggregate
        channel["title"] = f"{intent.aggregate}({intent.y})"
    return channel


def _color(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> dict[str, Any]:
    if intent.color is None:
        return {}
    return {"color": _field(intent.color, fields)}


def _tooltip_mark(kind: str, **extra: Any) -> dict[str, Any]:
    return {"type": kind, "tooltip": True, **extra}


def _need(value: str | None, role: str) -> str:
    if value is None:
        raise ChartError(f"Internal error: the chart's {role} was not decided.")
    return value


# ---------------------------------------------------------------- marks


def _bar(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    x = _need(intent.x, "x")
    category = _field(x, fields, time_unit=intent.time_unit)
    value = _value(intent, fields)
    if fields[x].kind == "number" and intent.y is None:
        category["bin"] = True  # histogram
    horizontal = bool(intent.horizontal)
    category_channel, value_channel = ("y", "x") if horizontal else ("x", "y")
    sort = _sort(intent, value_channel)
    if sort is not _NO_SORT:
        category["sort"] = sort
    if intent.stack == "normalized":
        value["stack"] = "normalize"
    encoding: dict[str, Any] = {category_channel: category, value_channel: value}
    encoding.update(_color(intent, fields))
    if intent.color and intent.stack == "grouped":
        # Side-by-side groups need discrete colours, even for dates or numbers.
        encoding["color"] = {"field": intent.color, "type": "nominal", "title": intent.color}
        encoding[f"{category_channel}Offset"] = {"field": intent.color, "type": "nominal"}
    spec: Spec = {"mark": _tooltip_mark("bar"), "encoding": encoding}
    if horizontal:
        spec["height"] = {"step": _BAR_STEP}
    return spec


_NO_SORT: Final = object()


def _sort(intent: ChartIntent, value_channel: str) -> Any:
    match intent.sort:
        case None:
            return _NO_SORT
        case "data":
            return None  # keep the SQL ORDER BY
        case "x":
            return "ascending"
        case "-x":
            return "descending"
        case "y":
            return value_channel
        case "-y":
            return f"-{value_channel}"


def _line_or_area(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    x_name, mark = _need(intent.x, "x"), _need(intent.mark, "mark")
    x = _field(x_name, fields, time_unit=intent.time_unit)
    y = _value(intent, fields)
    if mark == "area" and intent.stack == "normalized":
        y["stack"] = "normalize"
    mark_extra: dict[str, Any] = {}
    if mark == "line" and fields[x_name].distinct <= _LINE_POINTS_MAX:
        mark_extra["point"] = True
    encoding = {"x": x, "y": y, **_color(intent, fields)}
    return {"mark": _tooltip_mark(mark, **mark_extra), "encoding": encoding}


def _point(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    encoding: dict[str, Any] = {
        "x": _field(_need(intent.x, "x"), fields),
        "y": _field(_need(intent.y, "y"), fields),
        **_color(intent, fields),
    }
    if intent.size:
        encoding["size"] = _field(intent.size, fields)
    return {"mark": _tooltip_mark("point", filled=True), "encoding": encoding}


def _arc(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    theta = _value(intent, fields)
    if intent.y is not None and "aggregate" not in theta:
        theta["aggregate"] = "sum"  # several rows per slice add up
    category = _field(_need(intent.x, "x"), fields)
    mark = _tooltip_mark("arc", innerRadius=70) if intent.donut else _tooltip_mark("arc")
    return {
        "mark": mark,
        "encoding": {"theta": {**theta, "stack": True}, "color": category},
        "view": {"stroke": None},
    }


def _rect(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    x, y = _need(intent.x, "x"), _need(intent.y, "y")
    if intent.color is None:
        color: dict[str, Any] = {"aggregate": "count", "type": "quantitative", "title": "count"}
    else:
        color = _value(intent.model_copy(update={"y": intent.color}), fields)
    encoding = {
        "x": _field(x, fields, time_unit=intent.time_unit),
        "y": _field(y, fields),
        "color": color,
    }
    return {"mark": _tooltip_mark("rect"), "encoding": encoding}


def _text(intent: ChartIntent, fields: Mapping[str, FieldProfile]) -> Spec:
    _need(intent.y, "y")
    value = _value(intent, fields)
    value.pop("title", None)
    return {
        "height": _KPI_HEIGHT,
        "mark": {"type": "text", "fontSize": 48, "fontWeight": "bold"},
        "encoding": {"text": {**value, "format": ",~r"}},
    }
