"""Tests for chart recommendation and Vega-Lite compilation.

Compiled specs for representative scenarios are compared with golden files in
tests/golden/charts/. The UI test suite compiles the same files with the real
vega-lite library, so they are proven valid. Regenerate with UPDATE_GOLDEN=1.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from bi_chart_mcp.charts.compile import compile_chart
from bi_chart_mcp.charts.intent import ChartError, ChartIntent
from bi_chart_mcp.charts.recommend import FieldProfile, complete_intent, profile_fields
from bi_chart_mcp.query.values import ColumnKind, JsonValue

Scenario = tuple[list[tuple[str, ColumnKind]], dict[str, list[JsonValue]]]

GOLDEN_DIR = Path(__file__).resolve().parents[2] / "golden" / "charts"

MONTHLY: Scenario = (
    [("month", "temporal"), ("region", "text"), ("revenue", "number")],
    {
        "month": ["2026-01-01", "2026-02-01", "2026-01-01", "2026-02-01"],
        "region": ["North", "North", "South", "South"],
        "revenue": [10.5, 12.0, 7.25, 9.0],
    },
)
BY_STATUS: Scenario = (
    [("status", "text"), ("n", "number")],
    {"status": ["paid", "refunded"], "n": [30, 10]},
)
MANY_CATEGORIES: Scenario = (
    [("product", "text"), ("units", "number")],
    {"product": [f"Product {i}" for i in range(12)], "units": list(range(12))},
)
SCATTER: Scenario = (
    [("price", "number"), ("units", "number"), ("category", "text")],
    {"price": [1.0, 2.5, 4.0], "units": [10, 4, 1], "category": ["a", "b", "a"]},
)
KPI: Scenario = ([("total", "number")], {"total": [12345.678]})
SINGLE_NUMBER_COLUMN: Scenario = ([("amount", "number")], {"amount": [1.0, 2.0, 2.5, 9.0]})
TEXT_ONLY: Scenario = ([("status", "text")], {"status": ["a", "b", "a"]})
HEATMAP: Scenario = (
    [("weekday", "text"), ("hour", "text"), ("orders", "number")],
    {"weekday": ["Mon", "Mon", "Tue"], "hour": ["9", "10", "9"], "orders": [3, 5, 2]},
)
NOTHING_CHARTABLE: Scenario = ([("payload", "other")], {"payload": [{"a": 1}]})


def _build(
    scenario: Scenario, intent: ChartIntent | None = None
) -> tuple[ChartIntent, dict[str, Any]]:
    columns, data = scenario
    fields = profile_fields(columns, data)
    row_count = len(next(iter(data.values())))
    completed = complete_intent(intent or ChartIntent(), fields, row_count)
    return completed, compile_chart(completed, {f.name: f for f in fields})


# ---------------------------------------------------------------- recommendation


def test_profile_fields_counts_distinct_and_label_length() -> None:
    (month, region, revenue) = profile_fields(*MONTHLY)
    assert month.distinct == 2
    assert region == FieldProfile("region", "text", 2, 5)
    assert revenue.kind == "number"


def test_time_series_becomes_line_with_series_colour() -> None:
    intent, _ = _build(MONTHLY)
    assert (intent.mark, intent.x, intent.y, intent.color) == ("line", "month", "revenue", "region")


def test_categories_become_sorted_vertical_bars() -> None:
    intent, _ = _build(BY_STATUS)
    assert (intent.mark, intent.x, intent.y) == ("bar", "status", "n")
    assert intent.sort == "-y"
    assert intent.horizontal is False


def test_many_categories_become_horizontal_bars() -> None:
    intent, spec = _build(MANY_CATEGORIES)
    assert intent.horizontal is True
    assert spec["encoding"]["y"]["field"] == "product"
    assert spec["encoding"]["y"]["sort"] == "-x"
    assert spec["height"] == {"step": 22}


def test_two_numbers_become_scatter() -> None:
    intent, _ = _build(SCATTER)
    assert (intent.mark, intent.x, intent.y, intent.color) == (
        "point",
        "price",
        "units",
        "category",
    )


def test_single_value_becomes_kpi() -> None:
    intent, spec = _build(KPI)
    assert intent.mark == "text"
    assert spec["encoding"]["text"]["field"] == "total"


def test_single_numeric_column_becomes_histogram() -> None:
    intent, spec = _build(SINGLE_NUMBER_COLUMN)
    assert intent.mark == "bar"
    assert spec["encoding"]["x"]["bin"] is True
    assert spec["encoding"]["y"]["aggregate"] == "count"


def test_text_only_becomes_count_bars() -> None:
    _, spec = _build(TEXT_ONLY)
    assert spec["encoding"]["y"] == {"aggregate": "count", "type": "quantitative", "title": "count"}


def test_explicit_intent_is_respected() -> None:
    intent, _ = _build(MONTHLY, ChartIntent(mark="bar", x="region", y="revenue", color="month"))
    assert (intent.mark, intent.x, intent.y, intent.color) == ("bar", "region", "revenue", "month")
    assert intent.stack == "stacked"


def test_unknown_column_lists_available_columns() -> None:
    with pytest.raises(ChartError, match="Available columns: status, n"):
        _build(BY_STATUS, ChartIntent(x="nope"))


def test_non_numeric_value_is_rejected_unless_counted() -> None:
    with pytest.raises(ChartError, match="not numeric"):
        _build(BY_STATUS, ChartIntent(mark="bar", x="n", y="status"))
    intent, spec = _build(
        BY_STATUS, ChartIntent(mark="bar", x="n", y="status", aggregate="distinct")
    )
    assert spec["encoding"]["y"]["aggregate"] == "distinct"
    assert intent.y == "status"


def test_nothing_chartable() -> None:
    with pytest.raises(ChartError, match="table"):
        _build(NOTHING_CHARTABLE)


def test_line_without_numeric_column_is_rejected() -> None:
    with pytest.raises(ChartError, match="y axis of a line chart"):
        _build(TEXT_ONLY, ChartIntent(mark="line"))


def test_intent_rejects_unknown_keys_and_marks() -> None:
    with pytest.raises(ValidationError):
        ChartIntent.model_validate({"mark": "pie"})
    with pytest.raises(ValidationError):
        ChartIntent.model_validate({"data": {"url": "http://evil"}})


# ---------------------------------------------------------------- compiled specs


def test_specs_never_embed_data_or_urls() -> None:
    for scenario in (MONTHLY, BY_STATUS, SCATTER, KPI, HEATMAP):
        _, spec = _build(scenario)
        assert spec["data"] == {"name": "table"}
        assert "url" not in json.dumps(spec).replace("vega.github.io", "")
        assert "usermeta" not in spec


def test_temporal_axes_use_utc() -> None:
    _, spec = _build(MONTHLY, ChartIntent(time_unit="yearmonth"))
    assert spec["encoding"]["x"]["scale"] == {"type": "utc"}
    assert spec["encoding"]["x"]["timeUnit"] == "utcyearmonth"


GOLDEN: dict[str, tuple[Scenario, ChartIntent]] = {
    "line_series": (MONTHLY, ChartIntent(title="Revenue by month")),
    "bar_sorted": (BY_STATUS, ChartIntent()),
    "bar_horizontal": (MANY_CATEGORIES, ChartIntent()),
    "bar_grouped": (MONTHLY, ChartIntent(mark="bar", x="region", color="month", stack="grouped")),
    "bar_normalized": (
        MONTHLY,
        ChartIntent(mark="bar", x="month", color="region", stack="normalized", time_unit="month"),
    ),
    "bar_data_order": (BY_STATUS, ChartIntent(sort="data")),
    "area_stacked": (MONTHLY, ChartIntent(mark="area")),
    "scatter_sized": (SCATTER, ChartIntent(size="units", y="price", x="units")),
    "pie": (BY_STATUS, ChartIntent(mark="arc")),
    "donut": (BY_STATUS, ChartIntent(mark="arc", donut=True)),
    "heatmap": (HEATMAP, ChartIntent(mark="rect", x="hour", y="weekday", color="orders")),
    "heatmap_count": (HEATMAP, ChartIntent(mark="rect", x="hour", y="weekday")),
    "kpi": (KPI, ChartIntent(title="Total revenue")),
    "histogram": (SINGLE_NUMBER_COLUMN, ChartIntent()),
    "aggregated_raw_rows": (
        MONTHLY,
        ChartIntent(mark="bar", x="region", y="revenue", aggregate="mean", sort="x"),
    ),
}


@pytest.mark.parametrize("name", sorted(GOLDEN))
def test_golden_specs(name: str) -> None:
    scenario, intent = GOLDEN[name]
    _, spec = _build(scenario, intent)
    golden = GOLDEN_DIR / f"{name}.json"
    payload = {"spec": spec, "data": _rows(scenario[1])}
    rendered = json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    if os.environ.get("UPDATE_GOLDEN") == "1" or not golden.exists():
        golden.parent.mkdir(parents=True, exist_ok=True)
        golden.write_text(rendered, encoding="utf-8")
    assert golden.read_text(encoding="utf-8") == rendered


def _rows(data: Mapping[str, Sequence[JsonValue]]) -> list[dict[str, JsonValue]]:
    names = list(data)
    return [dict(zip(names, values, strict=True)) for values in zip(*data.values(), strict=True)]
