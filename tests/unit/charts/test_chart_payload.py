"""Tests for the visualize payload: size budget, summaries and notes."""

from __future__ import annotations

import json
from collections.abc import Sequence

from bi_chart_mcp.charts.intent import ChartIntent
from bi_chart_mcp.query.handles import QueryRecord
from bi_chart_mcp.settings import Limits
from bi_chart_mcp.sources.base import Column, ResultSet
from bi_chart_mcp.tools.chart_tools import (
    MAX_PAYLOAD_CHARS,
    build_payload,
    chart_text,
    describe_chart,
)


def _record(rows: Sequence[tuple[object, ...]], *, truncated: bool = False) -> QueryRecord:
    result = ResultSet(
        columns=(Column("label", "text"), Column("value", "int8")),
        rows=tuple(rows),
        truncated=truncated,
        elapsed_ms=5,
    )
    return QueryRecord(
        "q_1", "shop", "SELECT 1", "SELECT\n  1", result, "2026-11-18T00:00:00+00:00"
    )


def test_payload_stays_under_the_render_budget() -> None:
    big = [(f"label {i} " + "x" * 400, i) for i in range(2000)]
    limits = Limits(chart_rows=2000, max_cell_chars=1000)
    payload = build_payload(_record(big), ChartIntent(), limits)
    assert len(json.dumps(payload.model_dump(mode="json"))) < MAX_PAYLOAD_CHARS * 1.2
    assert payload.chart_rows < 2000
    assert any("first" in note for note in payload.notes)


def test_empty_and_truncated_results_get_notes() -> None:
    empty = build_payload(_record([]), ChartIntent(mark="bar", x="label", y="value"), Limits())
    assert "no rows" in empty.notes[0]
    capped = build_payload(
        _record([("a", 1), ("b", 2)], truncated=True), ChartIntent(), Limits(row_cap=2)
    )
    assert any("2-row cap" in note for note in capped.notes)


def test_describe_chart() -> None:
    assert (
        describe_chart(ChartIntent(mark="bar", x="label", y="value")) == "Bar chart: value by label"
    )
    assert (
        describe_chart(ChartIntent(mark="line", x="d", y="v", color="r", aggregate="sum"))
        == "Line chart: sum(v) by d, split by r"
    )
    assert describe_chart(ChartIntent(mark="text", y="total")) == "Single value: total"
    assert describe_chart(ChartIntent(mark="arc", x="label")) == "Pie chart: count by label"


def test_chart_text_previews_rows_for_the_model() -> None:
    payload = build_payload(_record([("a", 1), ("b", 2)]), ChartIntent(), Limits(preview_rows=1))
    text = chart_text(payload, rendered=True, limits=Limits(preview_rows=1))
    assert text.startswith("Showed the user an interactive chart")
    assert "Chart data (all rows, aggregated as drawn):" in text
    assert "| label | value |" in text
    # preview_rows is a budget for the digest, never a reason to hide charted rows.
    assert "| b | 2 |" in text
    assert "| a | 1 |" in text
