"""The model must see everything the chart shows, compactly (not just the first rows)."""

from __future__ import annotations

from typing import Any

from bi_chart_mcp.charts.digest import chart_digest
from bi_chart_mcp.charts.intent import ChartIntent
from bi_chart_mcp.query.formatting import format_value
from bi_chart_mcp.query.values import ColumnKind, JsonValue

CATEGORIES = ["Electronics", "Home", "Office", "Outdoor", "Toys"]


def _monthly() -> tuple[dict[str, ColumnKind], dict[str, list[JsonValue]]]:
    """22 months x 5 categories in long format, with a November spike."""
    months = [f"{2025 + (m // 12)}-{(m % 12) + 1:02d}-01T00:00:00" for m in range(22)]
    data: dict[str, list[JsonValue]] = {"month": [], "category": [], "revenue": []}
    for month in months:
        for i, category in enumerate(CATEGORIES):
            spike = 3.0 if month.startswith("2025-11") else 1.0
            data["month"].append(month)
            data["category"].append(category)
            data["revenue"].append(round(1000.5 * (5 - i) * spike, 3))
    return {"month": "temporal", "category": "text", "revenue": "number"}, data


def _digest(intent: ChartIntent, kinds: dict[str, ColumnKind], data: Any, **kw: Any) -> str:
    return chart_digest(intent, kinds, data, **kw)


def test_long_time_series_is_pivoted_so_every_point_is_visible() -> None:
    kinds, data = _monthly()
    text = _digest(ChartIntent(mark="line", x="month", y="revenue", color="category"), kinds, data)
    lines = text.splitlines()
    assert "| month | Electronics | Home | Office | Outdoor | Toys |" in lines
    # All 22 months are present, including the spike that sits past row 50 of the long data.
    assert sum(1 for line in lines if line.startswith("| 20")) == 22
    assert "| 2025-11-01 | 15,008 | 12,006 | 9,005 | 6,003 | 3,002 |" in lines
    assert "T00:00:00" not in text


def test_too_many_points_become_per_series_summaries() -> None:
    kinds, data = _monthly()
    text = _digest(
        ChartIntent(mark="line", x="month", y="revenue", color="category"), kinds, data, max_rows=10
    )
    assert "| month |" not in text
    electronics = next(line for line in text.splitlines() if line.startswith("- Electronics"))
    assert "22 points" in electronics
    assert "max 15,008 at 2025-11-01" in electronics
    assert "first 5,003 at 2025-01-01" in electronics
    assert "last 5,003 at 2026-10-01" in electronics


def test_bar_chart_is_sorted_like_the_chart_with_a_total() -> None:
    kinds: dict[str, ColumnKind] = {"status": "text", "n": "number"}
    data = {"status": ["refunded", "paid", "cancelled"], "n": [591, 6203, 246]}
    text = _digest(ChartIntent(mark="bar", x="status", y="n", sort="-y"), kinds, data)
    rows = [line for line in text.splitlines() if line.startswith("| ") and "---" not in line]
    assert rows == [
        "| status | n |",
        "| paid | 6,203 |",
        "| refunded | 591 |",
        "| cancelled | 246 |",
    ]
    assert "Total n: 7,040" in text


def test_many_categories_show_top_and_bottom() -> None:
    kinds: dict[str, ColumnKind] = {"product": "text", "units": "number"}
    data = {"product": [f"p{i:03d}" for i in range(200)], "units": list(range(200))}
    text = _digest(ChartIntent(mark="bar", x="product", y="units", sort="-y"), kinds, data)
    assert "| p199 | 199 |" in text  # top
    assert "| p000 | 0 |" in text  # bottom
    assert "| p100 | 100 |" not in text
    assert "170 more" in text


def test_small_budget_never_duplicates_or_hides_short_lists() -> None:
    kinds: dict[str, ColumnKind] = {"status": "text", "n": "number"}
    data = {"status": ["a", "b", "c"], "n": [1, 2, 3]}
    text = _digest(ChartIntent(mark="bar", x="status", y="n", sort="-y"), kinds, data, max_rows=1)
    assert text.count("| a | 1 |") == 1
    assert "more" not in text


def test_raw_rows_are_aggregated_as_the_chart_does() -> None:
    kinds: dict[str, ColumnKind] = {"region": "text", "amount": "number"}
    data = {"region": ["N", "S", "N", "S", "N"], "amount": [1, 2, 3, 4, 5]}
    mean = _digest(ChartIntent(mark="bar", x="region", y="amount", aggregate="mean"), kinds, data)
    assert "| N | 3 |" in mean
    assert "| S | 3 |" in mean
    count = _digest(ChartIntent(mark="bar", x="region"), kinds, data)
    assert "| region | count |" in count
    assert "| N | 3 |" in count


def test_pie_shows_shares() -> None:
    kinds: dict[str, ColumnKind] = {"channel": "text", "revenue": "number"}
    data = {"channel": ["app", "store", "web"], "revenue": [25.0, 25.0, 50.0]}
    text = _digest(ChartIntent(mark="arc", x="channel", y="revenue"), kinds, data)
    assert "| web | 50 | 50.0% |" in text


def test_heatmap_is_a_cross_table() -> None:
    kinds: dict[str, ColumnKind] = {"weekday": "text", "channel": "text", "orders": "number"}
    data = {
        "weekday": ["Mon", "Mon", "Tue", "Tue"],
        "channel": ["app", "web", "app", "web"],
        "orders": [3, 5, 2, 8],
    }
    text = _digest(ChartIntent(mark="rect", x="channel", y="weekday", color="orders"), kinds, data)
    assert "| weekday | app | web |" in text
    assert "| Tue | 2 | 8 |" in text


def test_scatter_reports_ranges_and_correlation() -> None:
    kinds: dict[str, ColumnKind] = {"price": "number", "units": "number"}
    data = {"price": [1.0, 2.0, 3.0, 4.0], "units": [8.0, 6.0, 4.0, 2.0]}
    text = _digest(ChartIntent(mark="point", x="price", y="units"), kinds, data)
    assert "4 points" in text
    assert "price from 1 to 4" in text
    assert "correlation -1.00" in text


def test_histogram_and_kpi() -> None:
    kinds: dict[str, ColumnKind] = {"amount": "number"}
    hist = _digest(ChartIntent(mark="bar", x="amount"), kinds, {"amount": [1, 2, 3, 4, 100]})
    assert "5 values" in hist
    assert "median 3" in hist
    assert "max 100" in hist
    kpi = _digest(ChartIntent(mark="text", y="amount"), kinds, {"amount": [3794560.123]})
    assert "amount: 3,794,560" in kpi


def test_format_value() -> None:
    assert format_value(91745.024) == "91,745"
    assert format_value(12.3456) == "12.35"
    assert format_value(0.012345) == "0.01235"
    assert format_value(7) == "7"
    assert format_value(None) == ""
    assert format_value("2025-01-01T00:00:00") == "2025-01-01"
    assert format_value("2025-01-01T09:30:00") == "2025-01-01T09:30:00"
    assert format_value(True) == "true"
