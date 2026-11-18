"""A compact text version of a chart, so the model sees everything the user sees.

The chart can draw hundreds of rows; the model only needs their shape. Long series are
pivoted (one row per x value, one column per series), and when even that is too big,
each series is summarised (total, min, max, first, last). Values are aggregated the way
the chart aggregates them.
"""

from __future__ import annotations

import statistics
from collections.abc import Callable, Mapping, Sequence
from typing import Final

from bi_chart_mcp.charts.intent import Aggregate, ChartIntent
from bi_chart_mcp.query.formatting import format_value
from bi_chart_mcp.query.table_text import markdown_table
from bi_chart_mcp.query.values import ColumnKind, JsonValue

DEFAULT_MAX_ROWS: Final = 60
_MAX_SERIES: Final = 8
_MAX_CROSS_COLUMNS: Final = 12
_TOP: Final = 15
_ADDITIVE: Final = frozenset({None, "sum", "count"})

Data = Mapping[str, Sequence[JsonValue]]
Kinds = Mapping[str, ColumnKind]
Number = int | float
Key = tuple[str, str]


# ---------------------------------------------------------------- aggregation


def _numbers(values: Sequence[JsonValue]) -> list[Number]:
    return [v for v in values if isinstance(v, int | float) and not isinstance(v, bool)]


def _aggregate(values: Sequence[JsonValue], how: Aggregate | None) -> JsonValue:
    if how == "count":
        return len(values)
    if how == "distinct":
        return len({str(v) for v in values if v is not None})
    numbers = _numbers(values)
    if not numbers:
        return None
    functions: dict[str, Callable[[list[Number]], Number]] = {
        "mean": statistics.fmean,
        "median": statistics.median,
        "min": min,
        "max": max,
    }
    if how in functions:
        return functions[how](numbers)
    return sum(numbers)


def _group(keys: Sequence[Key], values: Sequence[JsonValue]) -> dict[Key, list[JsonValue]]:
    groups: dict[Key, list[JsonValue]] = {}
    for key, value in zip(keys, values, strict=True):
        groups.setdefault(key, []).append(value)
    return groups


def _ordered(values: Sequence[JsonValue], kind: ColumnKind | None) -> list[str]:
    labels = list(dict.fromkeys(format_value(v) for v in values))
    return sorted(labels) if kind == "temporal" else labels


# ---------------------------------------------------------------- entry point


def chart_digest(
    intent: ChartIntent, kinds: Kinds, data: Data, *, max_rows: int = DEFAULT_MAX_ROWS
) -> str:
    """Everything the chart shows, as compact text. `intent` must be completed."""
    if intent.mark == "text":
        return _kpi(intent, data)
    if intent.mark == "point":
        return _scatter(intent, data, max_rows)
    if intent.mark == "rect":
        return _cross_table(intent, kinds, data, max_rows)
    if intent.mark == "bar" and intent.y is None and kinds.get(intent.x or "") == "number":
        return _distribution(intent.x or "", data)
    return _grouped(intent, kinds, data, max_rows)


def _measure(intent: ChartIntent, field: str | None) -> tuple[str, Aggregate | None]:
    how: Aggregate | None = intent.aggregate if field else "count"
    if field is None:
        return "count", how
    return (f"{intent.aggregate}({field})" if intent.aggregate else field), how


def _column(data: Data, field: str | None) -> Sequence[JsonValue]:
    if field is None:
        row_count = max((len(column) for column in data.values()), default=0)
        return [1] * row_count  # one per row, so "count" aggregates count rows
    return data.get(field, [])


# ---------------------------------------------------------------- shapes


def _kpi(intent: ChartIntent, data: Data) -> str:
    label, how = _measure(intent, intent.y)
    value = _aggregate(_column(data, intent.y), how or "sum")
    return f"{label}: {format_value(value)}"


def _distribution(field: str, data: Data) -> str:
    numbers = sorted(_numbers(data.get(field, [])))
    if not numbers:
        return f"No numeric values of {field}."
    quartiles = statistics.quantiles(numbers, n=4) if len(numbers) > 1 else numbers * 3
    parts = [
        f"min {format_value(numbers[0])}",
        f"p25 {format_value(quartiles[0])}",
        f"median {format_value(statistics.median(numbers))}",
        f"p75 {format_value(quartiles[2])}",
        f"max {format_value(numbers[-1])}",
        f"mean {format_value(statistics.fmean(numbers))}",
    ]
    return f"Distribution of {len(numbers)} values of {field}: " + ", ".join(parts) + "."


def _scatter(intent: ChartIntent, data: Data, max_rows: int) -> str:
    x, y = intent.x or "", intent.y or ""
    pairs = [
        (a, b)
        for a, b in zip(data.get(x, []), data.get(y, []), strict=False)
        if isinstance(a, int | float) and isinstance(b, int | float)
    ]
    lines = [f"{len(pairs)} points."]
    for name, values in ((x, [p[0] for p in pairs]), (y, [p[1] for p in pairs])):
        if values:
            lines.append(f"{name} from {format_value(min(values))} to {format_value(max(values))}.")
    correlation = _correlation(pairs)
    if correlation is not None:
        lines.append(f"correlation {correlation:.2f} between {x} and {y} (Pearson).")
    if len(pairs) <= max_rows:
        columns = [c for c in (x, y, intent.color, intent.size) if c]
        rows = [[format_value(data[c][i]) for c in columns] for i in range(len(data[x]))]
        lines += ["", markdown_table(columns, rows)]
    return "\n".join(lines).replace("Pearson correlation", "correlation")


def _correlation(pairs: Sequence[tuple[Number, Number]]) -> float | None:
    if len(pairs) < 3:
        return None
    try:
        return statistics.correlation([p[0] for p in pairs], [p[1] for p in pairs])
    except statistics.StatisticsError:  # constant input
        return None


def _cross_table(intent: ChartIntent, kinds: Kinds, data: Data, max_rows: int) -> str:
    x, y = intent.x or "", intent.y or ""
    label, how = _measure(intent, intent.color)
    keys = [
        (format_value(a), format_value(b))
        for a, b in zip(data.get(x, []), data.get(y, []), strict=True)
    ]
    groups = _group(keys, _column(data, intent.color))
    columns, rows = _ordered(data.get(x, []), kinds.get(x)), _ordered(data.get(y, []), kinds.get(y))
    if len(rows) <= max_rows and len(columns) <= _MAX_CROSS_COLUMNS:
        table = [
            [r, *(format_value(_aggregate(groups.get((c, r), []), how)) for c in columns)]
            for r in rows
        ]
        return f"{label} by {y} (rows) and {x} (columns):\n\n" + markdown_table(
            [y, *columns], table
        )
    top = sorted(groups.items(), key=lambda kv: _sort_key(_aggregate(kv[1], how)), reverse=True)
    lines = [f"Highest {label} cells ({x}, {y}) of {len(groups)}:"]
    lines += [f"- {k[0]}, {k[1]}: {format_value(_aggregate(v, how))}" for k, v in top[:_TOP]]
    return "\n".join(lines)


def _sort_key(value: JsonValue) -> float:
    return float(value) if isinstance(value, int | float) else float("-inf")


def _grouped(intent: ChartIntent, kinds: Kinds, data: Data, max_rows: int) -> str:
    x = intent.x or ""
    series = intent.color if intent.mark in ("bar", "line", "area") else None
    label, how = _measure(intent, intent.y)
    xs = [format_value(v) for v in data.get(x, [])]
    ss = [format_value(v) for v in data.get(series, [])] if series else [""] * len(xs)
    groups = _group(list(zip(xs, ss, strict=True)), _column(data, intent.y))
    x_order = _ordered(data.get(x, []), kinds.get(x))
    if series:
        series_order = list(dict.fromkeys(ss))
        if len(series_order) <= _MAX_SERIES and len(x_order) <= max_rows:
            return _pivot(x, label, how, groups, x_order, series_order)
        return _series_summaries(label, how, groups, x_order, series_order)
    totals = [(key, _aggregate(groups[(key, "")], how)) for key in x_order]
    return _single_series(intent, x, label, how, totals, kinds.get(x), max_rows)


def _pivot(
    x: str,
    label: str,
    how: Aggregate | None,
    groups: Mapping[tuple[str, str], list[JsonValue]],
    x_order: Sequence[str],
    series_order: Sequence[str],
) -> str:
    rows = [[key, *(_cell(groups, (key, s), how) for s in series_order)] for key in x_order]
    text = f"{label} by {x} (one column per series):\n\n" + markdown_table([x, *series_order], rows)
    if how in _ADDITIVE:
        totals = [
            f"{s} {format_value(_aggregate(_series_values(groups, s), how))}" for s in series_order
        ]
        text += "\nTotals: " + "; ".join(totals)
    return text


def _cell(
    groups: Mapping[tuple[str, str], list[JsonValue]], key: tuple[str, str], how: Aggregate | None
) -> str:
    """A pivot cell; blank (not zero) where the series has no point."""
    return format_value(_aggregate(groups[key], how)) if key in groups else ""


def _series_values(
    groups: Mapping[tuple[str, str], list[JsonValue]], series: str
) -> list[JsonValue]:
    return [v for (_, s), values in groups.items() if s == series for v in values]


def _series_summaries(
    label: str,
    how: Aggregate | None,
    groups: Mapping[tuple[str, str], list[JsonValue]],
    x_order: Sequence[str],
    series_order: Sequence[str],
) -> str:
    lines = [
        f"{label}, summarised per series ({len(x_order)} x values, {len(series_order)} series):"
    ]
    for s in series_order:
        points = [(key, _aggregate(groups[(key, s)], how)) for key in x_order if (key, s) in groups]
        lines.append(f"- {s or label}: {_describe_points(points, how)}")
    return "\n".join(lines)


def _describe_points(points: Sequence[tuple[str, JsonValue]], how: Aggregate | None) -> str:
    numeric = [(k, v) for k, v in points if isinstance(v, int | float)]
    if not numeric:
        return f"{len(points)} points"
    low = min(numeric, key=lambda p: p[1])
    high = max(numeric, key=lambda p: p[1])
    parts = [f"{len(points)} points"]
    if how in _ADDITIVE:
        parts.append(f"total {format_value(sum(v for _, v in numeric))}")
    parts += [
        f"min {format_value(low[1])} at {low[0]}",
        f"max {format_value(high[1])} at {high[0]}",
        f"first {format_value(numeric[0][1])} at {numeric[0][0]}",
        f"last {format_value(numeric[-1][1])} at {numeric[-1][0]}",
    ]
    return "; ".join(parts)


def _single_series(
    intent: ChartIntent,
    x: str,
    label: str,
    how: Aggregate | None,
    totals: list[tuple[str, JsonValue]],
    x_kind: ColumnKind | None,
    max_rows: int,
) -> str:
    ordered = _sort_like_chart(totals, intent.sort)
    if len(ordered) > max_rows and x_kind in ("temporal", "number"):
        return f"{label} by {x}: {_describe_points(ordered, how)}."
    grand = _aggregate([v for _, v in ordered], "sum") if how in _ADDITIVE else None
    headers = [x, label]
    rows = [[key, format_value(value)] for key, value in ordered]
    if intent.mark == "arc" and isinstance(grand, int | float) and grand:
        headers.append("share")
        rows = [
            [*row, f"{100 * value / grand:.1f}%" if isinstance(value, int | float) else ""]
            for row, (_, value) in zip(rows, ordered, strict=True)
        ]
    if len(rows) > max(max_rows, 2 * _TOP):
        hidden = len(rows) - 2 * _TOP
        rows = [*rows[:_TOP], [f"… {hidden} more …", *([""] * (len(headers) - 1))], *rows[-_TOP:]]
    text = markdown_table(headers, rows)
    if grand is not None and intent.mark in ("bar", "arc"):
        text += f"\nTotal {label}: {format_value(grand)}"
    return text


def _sort_like_chart(
    totals: list[tuple[str, JsonValue]], sort: str | None
) -> list[tuple[str, JsonValue]]:
    match sort:
        case "-y":
            return sorted(totals, key=lambda kv: _sort_key(kv[1]), reverse=True)
        case "y":
            return sorted(totals, key=lambda kv: _sort_key(kv[1]))
        case "x":
            return sorted(totals, key=lambda kv: kv[0])
        case "-x":
            return sorted(totals, key=lambda kv: kv[0], reverse=True)
        case _:
            return totals
