"""Fill in the parts of a ChartIntent the model left out, from the result's shape."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from bi_chart_mcp.charts.intent import ChartError, ChartIntent, Mark
from bi_chart_mcp.query.values import ColumnKind, JsonValue

_MAX_SERIES: Final = 10  # more colours than this stop being readable
_HORIZONTAL_CATEGORIES: Final = 8
_HORIZONTAL_LABEL_CHARS: Final = 12
# JSON, arrays and structs ("other") are never picked automatically.
_CATEGORICAL: Final = frozenset({"text", "boolean"})


@dataclass(frozen=True, slots=True)
class FieldProfile:
    name: str
    kind: ColumnKind
    distinct: int  # distinct non-null values in the chart data
    max_label_chars: int  # longest value as text, for label-length decisions

    @property
    def categorical(self) -> bool:
        return self.kind in _CATEGORICAL


def profile_fields(
    columns: Sequence[tuple[str, ColumnKind]], data: Mapping[str, Sequence[JsonValue]]
) -> tuple[FieldProfile, ...]:
    profiles: list[FieldProfile] = []
    for name, kind in columns:
        values = [v for v in data.get(name, ()) if v is not None]
        labels = [v if isinstance(v, str) else str(v) for v in values]
        profiles.append(
            FieldProfile(
                name=name,
                kind=kind,
                distinct=len(set(labels)),
                max_label_chars=max((len(label) for label in labels), default=0),
            )
        )
    return tuple(profiles)


def complete_intent(
    intent: ChartIntent, fields: Sequence[FieldProfile], row_count: int
) -> ChartIntent:
    """Return an intent with mark, x and y (where the mark needs them) decided."""
    by_name = {f.name: f for f in fields}
    _check_fields_exist(intent, by_name)
    used = {n for n in (intent.x, intent.y, intent.color, intent.size) if n}
    mark = intent.mark or _choose_mark(intent, fields, row_count, by_name)
    x, y = _choose_axes(mark, intent, fields, used)
    color = intent.color
    if color is None and mark in ("bar", "line", "area", "point"):
        color = _series_field(fields, used | {n for n in (x, y) if n})
    completed = intent.model_copy(update={"mark": mark, "x": x, "y": y, "color": color})
    return _with_layout_defaults(completed, by_name)


def _check_fields_exist(intent: ChartIntent, by_name: Mapping[str, FieldProfile]) -> None:
    for role in ("x", "y", "color", "size"):
        name = getattr(intent, role)
        if name is not None and name not in by_name:
            raise ChartError(
                f"Column {name!r} (for {role}) is not in the result. "
                f"Available columns: {', '.join(by_name)}."
            )


def _choose_mark(
    intent: ChartIntent,
    fields: Sequence[FieldProfile],
    row_count: int,
    by_name: Mapping[str, FieldProfile],
) -> Mark:
    x_kind = by_name[intent.x].kind if intent.x else None
    numeric = [f for f in fields if f.kind == "number"]
    temporal = [f for f in fields if f.kind == "temporal"]
    categorical = [f for f in fields if f.categorical]
    if row_count == 1 and len(numeric) == 1 and not intent.x:
        return "text"
    if x_kind == "temporal" or (x_kind is None and temporal and numeric):
        return "line"
    if x_kind in _CATEGORICAL:
        return "bar"
    if x_kind is None and categorical:
        # One row per category means an aggregated result: compare with bars.
        # Otherwise, with two measures, show their relationship as a scatter.
        aggregated = any(f.distinct == row_count for f in categorical)
        return "point" if len(numeric) >= 2 and not aggregated else "bar"
    if len(numeric) >= 2:
        return "point"
    if numeric:
        return "bar"  # histogram of the single numeric column
    raise ChartError("This result has no columns that can be charted; show it as a table.")


def _first(
    fields: Sequence[FieldProfile], kinds: Sequence[str], exclude: set[str]
) -> FieldProfile | None:
    for kind in kinds:
        for f in fields:
            matches = f.categorical if kind == "categorical" else f.kind == kind
            if matches and f.name not in exclude:
                return f
    return None


_X_PREFERENCE: Final[dict[Mark, tuple[str, ...]]] = {
    "line": ("temporal", "number", "categorical"),
    "area": ("temporal", "number", "categorical"),
    "bar": ("categorical", "temporal", "number"),
    "arc": ("categorical", "temporal"),
    "rect": ("categorical", "temporal", "number"),
    "point": ("number", "temporal", "categorical"),
    "text": (),
}


def _choose_axes(
    mark: Mark, intent: ChartIntent, fields: Sequence[FieldProfile], used: set[str]
) -> tuple[str | None, str | None]:
    x = intent.x
    if x is None and mark != "text":
        found = _first(fields, _X_PREFERENCE[mark], used)
        x = found.name if found else None
        if x is None:
            raise ChartError(f"No column fits the x axis of a {mark} chart.")
    taken = used | {x} if x else set(used)
    y = intent.y
    if y is None:
        if mark == "rect":
            found = _first(fields, ("categorical", "temporal"), taken)
        else:
            found = _first(fields, ("number",), taken)
        y = found.name if found else None
    if y is None and mark in ("line", "area", "point", "text", "rect"):
        raise ChartError(f"No numeric column fits the y axis of a {mark} chart.")
    return x, y


def _series_field(fields: Sequence[FieldProfile], exclude: set[str]) -> str | None:
    for f in fields:
        if f.categorical and f.name not in exclude and 1 < f.distinct <= _MAX_SERIES:
            return f.name
    return None


def _with_layout_defaults(intent: ChartIntent, by_name: Mapping[str, FieldProfile]) -> ChartIntent:
    updates: dict[str, object] = {}
    x = by_name.get(intent.x) if intent.x else None
    if intent.mark == "bar" and x is not None and x.categorical:
        if intent.horizontal is None:
            updates["horizontal"] = (
                x.distinct > _HORIZONTAL_CATEGORIES or x.max_label_chars > _HORIZONTAL_LABEL_CHARS
            )
        if intent.sort is None:
            updates["sort"] = "-y"
    if intent.color and intent.mark in ("bar", "area") and intent.stack is None:
        updates["stack"] = "stacked"
    return intent.model_copy(update=updates) if updates else intent
