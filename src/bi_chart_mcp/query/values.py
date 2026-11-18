"""Turn database values into JSON-safe values, and classify column types."""

from __future__ import annotations

import base64
import math
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Final, Literal, cast

ColumnKind = Literal["number", "temporal", "text", "boolean", "other"]

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]

# Integers beyond this lose precision in JavaScript, so they are sent as strings.
_MAX_SAFE_INT: Final = 2**53 - 1
_BINARY_PREVIEW_BYTES: Final = 16
_DECIMAL_AS_FLOAT_LIMIT: Final = Decimal("1e15")

_NUMBER = re.compile(
    r"^(small|big)?int|^int[248]?$|^integer|^(u)?(tiny|small|big|huge)?int|^numeric|^decimal"
    r"|^real|^double|^float|^money|^serial|^bigserial|^smallserial|^oid$|^hugeint|^ubigint"
)
_TEMPORAL = re.compile(r"^date|^time|^timestamp|^interval")
_BOOLEAN = re.compile(r"^bool")
_TEXT = re.compile(r"^(text|varchar|char|character|bpchar|name|citext|uuid|string|enum)")


def column_kind(db_type: str) -> ColumnKind:
    """Classify a database type name (PostgreSQL or DuckDB spelling)."""
    lowered = db_type.strip().lower()
    if lowered.endswith("[]") or lowered.startswith(("struct", "map", "list", "json", "array")):
        return "other"
    if _BOOLEAN.match(lowered):
        return "boolean"
    if _TEMPORAL.match(lowered):
        return "temporal"
    if _NUMBER.match(lowered):
        return "number"
    if _TEXT.match(lowered):
        return "text"
    return "other"


def to_json_value(value: object, max_chars: int) -> JsonValue:
    """Convert one database value to something `json.dumps` and JavaScript handle exactly."""
    result: JsonValue
    match value:
        case None | bool():
            result = value
        case int():
            result = value if abs(value) <= _MAX_SAFE_INT else str(value)
        case float():
            result = value if math.isfinite(value) else None
        case Decimal():
            result = _decimal(value)
        case str():
            result = _truncate(value, max_chars)
        case datetime() | date() | time():
            result = value.isoformat()
        case timedelta():
            result = _truncate(str(value), max_chars)
        case bytes() | bytearray():
            result = _binary_preview(bytes(value))
        case memoryview():
            result = _binary_preview(cast("memoryview[int]", value).tobytes())
        case Mapping():
            items = cast(Mapping[object, object], value).items()
            result = {str(k): to_json_value(v, max_chars) for k, v in items}
        case list() | tuple():
            result = [to_json_value(v, max_chars) for v in cast(Sequence[object], value)]
        case _:
            result = _truncate(str(value), max_chars)
    return result


def row_to_json(row: Sequence[object], max_chars: int) -> list[JsonValue]:
    return [to_json_value(value, max_chars) for value in row]


def _decimal(value: Decimal) -> float | str | None:
    if not value.is_finite():
        return None
    # Below 1e15 a float keeps every digit a chart or a person cares about.
    return float(value) if abs(value) < _DECIMAL_AS_FLOAT_LIMIT else str(value)


def _truncate(text: str, max_chars: int) -> str:
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def _binary_preview(data: bytes) -> str:
    head = base64.b64encode(data[:_BINARY_PREVIEW_BYTES]).decode("ascii")
    return f"<{len(data)} bytes, base64 {head}{'…' if len(data) > _BINARY_PREVIEW_BYTES else ''}>"
