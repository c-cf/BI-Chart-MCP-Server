"""Tests for value conversion, column classification and the markdown table."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

import pytest

from bi_chart_mcp.query.table_text import markdown_table
from bi_chart_mcp.query.values import column_kind, row_to_json, to_json_value


@pytest.mark.parametrize(
    ("db_type", "kind"),
    [
        ("int4", "number"),
        ("int8", "number"),
        ("bigint", "number"),
        ("INTEGER", "number"),
        ("HUGEINT", "number"),
        ("numeric(10,2)", "number"),
        ("DECIMAL(21,1)", "number"),
        ("double precision", "number"),
        ("float8", "number"),
        ("money", "number"),
        ("date", "temporal"),
        ("timestamptz", "temporal"),
        ("TIMESTAMP WITH TIME ZONE", "temporal"),
        ("interval", "temporal"),
        ("text", "text"),
        ("VARCHAR", "text"),
        ("character varying(20)", "text"),
        ("uuid", "text"),
        ("bool", "boolean"),
        ("BOOLEAN", "boolean"),
        ("jsonb", "other"),
        ("integer[]", "other"),
        ("STRUCT(k INTEGER)", "other"),
        ("bytea", "other"),
    ],
)
def test_column_kind(db_type: str, kind: str) -> None:
    assert column_kind(db_type) == kind


def test_scalar_conversions() -> None:
    assert to_json_value(None, 10) is None
    assert to_json_value(True, 10) is True
    assert to_json_value(42, 10) == 42
    assert to_json_value(2**60, 10) == str(2**60)
    assert to_json_value(1.5, 10) == 1.5
    assert to_json_value(float("nan"), 10) is None
    assert to_json_value(Decimal("12.34"), 10) == 12.34
    assert to_json_value(Decimal("1e20"), 10) == "1E+20"
    assert to_json_value(Decimal("NaN"), 10) is None
    assert to_json_value(date(2026, 1, 2), 10) == "2026-01-02"
    assert to_json_value(datetime(2026, 1, 2, 3, 4, tzinfo=UTC), 40) == "2026-01-02T03:04:00+00:00"
    assert to_json_value(time(3, 4), 10) == "03:04:00"
    assert to_json_value(timedelta(days=1, hours=2), 20) == "1 day, 2:00:00"
    assert to_json_value(uuid.UUID(int=1), 40) == "00000000-0000-0000-0000-000000000001"


def test_strings_are_truncated() -> None:
    assert to_json_value("abcdefghij", 5) == "abcd…"
    assert to_json_value("abc", 5) == "abc"


def test_binary_values_become_previews() -> None:
    preview = to_json_value(bytes(range(20)), 100)
    assert isinstance(preview, str)
    assert preview.startswith("<20 bytes, base64 ")
    assert preview.endswith("…>")
    assert to_json_value(memoryview(b"ab"), 100) == "<2 bytes, base64 YWI=>"


def test_nested_values_are_converted_recursively() -> None:
    value = {"a": [Decimal("1.5"), {"b": date(2026, 1, 1)}], 3: None}
    assert to_json_value(value, 10) == {"a": [1.5, {"b": "2026-01-01"}], "3": None}
    assert row_to_json((1, "x", (2, 3)), 10) == [1, "x", [2, 3]]
    json.dumps(to_json_value(value, 10))  # serializable


def test_markdown_table_escapes_cells() -> None:
    table = markdown_table(["a", "b|c"], [["x|y", "line1\nline2"], [None, 3], [{"k": 1}, True]])
    lines = table.splitlines()
    assert lines[0] == "| a | b\\|c |"
    assert lines[1] == "|---|---|"
    assert lines[2] == "| x\\|y | line1 line2 |"
    assert lines[3] == "|  | 3 |"
    assert lines[4] == '| {"k": 1} | true |'
