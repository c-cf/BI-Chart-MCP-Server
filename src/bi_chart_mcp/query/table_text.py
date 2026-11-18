"""Render result rows as a compact markdown table for the model."""

from __future__ import annotations

import json
from collections.abc import Sequence

from bi_chart_mcp.query.values import JsonValue


def _cell(value: JsonValue) -> str:
    if value is None:
        return ""
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return text.replace("\\", "\\\\").replace("|", "\\|").replace("\r", " ").replace("\n", " ")


def markdown_table(headers: Sequence[str], rows: Sequence[Sequence[JsonValue]]) -> str:
    """A GitHub-flavoured markdown table; `|` and newlines in cells are escaped."""
    head = "| " + " | ".join(_cell(h) for h in headers) + " |"
    rule = "|" + "|".join("---" for _ in headers) + "|"
    body = ["| " + " | ".join(_cell(v) for v in row) + " |" for row in rows]
    return "\n".join([head, rule, *body])
