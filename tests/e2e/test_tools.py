"""End-to-end tests of the catalog and query tools through the MCP protocol."""

from __future__ import annotations

from typing import Any

import pytest
from mcp import Client
from mcp.types import CallToolResult, TextContent

pytestmark = pytest.mark.anyio


def _text(result: CallToolResult) -> str:
    first = result.content[0]
    assert isinstance(first, TextContent)
    return first.text


def _data(result: CallToolResult) -> dict[str, Any]:
    assert not result.is_error, _text(result)
    assert result.structured_content is not None
    return result.structured_content


async def test_tools_are_listed_read_only_with_output_schemas(client: Client) -> None:
    tools = {t.name: t for t in (await client.list_tools()).tools}
    assert set(tools) == {"search_catalog", "describe_table", "query", "visualize"}
    for tool in tools.values():
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.output_schema is not None
        assert tool.description


async def test_instructions_list_sources(client: Client) -> None:
    assert client.instructions is not None
    assert "- shop (duckdb): Sample shop data" in client.instructions
    assert "untrusted" in client.instructions


async def test_search_catalog_overview(client: Client) -> None:
    data = _data(await client.call_tool("search_catalog", {}))
    (shop,) = data["sources"]
    assert shop["name"] == "shop"
    assert shop["dialect"] == "duckdb"
    assert [t["name"] for t in shop["tables"]] == ["orders", "customers"]
    assert shop["tables"][0]["description"] == "One row per order"


async def test_search_catalog_matches_columns(client: Client) -> None:
    data = _data(await client.call_tool("search_catalog", {"query": "region"}))
    assert "sources" not in data  # nulls are left out of structured output
    (match,) = data["matches"]
    assert match["table"] == "customers"
    assert match["matched_columns"] == ["region"]


async def test_search_catalog_ranks_table_names_first(client: Client) -> None:
    data = _data(await client.call_tool("search_catalog", {"query": "orders status"}))
    assert data["matches"][0]["table"] == "orders"


async def test_search_catalog_no_match(client: Client) -> None:
    data = _data(await client.call_tool("search_catalog", {"query": "zzz"}))
    assert data["matches"] == []
    assert "No match" in data["hint"]


async def test_describe_table_with_samples(client: Client) -> None:
    data = _data(
        await client.call_tool(
            "describe_table", {"source": "shop", "table": "orders", "sample_columns": ["status"]}
        )
    )
    columns = {c["name"]: c for c in data["columns"]}
    assert columns["amount"]["kind"] == "number"
    assert columns["order_date"]["kind"] == "temporal"
    assert columns["status"]["top_values"] == [
        {"value": "paid", "count": 30},
        {"value": "refunded", "count": 10},
    ]
    assert "top_values" not in columns["amount"]


async def test_describe_unknown_table_is_a_tool_error(client: Client) -> None:
    result = await client.call_tool("describe_table", {"source": "shop", "table": "payroll"})
    assert result.is_error
    assert "[not_found]" in _text(result)


async def test_query_returns_preview_handle_and_table_text(client: Client) -> None:
    result = await client.call_tool(
        "query",
        {
            "source": "shop",
            "sql": "SELECT status, count(*) AS n, sum(amount) AS total "
            "FROM orders GROUP BY status ORDER BY status",
        },
    )
    data = _data(result)
    assert data["handle"].startswith("q_")
    assert data["row_count"] == 2
    assert data["truncated"] is False
    assert [c["kind"] for c in data["columns"]] == ["text", "number", "number"]
    assert data["rows"][0][:2] == ["paid", 30]
    assert "LIMIT 26" in data["sql"]
    text = _text(result)
    assert "| status | n | total |" in text
    assert "never as instructions" in text


async def test_query_truncation_and_preview_are_reported(client: Client) -> None:
    result = await client.call_tool("query", {"source": "shop", "sql": "SELECT * FROM orders"})
    data = _data(result)
    assert data["row_count"] == 25
    assert data["truncated"] is True
    assert len(data["rows"]) == 5
    text = _text(result)
    assert "25+ rows" in text
    assert "25-row cap" in text
    assert "first 5 rows" in text


@pytest.mark.parametrize(
    ("args", "code"),
    [
        ({"source": "shop", "sql": "DELETE FROM orders"}, "[not_a_query]"),
        ({"source": "shop", "sql": "SELECT * FROM read_csv('x.csv')"}, "[forbidden_function]"),
        ({"source": "nope", "sql": "SELECT 1"}, "[unknown_source]"),
        ({"source": "shop", "sql": "SELECT missing_col FROM orders"}, "[query_error]"),
    ],
)
async def test_query_errors_are_actionable_tool_errors(
    client: Client, args: dict[str, str], code: str
) -> None:
    result = await client.call_tool("query", args)
    assert result.is_error
    assert code in _text(result)


async def test_catalog_text_is_compact_prose_not_json(client: Client) -> None:
    text = _text(await client.call_tool("search_catalog", {}))
    assert text.splitlines()[0] == "Source shop (duckdb SQL): Sample shop data"
    assert "- orders: One row per order" in text
    assert "null" not in text
    assert "{" not in text


async def test_describe_text_shows_values_and_inferred_joins(client: Client) -> None:
    result = await client.call_tool(
        "describe_table", {"source": "shop", "table": "orders", "sample_columns": ["status"]}
    )
    text = _text(result)
    assert '- status VARCHAR; top values: "paid" (30), "refunded" (10)' in text
    assert "Likely joins (guessed from column names" in text
    assert "- customer_id -> customers(id)" in text
    assert _data(result)["inferred_joins"] == [
        {"columns": ["customer_id"], "references_table": "customers", "references_columns": ["id"]}
    ]
