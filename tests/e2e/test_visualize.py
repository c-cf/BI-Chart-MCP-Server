"""End-to-end tests of the visualize tool and the chart view resource."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from mcp import Client
from mcp.client import advertise
from mcp.server.apps import APP_MIME_TYPE, EXTENSION_ID
from mcp.types import CallToolResult, TextContent, TextResourceContents

from bi_chart_mcp.server import build_server
from bi_chart_mcp.services import Services
from bi_chart_mcp.tools.chart_tools import CHART_RESOURCE_URI

pytestmark = pytest.mark.anyio

BY_STATUS = "SELECT status, sum(amount) AS revenue FROM orders GROUP BY status ORDER BY status"


@pytest.fixture
async def apps_client(services: Services) -> AsyncIterator[Client]:
    """A client that, like Claude Desktop, advertises MCP Apps support."""
    supports_apps = advertise(EXTENSION_ID, {"mimeTypes": [APP_MIME_TYPE]})
    async with Client(
        build_server(services), raise_exceptions=True, extensions=[supports_apps]
    ) as connected:
        yield connected


def _text(result: CallToolResult) -> str:
    first = result.content[0]
    assert isinstance(first, TextContent)
    return first.text


def _data(result: CallToolResult) -> dict[str, Any]:
    assert not result.is_error, _text(result)
    assert result.structured_content is not None
    return result.structured_content


async def test_chart_view_resource_is_served(client: Client) -> None:
    resources = (await client.list_resources()).resources
    view = next(r for r in resources if str(r.uri) == CHART_RESOURCE_URI)
    assert view.mime_type == APP_MIME_TYPE
    contents = (await client.read_resource(CHART_RESOURCE_URI)).contents
    (html,) = contents
    assert isinstance(html, TextResourceContents)
    assert html.text.lstrip().lower().startswith("<!doctype html>")
    assert "<script" in html.text
    assert 'src="http' not in html.text  # fully self-contained, nothing fetched


async def test_visualize_is_bound_to_the_view(client: Client) -> None:
    tools = {t.name: t for t in (await client.list_tools()).tools}
    meta = tools["visualize"].meta or {}
    assert meta["ui"]["resourceUri"] == CHART_RESOURCE_URI


async def test_visualize_from_sql_for_an_apps_host(apps_client: Client) -> None:
    result = await apps_client.call_tool("visualize", {"source": "shop", "sql": BY_STATUS})
    data = _data(result)
    assert data["chart"]["mark"] == "bar"
    assert data["chart"]["x"] == "status"
    assert data["summary"] == "Bar chart: revenue by status"
    assert data["data"]["status"] == ["paid", "refunded"]
    assert all(isinstance(v, float) for v in data["data"]["revenue"])
    assert data["spec"]["data"] == {"name": "table"}
    assert data["chart_rows"] == 2
    assert data["display_sql"].startswith("SELECT")
    text = _text(result)
    assert text.startswith("Showed the user an interactive chart")
    assert "| status | revenue |" in text


async def test_visualize_from_handle_falls_back_to_text(client: Client) -> None:
    queried = await client.call_tool("query", {"source": "shop", "sql": BY_STATUS})
    handle = _data(queried)["handle"]
    result = await client.call_tool(
        "visualize",
        {"handle": handle, "chart": {"mark": "arc", "donut": True}, "title": "Revenue mix"},
    )
    data = _data(result)
    assert data["handle"] == handle
    assert data["chart"]["mark"] == "arc"
    assert data["summary"] == "Revenue mix"
    assert data["spec"]["title"] == "Revenue mix"
    assert "cannot display interactive charts" in _text(result)


async def test_visualize_time_series_with_notes(apps_client: Client) -> None:
    result = await apps_client.call_tool(
        "visualize", {"source": "shop", "sql": "SELECT order_date, amount FROM orders"}
    )
    data = _data(result)
    assert data["chart"]["mark"] == "line"
    assert data["truncated"] is True
    assert any("25-row cap" in note for note in data["notes"])


@pytest.mark.parametrize(
    ("args", "code"),
    [
        ({}, "[invalid_arguments]"),
        ({"handle": "q_x", "sql": "SELECT 1", "source": "shop"}, "[invalid_arguments]"),
        ({"source": "shop"}, "[invalid_arguments]"),
        ({"handle": "q_missing"}, "[unknown_handle]"),
        ({"source": "shop", "sql": BY_STATUS, "chart": {"x": "nope"}}, "[chart_error]"),
        ({"source": "shop", "sql": "DROP TABLE orders"}, "[not_a_query]"),
    ],
)
async def test_visualize_errors(client: Client, args: dict[str, Any], code: str) -> None:
    result = await client.call_tool("visualize", args)
    assert result.is_error
    assert code in _text(result)
