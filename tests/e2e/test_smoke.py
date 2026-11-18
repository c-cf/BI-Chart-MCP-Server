"""Smoke tests: the server starts and answers over in-memory and real stdio transports."""

from __future__ import annotations

import sys

import pytest
from mcp import Client, StdioServerParameters
from mcp.types import TextContent

from bi_chart_mcp import __version__
from bi_chart_mcp.server import build_server

pytestmark = pytest.mark.anyio


def _text(result_content: list[object]) -> str:
    first = result_content[0]
    assert isinstance(first, TextContent)
    return first.text


async def test_ping_in_memory() -> None:
    async with Client(build_server(), raise_exceptions=True) as client:
        result = await client.call_tool("ping", {})
    assert not result.is_error
    assert _text(list(result.content)) == f"bi-chart-mcp {__version__}"


async def test_ping_is_marked_read_only() -> None:
    async with Client(build_server(), raise_exceptions=True) as client:
        tools = await client.list_tools()
    ping = next(t for t in tools.tools if t.name == "ping")
    assert ping.annotations is not None
    assert ping.annotations.read_only_hint is True


async def test_ping_over_stdio_subprocess() -> None:
    """Spawns the real CLI: proves stdio plus worker threads work on this OS (incl. Windows)."""
    params = StdioServerParameters(command=sys.executable, args=["-m", "bi_chart_mcp"])
    async with Client(params) as client:
        result = await client.call_tool("ping", {})
    assert _text(list(result.content)) == f"bi-chart-mcp {__version__}"
