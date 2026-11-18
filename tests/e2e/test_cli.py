"""The real CLI over stdio: proves startup, config loading and shutdown on this OS."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from mcp import Client, StdioServerParameters

pytestmark = pytest.mark.anyio


async def test_query_over_stdio_subprocess(config_file: Path) -> None:
    params = StdioServerParameters(
        command=sys.executable, args=["-m", "bi_chart_mcp", "--config", str(config_file)]
    )
    async with Client(params) as client:
        result = await client.call_tool(
            "query", {"source": "shop", "sql": "SELECT count(*) AS n FROM orders"}
        )
    assert not result.is_error
    assert result.structured_content is not None
    assert result.structured_content["rows"] == [[40]]


def test_config_error_exits_with_status_2(tmp_path: Path) -> None:
    completed = subprocess.run(  # noqa: S603 - fixed argv, test only
        [sys.executable, "-m", "bi_chart_mcp", "--config", str(tmp_path / "missing.yml")],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert completed.returncode == 2
    assert "Config file not found" in completed.stderr
