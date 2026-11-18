"""Tests for JSON logging and the CLI argument parsing."""

from __future__ import annotations

import json
import logging

import pytest

from bi_chart_mcp import __main__ as cli
from bi_chart_mcp.logs import LOGGER_NAME, JsonFormatter, configure_logging, get_logger


def test_json_formatter_includes_extras_and_exceptions() -> None:
    record = logging.LogRecord("x", logging.WARNING, __file__, 1, "hello %s", ("world",), None)
    record.source = "warehouse"
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        record.exc_info = sys.exc_info()
    payload = json.loads(JsonFormatter().format(record))
    assert payload["msg"] == "hello world"
    assert payload["level"] == "WARNING"
    assert payload["source"] == "warehouse"
    assert "RuntimeError: boom" in payload["exc"]


def test_configure_logging_writes_json_to_stderr(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("DEBUG")
    get_logger("test").info("ready", extra={"rows": 3})
    captured = capsys.readouterr()
    assert captured.out == ""
    line = json.loads(captured.err.strip().splitlines()[-1])
    assert line["logger"] == f"{LOGGER_NAME}.test"
    assert line["rows"] == 3


def test_cli_version_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as info:
        cli.main(["--version"])
    assert info.value.code == 0
    assert "bi-chart-mcp" in capsys.readouterr().out
