"""Command-line entry point: `bi-chart-mcp` / `python -m bi_chart_mcp`."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

import anyio

from bi_chart_mcp import __version__
from bi_chart_mcp.errors import ConfigError
from bi_chart_mcp.logs import configure_logging, get_logger
from bi_chart_mcp.server import build_server
from bi_chart_mcp.services import Services
from bi_chart_mcp.settings import CONFIG_ENV_VAR, load_config, resolve_config_path

_log = get_logger("main")

EXIT_CONFIG_ERROR = 2


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bi-chart-mcp",
        description="MCP server for read-only BI over PostgreSQL and DuckDB.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--config",
        help=f"Path to the YAML config (default: ${CONFIG_ENV_VAR} or ./bi-chart-mcp.yml).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level for the JSON logs written to stderr.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Load the config, open the sources and serve over stdio."""
    args = _parse_args(argv)
    configure_logging(args.log_level)
    try:
        config = load_config(resolve_config_path(args.config))
        services = Services.from_config(config)
    except ConfigError as exc:
        _log.error("configuration error", extra={"error": exc.message})
        print(f"bi-chart-mcp: {exc.message}", file=sys.stderr)
        sys.exit(EXIT_CONFIG_ERROR)
    try:
        server = build_server(services)
        _log.info(
            "starting",
            extra={
                "version": __version__,
                "transport": "stdio",
                "sources": services.registry.names(),
            },
        )
        anyio.run(server.run_stdio_async)
    finally:
        services.close()


if __name__ == "__main__":  # pragma: no cover
    main()
