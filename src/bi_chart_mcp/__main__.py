"""Command-line entry point: `bi-chart-mcp` / `python -m bi_chart_mcp`."""

from __future__ import annotations

import argparse
from collections.abc import Sequence

import anyio

from bi_chart_mcp import __version__
from bi_chart_mcp.logs import configure_logging, get_logger
from bi_chart_mcp.server import build_server

_log = get_logger("main")


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="bi-chart-mcp", description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level for the JSON logs written to stderr.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    """Run the server over stdio."""
    args = _parse_args(argv)
    configure_logging(args.log_level)
    server = build_server()
    _log.info("starting", extra={"version": __version__, "transport": "stdio"})
    anyio.run(server.run_stdio_async)


if __name__ == "__main__":  # pragma: no cover
    main()
