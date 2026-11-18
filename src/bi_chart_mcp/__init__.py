"""BI Chart MCP: safe, read-only BI over PostgreSQL and DuckDB with interactive charts."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("bi-chart-mcp")
except PackageNotFoundError:  # pragma: no cover - only when running from a raw checkout
    __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
