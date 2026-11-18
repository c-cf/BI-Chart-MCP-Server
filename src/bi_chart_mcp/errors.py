"""Domain errors.

Every error raised for a reason the model can act on derives from `BiChartError`.
Tool handlers convert these into MCP `ToolError`s, so the message must be safe to
show to the model: no DSNs, file system paths or stack traces.
"""

from __future__ import annotations

from typing import ClassVar


class BiChartError(Exception):
    """Base class for anticipated, model-facing failures."""

    default_code: ClassVar[str] = "error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self._code = code or self.default_code

    @property
    def code(self) -> str:
        """Stable machine-readable reason, e.g. `forbidden_function`."""
        return self._code


class ConfigError(BiChartError):
    """The server configuration is invalid."""

    default_code = "config_error"


class UnknownSourceError(BiChartError):
    """A tool referenced a data source that is not configured."""

    default_code = "unknown_source"


class QueryError(BiChartError):
    """The database rejected or failed to run a query."""

    default_code = "query_error"
