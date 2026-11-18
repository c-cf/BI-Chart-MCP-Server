"""Helpers shared by source implementations."""

from __future__ import annotations

import re
import threading
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from typing import Final

from bi_chart_mcp.errors import QueryError

TIMEOUT_CODE: Final = "timeout"
CONNECTION_CODE: Final = "connection_error"
NOT_FOUND_CODE: Final = "not_found"

_MAX_MESSAGE_CHARS: Final = 600
# Quoted absolute paths: "C:\x\y", "C:/x/y", "/x/y", '/x/y'
_QUOTED_PATH = re.compile(r"""(["'])(?:[A-Za-z]:[\\/]|/)[^"']*\1""")


def sanitize_message(message: str, secrets: Iterable[str] = ()) -> str:
    """Strip file system paths and other secrets from a database error message."""
    cleaned = message
    for secret in secrets:
        if secret:
            cleaned = cleaned.replace(secret, "<redacted>")
    cleaned = _QUOTED_PATH.sub(r"\1<path>\1", cleaned)
    cleaned = cleaned.strip()
    if len(cleaned) > _MAX_MESSAGE_CHARS:
        cleaned = cleaned[: _MAX_MESSAGE_CHARS - 1] + "…"
    return cleaned


def timeout_error(timeout_ms: int) -> QueryError:
    return QueryError(
        f"The query was cancelled after {timeout_ms / 1000:g}s. "
        "Add filters, aggregate earlier, or query a smaller time range.",
        code=TIMEOUT_CODE,
    )


@contextmanager
def interrupt_after(
    timeout_ms: int, interrupt: Callable[[], None]
) -> Generator[Callable[[], bool]]:
    """Call `interrupt` if the block is still running after `timeout_ms`.

    Yields a function reporting whether the timer fired.
    """
    fired = threading.Event()

    def _fire() -> None:
        fired.set()
        interrupt()

    timer = threading.Timer(timeout_ms / 1000, _fire)
    timer.daemon = True
    timer.start()
    try:
        yield fired.is_set
    finally:
        timer.cancel()
