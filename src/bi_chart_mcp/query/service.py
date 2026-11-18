"""Run guarded queries and remember their results by handle."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

import anyio

from bi_chart_mcp.errors import QueryError
from bi_chart_mcp.logs import get_logger
from bi_chart_mcp.query.handles import HandleStore, QueryRecord, make_handle
from bi_chart_mcp.safety.sql_guard import guard_sql
from bi_chart_mcp.sources.registry import SourceRegistry

_audit = get_logger("audit")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


class QueryService:
    """Guard, execute (in a worker thread), audit-log and store a query."""

    def __init__(
        self,
        registry: SourceRegistry,
        handles: HandleStore,
        *,
        clock: Callable[[], str] = _utc_now,
    ) -> None:
        self._registry = registry
        self._handles = handles
        self._clock = clock

    async def run(self, source: str, sql: str) -> QueryRecord:
        src = self._registry.get(source)
        guarded = guard_sql(sql, src.policy)
        try:
            result = await anyio.to_thread.run_sync(src.execute, guarded)
        except QueryError as exc:
            _audit.info(
                "query failed",
                extra={"source": src.name, "code": exc.code, "sql": guarded.sql},
            )
            raise
        record = QueryRecord(
            handle=make_handle(src.name, guarded.sql),
            source=src.name,
            sql=guarded.sql,
            display_sql=guarded.display_sql,
            result=result,
            executed_at=self._clock(),
        )
        self._handles.put(record)
        _audit.info(
            "query",
            extra={
                "source": src.name,
                "handle": record.handle,
                "rows": len(result.rows),
                "truncated": result.truncated,
                "elapsed_ms": result.elapsed_ms,
                "tables": list(guarded.tables),
                "sql": guarded.sql,
            },
        )
        return record

    def record(self, handle: str) -> QueryRecord:
        found = self._handles.get(handle)
        if found is None:
            raise QueryError(
                f"Unknown or expired handle {handle!r}. Run the query again to get a new handle.",
                code="unknown_handle",
            )
        return found
