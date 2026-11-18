"""Query handles: short ids for executed queries, so later calls can reuse the result.

A handle is derived from the source name and the canonical SQL, so re-running the
same query yields the same handle. Results are kept in a bounded in-memory LRU.
"""

from __future__ import annotations

import hashlib
from collections import OrderedDict
from dataclasses import dataclass

from bi_chart_mcp.sources.base import ResultSet

HANDLE_PREFIX = "q_"


def make_handle(source: str, sql: str) -> str:
    digest = hashlib.sha256(f"{source}\0{sql}".encode()).hexdigest()
    return f"{HANDLE_PREFIX}{digest[:16]}"


@dataclass(frozen=True, slots=True)
class QueryRecord:
    """An executed query and its (capped) result."""

    handle: str
    source: str
    sql: str  # canonical SQL that was executed
    display_sql: str
    result: ResultSet
    executed_at: str  # ISO-8601, UTC


class HandleStore:
    """Bounded LRU of recent query records. Used from the event loop thread only."""

    # Each record may hold up to row_cap rows; keep only the recent few a chat reuses.
    def __init__(self, capacity: int = 16) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._records: OrderedDict[str, QueryRecord] = OrderedDict()

    def put(self, record: QueryRecord) -> None:
        self._records[record.handle] = record
        self._records.move_to_end(record.handle)
        while len(self._records) > self._capacity:
            self._records.popitem(last=False)

    def get(self, handle: str) -> QueryRecord | None:
        record = self._records.get(handle)
        if record is not None:
            self._records.move_to_end(handle)
        return record

    def __len__(self) -> int:
        return len(self._records)
