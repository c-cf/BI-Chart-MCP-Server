"""Catalog: what can be queried, found by name, described with sample values."""

from __future__ import annotations

import re
import time
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Final

import anyio

from bi_chart_mcp.catalog.joins import infer_joins
from bi_chart_mcp.sources.base import ForeignKey, Source, TableDetail, TableInfo, ValueCount
from bi_chart_mcp.sources.registry import SourceRegistry

_TABLE_CACHE_TTL_S: Final = 60.0
_MAX_TERMS: Final = 8
_COLUMN_SCAN_LIMIT: Final = 300
_NAME_WEIGHT: Final = 3


@dataclass(frozen=True, slots=True)
class SourceSummary:
    name: str
    dialect: str
    description: str | None
    tables: tuple[TableInfo, ...]


@dataclass(frozen=True, slots=True)
class TableMatch:
    source: str
    table: TableInfo
    matched_columns: tuple[str, ...]
    score: int


@dataclass(frozen=True, slots=True)
class TableProfile:
    source: str
    detail: TableDetail
    top_values: dict[str, tuple[ValueCount, ...]]
    inferred_joins: tuple[ForeignKey, ...] = ()  # from column names; see catalog.joins


def search_terms(text: str) -> tuple[str, ...]:
    """Lower-case word tokens of at least two characters, de-duplicated, capped."""
    seen: dict[str, None] = {}
    for token in re.findall(r"\w+", text.lower()):
        if len(token) >= 2:
            seen.setdefault(token, None)
    return tuple(seen)[:_MAX_TERMS]


class CatalogService:
    """Async facade over the sources' (synchronous) introspection."""

    def __init__(
        self,
        registry: SourceRegistry,
        *,
        cache_ttl_s: float = _TABLE_CACHE_TTL_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._registry = registry
        self._ttl = cache_ttl_s
        self._clock = clock
        self._tables: dict[str, tuple[float, tuple[TableInfo, ...]]] = {}

    async def overview(self, source: str | None = None) -> tuple[SourceSummary, ...]:
        summaries: list[SourceSummary] = []
        for src in self._selected(source):
            summaries.append(
                SourceSummary(
                    name=src.name,
                    dialect=src.dialect.value,
                    description=src.description,
                    tables=await self._list_tables(src),
                )
            )
        return tuple(summaries)

    async def search(
        self, text: str, source: str | None = None, limit: int = 30
    ) -> tuple[TableMatch, ...]:
        terms = search_terms(text)
        if not terms:
            return ()
        matches: list[TableMatch] = []
        for src in self._selected(source):
            tables = await self._list_tables(src)
            columns = await anyio.to_thread.run_sync(src.find_columns, terms, _COLUMN_SCAN_LIMIT)
            by_table: dict[str, list[str]] = defaultdict(list)
            for match in columns:
                by_table[match.table].append(match.column)
            for table in tables:
                matched = tuple(by_table.get(table.qualified_name, ()))
                score = _score(table, terms) + len(matched)
                if score > 0:
                    matches.append(TableMatch(src.name, table, matched, score))
        matches.sort(key=lambda m: (-m.score, m.source, m.table.qualified_name))
        return tuple(matches[:limit])

    async def describe(
        self, source: str, table: str, sample_columns: Sequence[str] = (), top_n: int = 10
    ) -> TableProfile:
        src = self._registry.get(source)
        detail = await anyio.to_thread.run_sync(src.describe_table, table)
        samples: dict[str, tuple[ValueCount, ...]] = {}
        for column in dict.fromkeys(sample_columns):
            samples[column] = await anyio.to_thread.run_sync(
                src.top_values, detail.table.qualified_name, column, top_n
            )
        inferred = infer_joins(detail, await self._list_tables(src))
        return TableProfile(
            source=src.name, detail=detail, top_values=samples, inferred_joins=inferred
        )

    def _selected(self, source: str | None) -> tuple[Source, ...]:
        return (self._registry.get(source),) if source else tuple(self._registry)

    async def _list_tables(self, src: Source) -> tuple[TableInfo, ...]:
        cached = self._tables.get(src.name)
        now = self._clock()
        if cached is not None and now - cached[0] < self._ttl:
            return cached[1]
        tables = await anyio.to_thread.run_sync(src.list_tables)
        self._tables[src.name] = (now, tables)
        return tables


def _score(table: TableInfo, terms: Sequence[str]) -> int:
    name = table.qualified_name.lower()
    description = (table.description or "").lower()
    return sum(_NAME_WEIGHT * (t in name) + (t in description) for t in terms)
