"""Tests for query handles, the query service and catalog search."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from bi_chart_mcp.catalog.service import CatalogService, search_terms
from bi_chart_mcp.errors import QueryError
from bi_chart_mcp.query.handles import HandleStore, QueryRecord, make_handle
from bi_chart_mcp.query.service import QueryService
from bi_chart_mcp.safety.sql_guard import GuardViolation
from bi_chart_mcp.settings import parse_config
from bi_chart_mcp.sources.base import ResultSet
from bi_chart_mcp.sources.registry import SourceRegistry, build_sources
from support.files import write_sample_files

pytestmark = pytest.mark.anyio


def _record(handle: str) -> QueryRecord:
    empty = ResultSet(columns=(), rows=(), truncated=False, elapsed_ms=0)
    return QueryRecord(handle, "s", "SELECT 1", "SELECT 1", empty, "2026-01-01T00:00:00+00:00")


def test_handles_are_deterministic_and_distinct() -> None:
    assert make_handle("a", "SELECT 1") == make_handle("a", "SELECT 1")
    assert make_handle("a", "SELECT 1") != make_handle("b", "SELECT 1")
    assert make_handle("a", "SELECT 1").startswith("q_")


def test_handle_store_evicts_least_recently_used() -> None:
    store = HandleStore(capacity=2)
    store.put(_record("a"))
    store.put(_record("b"))
    assert store.get("a") is not None  # touch a, so b is now the oldest
    store.put(_record("c"))
    assert store.get("b") is None
    assert store.get("a") is not None
    assert len(store) == 2
    with pytest.raises(ValueError, match="capacity"):
        HandleStore(capacity=0)


def test_search_terms() -> None:
    assert search_terms("Monthly revenue, by region & a") == ("monthly", "revenue", "by", "region")
    assert search_terms("x !") == ()
    assert len(search_terms(" ".join(f"w{i}" for i in range(20)))) == 8


@pytest.fixture
def registry(tmp_path: Path) -> Iterator[SourceRegistry]:
    files = write_sample_files(tmp_path)
    raw = {
        "sources": [
            {
                "type": "duckdb",
                "name": "shop",
                "tables": [
                    {"name": "orders", "path": str(files["orders"]), "description": "Sales"},
                    {"name": "customers", "path": str(files["customers"])},
                ],
            }
        ]
    }
    built = build_sources(parse_config(raw, base_dir=tmp_path))
    yield built
    built.close()


async def test_query_service_runs_and_stores(registry: SourceRegistry) -> None:
    service = QueryService(registry, HandleStore(), clock=lambda: "2026-11-18T00:00:00+00:00")
    record = await service.run("shop", "SELECT count(*) FROM orders")
    assert record.result.rows == ((40,),)
    assert record.executed_at == "2026-11-18T00:00:00+00:00"
    assert service.record(record.handle) is record
    with pytest.raises(QueryError) as info:
        service.record("q_missing")
    assert info.value.code == "unknown_handle"


async def test_query_service_rejects_before_executing(registry: SourceRegistry) -> None:
    service = QueryService(registry, HandleStore())
    with pytest.raises(GuardViolation):
        await service.run("shop", "DROP TABLE orders")


async def test_catalog_caches_table_lists(registry: SourceRegistry) -> None:
    now = [0.0]
    catalog = CatalogService(registry, cache_ttl_s=10, clock=lambda: now[0])
    first = await catalog.overview()
    second = await catalog.overview("shop")
    assert first[0].tables is second[0].tables  # served from cache
    now[0] = 11.0
    third = await catalog.overview()
    assert third[0].tables is not first[0].tables


async def test_catalog_search_uses_descriptions(registry: SourceRegistry) -> None:
    catalog = CatalogService(registry)
    matches = await catalog.search("sales")
    assert [m.table.name for m in matches] == ["orders"]
    assert await catalog.search("!") == ()
