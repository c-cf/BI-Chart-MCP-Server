"""Tests for building and looking up sources."""

from __future__ import annotations

from pathlib import Path

import pytest

from bi_chart_mcp.errors import ConfigError, UnknownSourceError
from bi_chart_mcp.settings import parse_config
from bi_chart_mcp.sources.postgres_source import PostgresSource
from bi_chart_mcp.sources.registry import build_sources
from support.files import write_sample_files


def _raw_config(tmp_path: Path) -> dict[str, object]:
    files = write_sample_files(tmp_path)
    return {
        "sources": [
            {
                "type": "duckdb",
                "name": "files",
                "tables": [{"name": "orders", "path": str(files["orders"])}],
            },
            {"type": "postgres", "name": "warehouse", "dsn_env": "WAREHOUSE_DSN"},
        ]
    }


def test_build_and_lookup(tmp_path: Path) -> None:
    config = parse_config(_raw_config(tmp_path), base_dir=tmp_path)
    # Pool opens lazily (min_size=0), so no database is needed to build the source.
    registry = build_sources(config, {"WAREHOUSE_DSN": "postgresql://u:p@127.0.0.1:1/db"})
    try:
        assert registry.names() == ("files", "warehouse")
        assert len(registry) == 2
        assert isinstance(registry.get("warehouse"), PostgresSource)
        assert [s.name for s in registry] == ["files", "warehouse"]
        with pytest.raises(UnknownSourceError, match="Configured sources: files, warehouse"):
            registry.get("nope")
    finally:
        registry.close()


def test_missing_dsn_env_is_a_config_error(tmp_path: Path) -> None:
    config = parse_config(_raw_config(tmp_path), base_dir=tmp_path)
    with pytest.raises(ConfigError, match="WAREHOUSE_DSN is not set"):
        build_sources(config, {})
