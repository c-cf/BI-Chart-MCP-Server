"""Tests for configuration loading and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from bi_chart_mcp.errors import ConfigError
from bi_chart_mcp.settings import (
    CONFIG_ENV_VAR,
    DuckDBSourceConfig,
    PostgresSourceConfig,
    load_config,
    parse_config,
    resolve_config_path,
)

VALID = {
    "sources": [
        {"type": "postgres", "name": "warehouse", "dsn_env": "WAREHOUSE_DSN", "schemas": ["sales"]},
        {
            "type": "duckdb",
            "name": "files",
            "tables": [{"name": "orders", "path": "data/orders.parquet"}],
        },
    ],
    "limits": {"row_cap": 1000},
}


def test_valid_config_parses_and_resolves_relative_paths(tmp_path: Path) -> None:
    config = parse_config(VALID, base_dir=tmp_path)
    warehouse, files = config.sources
    assert isinstance(warehouse, PostgresSourceConfig)
    assert warehouse.schemas == ("sales",)
    assert isinstance(files, DuckDBSourceConfig)
    assert files.tables[0].path == (tmp_path / "data" / "orders.parquet").resolve()
    assert config.limits.row_cap == 1000
    assert config.limits.preview_rows == 50  # default


def test_load_config_from_file(tmp_path: Path) -> None:
    path = tmp_path / "bi-chart-mcp.yml"
    path.write_text(
        "sources:\n  - type: duckdb\n    name: files\n    tables:\n"
        "      - name: orders\n        path: orders.csv\n",
        encoding="utf-8",
    )
    config = load_config(path)
    source = config.sources[0]
    assert isinstance(source, DuckDBSourceConfig)
    assert source.tables[0].path == (tmp_path / "orders.csv").resolve()


@pytest.mark.parametrize(
    ("raw", "fragment"),
    [
        ({"sources": []}, "at least one source"),
        ({"sources": [{"type": "mysql", "name": "x"}]}, "sources.0"),
        (
            {"sources": [{"type": "postgres", "name": "Bad Name", "dsn_env": "X"}]},
            "name",
        ),
        (
            {
                "sources": [
                    {"type": "postgres", "name": "a", "dsn_env": "X"},
                    {"type": "postgres", "name": "a", "dsn_env": "Y"},
                ]
            },
            "duplicate source names: a",
        ),
        (
            {
                "sources": [
                    {"type": "duckdb", "name": "f", "tables": [{"name": "t", "path": "x.xlsx"}]}
                ]
            },
            "unsupported file type",
        ),
        (
            {
                "sources": [
                    {
                        "type": "duckdb",
                        "name": "f",
                        "tables": [{"name": "t", "path": "a.csv"}, {"name": "t", "path": "b.csv"}],
                    }
                ]
            },
            "duplicate table names: t",
        ),
        ({"sources": [{"type": "postgres", "name": "a", "dsn": "postgres://u:p@h/db"}]}, "dsn"),
    ],
)
def test_invalid_configs_raise_config_error(raw: dict[str, object], fragment: str) -> None:
    with pytest.raises(ConfigError, match=fragment):
        parse_config(raw, base_dir=Path())


def test_missing_and_malformed_files(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config(tmp_path / "missing.yml")
    bad = tmp_path / "bad.yml"
    bad.write_text("sources: [", encoding="utf-8")
    with pytest.raises(ConfigError, match="not valid YAML"):
        load_config(bad)
    scalar = tmp_path / "scalar.yml"
    scalar.write_text("42", encoding="utf-8")
    with pytest.raises(ConfigError, match="mapping"):
        load_config(scalar)


def test_resolve_config_path_precedence() -> None:
    assert resolve_config_path("x.yml", {CONFIG_ENV_VAR: "y.yml"}) == Path("x.yml")
    assert resolve_config_path(None, {CONFIG_ENV_VAR: "y.yml"}) == Path("y.yml")
    assert resolve_config_path(None, {}) == Path("bi-chart-mcp.yml")
