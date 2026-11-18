"""End-to-end fixtures: a real server over DuckDB sample files, via an in-memory client."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
from mcp import Client

from bi_chart_mcp.server import build_server
from bi_chart_mcp.services import Services
from bi_chart_mcp.settings import parse_config
from support.files import write_sample_files

CONFIG_YAML = """\
sources:
  - type: duckdb
    name: shop
    description: Sample shop data
    tables:
      - name: orders
        path: data/orders.parquet
        description: One row per order
      - name: customers
        path: data/customers.csv
limits:
  row_cap: 25
  preview_rows: 5
"""


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    write_sample_files(tmp_path / "data")
    path = tmp_path / "bi-chart-mcp.yml"
    path.write_text(CONFIG_YAML, encoding="utf-8")
    return path


@pytest.fixture
def services(config_file: Path) -> Iterator[Services]:
    import yaml

    raw = yaml.safe_load(config_file.read_text(encoding="utf-8"))
    built = Services.from_config(parse_config(raw, base_dir=config_file.parent))
    yield built
    built.close()


@pytest.fixture
async def client(services: Services) -> AsyncIterator[Client]:
    async with Client(build_server(services), raise_exceptions=True) as connected:
        yield connected
