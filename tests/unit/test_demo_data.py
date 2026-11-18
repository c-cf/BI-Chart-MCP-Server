"""The demo generator and the example configs work together."""

from __future__ import annotations

import importlib.util
import shutil
from pathlib import Path
from types import ModuleType

import pytest

from bi_chart_mcp.services import Services
from bi_chart_mcp.settings import load_config

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"

pytestmark = pytest.mark.anyio


def _generator() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "generate_demo_data", EXAMPLES / "generate_demo_data.py"
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generator_is_deterministic(tmp_path: Path) -> None:
    generate = _generator().generate
    generate(tmp_path / "a")
    generate(tmp_path / "b")
    for name in ("customers", "products", "orders", "order_items"):
        assert (tmp_path / "a" / f"{name}.csv").read_bytes() == (
            tmp_path / "b" / f"{name}.csv"
        ).read_bytes()


async def test_duckdb_example_config_answers_a_question(tmp_path: Path) -> None:
    _generator().generate(tmp_path / "demo_data")
    shutil.copy(EXAMPLES / "bi-chart-mcp.duckdb.yml", tmp_path / "bi-chart-mcp.yml")
    services = Services.from_config(load_config(tmp_path / "bi-chart-mcp.yml"))
    try:
        record = await services.queries.run(
            "shop",
            "SELECT p.category, sum(i.quantity * i.unit_price * (1 - i.discount)) AS revenue "
            "FROM order_items i JOIN products p ON p.id = i.product_id "
            "JOIN orders o ON o.id = i.order_id WHERE o.status = 'paid' "
            "GROUP BY 1 ORDER BY 2 DESC",
        )
    finally:
        services.close()
    assert next(row[0] for row in record.result.rows) == "Electronics"
    assert len(record.result.rows) == 5


def test_full_example_config_is_valid(tmp_path: Path) -> None:
    import yaml

    from bi_chart_mcp.settings import parse_config

    raw = yaml.safe_load((EXAMPLES / "bi-chart-mcp.example.yml").read_text(encoding="utf-8"))
    config = parse_config(raw, base_dir=tmp_path)
    assert [s.name for s in config.sources] == ["warehouse", "files"]
