"""Write real `visualize` results for the UI dev host (ui/dev/host.html).

Usage:
    uv run python examples/generate_demo_data.py
    uv run python scripts/make_ui_samples.py

Runs the server in-process against the demo DuckDB config and saves each tool result
as JSON under ui/dev/samples/.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anyio
from mcp import Client
from mcp.client import advertise
from mcp.server.apps import APP_MIME_TYPE, EXTENSION_ID

from bi_chart_mcp.server import build_server
from bi_chart_mcp.services import Services
from bi_chart_mcp.settings import load_config

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "ui" / "dev" / "samples"

REVENUE = "sum(i.quantity * i.unit_price * (1 - i.discount))"
JOINS = (
    "FROM order_items i JOIN orders o ON o.id = i.order_id "
    "JOIN products p ON p.id = i.product_id JOIN customers c ON c.id = o.customer_id "
    "WHERE o.status = 'paid'"
)

SAMPLES: dict[str, dict[str, Any]] = {
    "monthly-revenue-by-category": {
        "sql": f"SELECT date_trunc('month', o.order_date) AS month, p.category, "
        f"{REVENUE} AS revenue {JOINS} GROUP BY 1, 2 ORDER BY 1, 2",
        "title": "Monthly revenue by category",
    },
    "revenue-by-product": {
        "sql": f"SELECT p.name AS product, {REVENUE} AS revenue {JOINS} "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 15",
    },
    "revenue-mix-by-region": {
        "sql": f"SELECT c.region, {REVENUE} AS revenue {JOINS} GROUP BY 1",
        "chart": {"mark": "arc", "donut": True},
        "title": "Revenue mix by region",
    },
    "orders-heatmap": {
        "sql": "SELECT strftime(order_date, '%a') AS weekday, channel, count(*) AS orders "
        "FROM orders GROUP BY 1, 2",
        "chart": {"mark": "rect", "x": "channel", "y": "weekday", "color": "orders"},
    },
    "total-revenue": {
        "sql": f"SELECT {REVENUE} AS total_revenue {JOINS}",
        "title": "Total paid revenue",
    },
}


async def main() -> None:
    services = Services.from_config(load_config(ROOT / "examples" / "bi-chart-mcp.duckdb.yml"))
    OUT.mkdir(parents=True, exist_ok=True)
    supports_apps = advertise(EXTENSION_ID, {"mimeTypes": [APP_MIME_TYPE]})
    try:
        async with Client(build_server(services), extensions=[supports_apps]) as client:
            for name, args in SAMPLES.items():
                result = await client.call_tool("visualize", {"source": "shop", **args})
                if result.is_error:
                    raise SystemExit(f"{name}: {result.content}")
                payload = result.model_dump(mode="json", by_alias=True, exclude_none=True)
                (OUT / f"{name}.json").write_text(json.dumps(payload, indent=1), "utf-8")
                print(f"wrote {name}")
    finally:
        services.close()


if __name__ == "__main__":
    anyio.run(main)
