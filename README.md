# BI Chart MCP

An MCP server that answers business questions from **PostgreSQL** and local **CSV /
Parquet / JSON** files (via DuckDB). It shows the answer as an **interactive chart inside
Claude Desktop**.

- **Safe by construction.** Access is read-only and enforced by the database (a read-only
  role, or a locked DuckDB connection). A sqlglot guard adds a second layer. Rejections come
  with reasons the model can act on.
- **Charts that explain themselves.** Charts are Vega-Lite, rendered through MCP Apps. Each
  chart has a table view, the exact SQL that ran, and when it ran.
- **Small and predictable.** Four tools: `search_catalog`, `describe_table`, `query`,
  `visualize`. The host model writes the SQL; the server needs no LLM API key.

> Status: v2 rewrite, Phase 1 (alpha). See [docs/architecture.md](docs/architecture.md) for
> the design and the roadmap below for what comes next.

## Quickstart (5 minutes, no database needed)

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
git clone https://github.com/c-cf/BI-Chart-MCP-Server.git
cd BI-Chart-MCP-Server
uv sync
uv run python examples/generate_demo_data.py
```

Add the server to Claude Desktop (**Settings → Developer → Edit Config**). Use absolute
paths; see [`examples/claude_desktop_config.json`](examples/claude_desktop_config.json):

```json
{
  "mcpServers": {
    "bi-chart": {
      "command": "/absolute/path/to/uv",
      "args": ["--directory", "/absolute/path/to/BI-Chart-MCP-Server", "run", "bi-chart-mcp",
               "--config", "/absolute/path/to/BI-Chart-MCP-Server/examples/bi-chart-mcp.duckdb.yml"]
    }
  }
}
```

Restart Claude Desktop and ask:

- "What does the shop data contain?"
- "Show monthly revenue from paid orders by product category."
- "Which region had the highest refund rate last quarter? Chart it."

### With PostgreSQL

```bash
docker compose -f examples/docker-compose.yml up -d   # demo data + read-only role
```

Then use a config with a `postgres` source. The DSN goes in the server's `env` block, never
in the config file:

```json
"env": { "SHOP_DSN": "postgresql://bi_reader:bi_reader_demo@127.0.0.1:55432/shop" }
```

See [docs/configuration.md](docs/configuration.md) for every option.

## Tools

| Tool | What it does |
|---|---|
| `search_catalog` | Without arguments: every source, its SQL dialect and its tables. With words: matching tables and columns, ranked. |
| `describe_table` | Columns, types, descriptions, foreign keys, likely joins guessed from column names (`customer_id` -> `customers.id`), and optionally the most frequent values of chosen columns. |
| `query` | Runs one read-only query. Returns a preview, column types, a truncation flag and a handle. |
| `visualize` | Draws an interactive chart from a handle or from SQL. The chart type is chosen automatically unless the model specifies it. |

## Security

Read [docs/security.md](docs/security.md) before pointing the server at company data. In
short:

- Connect PostgreSQL as the role from
  [`scripts/create_readonly_role.sql`](scripts/create_readonly_role.sql).
- List only the schemas the role should see.
- Keep DuckDB data files read-only.

## Development

```bash
uv sync
uv run pytest                      # unit + end-to-end
uv run pytest -m integration       # PostgreSQL via Docker (testcontainers)
uv run ruff check . && uv run ruff format --check . && uv run pyright
cd ui && npm ci && npm test && npm run build   # chart view; commit src/bi_chart_mcp/ui/chart.html
```

## Roadmap

- **Phase 2: context layer and insights.**
  - `context.yml`: glossary, metric definitions, verified queries, served via `get_context`.
  - Deterministic evals.
  - Saved insights (`bi://insights/{id}`).
- **Phase 3: interactivity.**
  - Persistent handles.
  - Drill-down and paging from the chart view.
  - PNG fallback.
- **Phase 4: company deployment.**
  - Streamable HTTP with OAuth and per-user database roles.
  - MCPB bundle and PyPI release.

## License

[Apache-2.0](LICENSE)
