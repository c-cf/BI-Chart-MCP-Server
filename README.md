# BI Chart MCP

> **Status: v2 rewrite in progress.** The 2025 prototype has been removed; this branch rebuilds the server from scratch.

An MCP server for business questions over **PostgreSQL** and **DuckDB** (CSV / Parquet):

- **Safe, read-only queries**: the database role is the primary boundary; a sqlglot AST guard is the second layer.
- **Interactive charts in Claude Desktop**: Vega-Lite rendered inline through MCP Apps, with the SQL and query time always visible.
- **A lightweight context layer** (planned): glossary, metric definitions and verified queries that improve answer accuracy.

The host model (Claude) writes the SQL; this server makes running it safe and makes the answer visible.

## Development

```bash
uv sync
uv run pytest
uv run ruff check . && uv run pyright
```

Integration tests start a throwaway PostgreSQL with Docker:

```bash
uv run pytest -m integration
```

## License

Apache-2.0
