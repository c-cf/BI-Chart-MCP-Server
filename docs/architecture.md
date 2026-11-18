# Architecture

BI Chart MCP lets an MCP host (Claude Desktop first) answer business questions from
PostgreSQL and local data files, and show the answer as an interactive chart.

The host model writes the SQL. This server makes running it safe, keeps results small
enough for a chat, and turns them into charts. It does not embed its own LLM.

## Request flow

```
Claude Desktop ──stdio──▶ MCPServer (mcp 2.x, spec 2026-07-28)
                           │
   search_catalog ─────────┼─▶ CatalogService ─▶ Source.list_tables / find_columns
   describe_table ─────────┤                    Source.describe_table / top_values
   query ──────────────────┼─▶ QueryService ─▶ guard_sql ─▶ Source.execute (worker thread)
   visualize ──────────────┘        │                         │
        │                           └── HandleStore (LRU) ◀───┘
        ├─ ChartIntent ─▶ recommend.complete_intent ─▶ compile.compile_chart (Vega-Lite 6)
        └─ structuredContent ─────────────▶ ui://bi-chart/chart.html (MCP Apps view)
```

## Modules

| Path | Responsibility |
|---|---|
| `settings.py` | YAML config, validated by frozen pydantic models. DSNs come from env vars. |
| `sources/` | `Source` protocol plus PostgreSQL and DuckDB implementations. Synchronous; called through `anyio.to_thread`. |
| `safety/sql_guard.py` | Parses SQL with sqlglot, accepts one read-only query, applies the row cap, returns the canonical SQL to execute. |
| `catalog/service.py` | Overview, keyword search and table profiles across sources. |
| `query/` | Query execution and audit logging, result handles, JSON-safe value conversion, markdown previews. |
| `charts/` | `ChartIntent` (what the model asks for), recommendation (what fills the gaps), compilation to Vega-Lite. |
| `tools/` | MCP tool handlers: thin adapters that call services and shape output. |
| `server.py` | Assembly only: registers tools and the Apps view. |
| `ui/` (repo root) | The chart view: Vite + TypeScript, built into `src/bi_chart_mcp/ui/chart.html`. |

## Design decisions

**Few tools, flat parameters.** Four model-visible tools cover the workflow. A tool per
chart type would cost tokens on every request, and union-typed parameters confuse models.

**Handles instead of re-sending data.** `query` returns a handle. `visualize` can chart a
handle without running the query again, or take `source` + `sql` directly. Handles are
content-addressed (source + canonical SQL) and kept in an in-memory LRU. They do not
survive a server restart; persisting them is planned for Phase 3.

**The model picks intent, not Vega-Lite.** `ChartIntent` is small and validated. The server
chooses chart type, axes, sort and orientation when the model leaves them out. The server
compiles the spec, so it never contains URLs or data. The view inlines the rows as the
`table` dataset.

**Two audiences per tool result.** `content` is short text plus a markdown preview for the
model. `structuredContent` is the full typed result for the view. Claude Code shows the
model `structuredContent` instead of `content`, so it stays readable too.

**Results stay small.** Rows are capped per query (`row_cap`) and per chart (`chart_rows`).
The chart payload is also trimmed to about 100k characters, because Claude stops rendering
apps whose tool result exceeds about 150k.

**Single-file view, no network.** vega, vega-lite and vega-embed are bundled into one HTML
file. The view needs no CSP exceptions. Vega runs with `ast: true` (no `eval`) and a loader
that refuses every request.

## Testing

| Suite | What it proves |
|---|---|
| `tests/unit` | Guard corpus (allow/deny with reason codes), value conversion, chart recommendation, golden Vega-Lite specs, DuckDB lockdown. |
| `tests/e2e` | Every tool through the MCP protocol (in-memory client), plus the real CLI over stdio. |
| `tests/integration` | PostgreSQL in Docker as a read-only role: introspection, timeouts, session reset, and the deny corpus replayed with the guard disabled. |
| `ui/test` | Every golden spec compiles with the real vega-lite library without warnings; payload parsing. |
