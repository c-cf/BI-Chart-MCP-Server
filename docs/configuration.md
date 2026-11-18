# Configuration

The server reads one YAML file. It looks for the file in this order:

1. `--config path/to/file.yml`
2. the `BI_CHART_MCP_CONFIG` environment variable
3. `./bi-chart-mcp.yml`

Relative paths inside the file resolve against the file's own directory. Two examples are
provided:

- [`examples/bi-chart-mcp.duckdb.yml`](../examples/bi-chart-mcp.duckdb.yml): the
  zero-setup demo.
- [`examples/bi-chart-mcp.example.yml`](../examples/bi-chart-mcp.example.yml): every
  option.

An invalid config stops the server at startup with exit code 2 and a message on stderr.

## `sources`

At least one source is required. Names must be lower-case identifiers. The model refers to
sources by name, and the names appear in the server instructions.

### PostgreSQL

```yaml
- type: postgres
  name: warehouse
  description: Sales warehouse # shown to the model
  dsn_env: WAREHOUSE_DSN # env var holding the connection string (required)
  schemas: [sales, finance] # visible and queryable schemas (default [public])
  statement_timeout_ms: 15000 # optional; defaults to limits.statement_timeout_ms
  pool_max_size: 4 # connections per source (1-32)
```

The DSN is never written in the config file. Set it in the environment of the process that
starts the server. For Claude Desktop, that is the `env` block of the server entry:

```json
"env": { "WAREHOUSE_DSN": "postgresql://bi_reader:secret@127.0.0.1:5432/analytics" }
```

Connect as a dedicated read-only role. See [security.md](security.md) and
[`scripts/create_readonly_role.sql`](../scripts/create_readonly_role.sql).

The server starts even when the database is down. Queries then return a `connection_error`
that tells the user what to check.

### DuckDB (local files)

```yaml
- type: duckdb
  name: files
  description: Finance spreadsheets
  memory_limit: 1GB # optional
  statement_timeout_ms: 15000 # optional
  tables:
    - name: targets # the view name used in SQL
      path: data/targets.csv # .csv, .tsv, .parquet or .json
      description: Monthly revenue targets by region
```

Only the listed files are reachable. A missing file is a configuration error.

## `limits`

| Key | Default | Meaning |
|---|---|---|
| `row_cap` | 5000 | Maximum rows fetched per query. One more row is fetched to detect truncation. |
| `preview_rows` | 50 | Rows included in tool text for the model. |
| `chart_rows` | 500 | Rows sent to the chart view. The payload is also trimmed to stay under about 100k characters. |
| `max_cell_chars` | 500 | Longer text values are cut. |
| `statement_timeout_ms` | 15000 | Default query timeout for sources that do not set their own. |

## Claude Desktop

Add the server to `claude_desktop_config.json`. Use absolute paths, because Claude Desktop
does not start in your project directory:

```json
{
  "mcpServers": {
    "bi-chart": {
      "command": "C:\\Users\\YOU\\.local\\bin\\uv.exe",
      "args": ["--directory", "C:\\path\\to\\BI-Chart-MCP-Server", "run", "bi-chart-mcp",
               "--config", "C:\\path\\to\\your\\bi-chart-mcp.yml"],
      "env": { "WAREHOUSE_DSN": "postgresql://..." }
    }
  }
}
```

Find the path to `uv` with `where uv` (Windows) or `which uv` (macOS/Linux).

## Troubleshooting

- **Logs.** The server logs JSON lines to stderr; set the level with `--log-level DEBUG`.
  Claude Desktop keeps MCP server logs under its logs folder (`mcp-server-bi-chart.log`).
- **Slow first query on Windows with `localhost`.** Windows tries IPv6 (`::1`) first. If the
  database listens only on IPv4, every new connection waits for that attempt to time out.
  Use `127.0.0.1` in the DSN.
- **The chart does not appear.** The host must support MCP Apps (Claude Desktop and
  claude.ai do). Other hosts get the same data as a table. Check that
  `src/bi_chart_mcp/ui/chart.html` exists; rebuild it with `cd ui && npm ci && npm run build`.
