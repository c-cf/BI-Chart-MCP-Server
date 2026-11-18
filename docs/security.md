# Security

The server runs SQL written by a language model, and the model may be steered by
untrusted text, including text stored in the database itself. The design assumes any
SQL can be hostile. Defences are layered, and **the database is the primary boundary**.

## Layers

### 1. Database permissions (primary)

- **PostgreSQL:** connect as a dedicated role created by
  [`scripts/create_readonly_role.sql`](../scripts/create_readonly_role.sql).
  - It has `SELECT` only, on the schemas you list.
  - It is not a member of any `pg_*` predefined role, and owns nothing.
  - It defaults to `default_transaction_read_only = on` plus statement, lock and idle
    timeouts.
- **DuckDB:** each configured file becomes a view. Then:
  1. `allowed_paths` is set to exactly those files.
  2. `enable_external_access = false` is set.
  3. Extension install and autoload are disabled.
  4. `lock_configuration = true` freezes all of the above. No query can undo it.

### 2. Per-query session (PostgreSQL)

- Every query runs in `BEGIN READ ONLY`. The transaction is always rolled back.
- `SET LOCAL` sets `statement_timeout`, `lock_timeout` and a `search_path` limited to the
  configured schemas.
- `DISCARD ALL` runs when a connection returns to the pool. This drops session-level
  leftovers such as advisory locks, settings and prepared statements.

### 3. SQL guard (second line)

[`safety/sql_guard.py`](../src/bi_chart_mcp/safety/sql_guard.py) parses the SQL with
sqlglot. It accepts **exactly one** `SELECT` / `WITH` / set operation and rejects:

- non-query statements (DML, DDL, `COPY`, `SET`, `ATTACH`, `INSTALL`, `PRAGMA`, ...);
- write paths hidden inside queries: data-modifying CTEs, `SELECT ... INTO`, `FOR UPDATE/SHARE`;
- dangerous functions:
  - PostgreSQL: `pg_*` except a small allowlist, `lo_*`, `dblink*`, `set_config`,
    `current_setting`, `nextval`/`setval`, `query_to_xml`-style functions that run SQL
    strings, `ts_stat`;
  - DuckDB: `read_*`, `glob`, `query`, `query_table`, `duckdb_*`, `pragma_*`, `getenv`, ...;
- system catalogs (`pg_catalog`, `information_schema`, unqualified `pg_*` views);
- PostgreSQL tables in schemas that are not configured;
- for DuckDB, any table that is not a configured view, including `FROM 'file.csv'` scans.

The server executes **the SQL regenerated from the parse tree**, never the original text.
That removes tricks that rely on the guard and the database parsing differently.
Comments are stripped. Unicode-escaped identifiers fail to parse, so they are rejected.

Every query also gets `LIMIT row_cap + 1` (or the user's smaller limit), so truncation can
be detected and reported.

### 4. Output handling

- Values are converted to JSON-safe forms. Long text is truncated. Binary values become
  short previews.
- Error messages are sanitized: no DSNs, passwords or file system paths.
- Tool text and server instructions tell the model that rows are untrusted data, not
  instructions.
- The chart view has no network access: no CSP exceptions, and a Vega loader that rejects
  every request.
- An audit log line for every query goes to stderr as JSON: source, handle, tables, row
  count, duration and SQL.

## What each layer actually stops

The integration suites replay the guard's deny corpus **with the guard disabled**, to
record which layer stops what:

| Attack class | Stopped by the database | Stopped only by the guard |
|---|---|---|
| Writes, DDL, data-modifying CTEs, `SELECT INTO`, `FOR UPDATE` | ✅ read-only transaction | |
| Privileged reads (`pg_read_file`, `pg_authid`, other schemas) | ✅ role permissions | |
| `nextval`, large-object writes, `query_to_xml('DELETE ...')` | ✅ read-only transaction | |
| DuckDB file reads, `ATTACH`, `INSTALL`, `COPY`, `SET` | ✅ locked configuration | |
| `pg_sleep`, advisory locks, `set_config`, `pg_terminate_backend` | (timeouts and session reset limit the damage) | ✅ |
| Catalog snooping (`pg_settings`, `information_schema`, `duckdb_settings()`) | | ✅ |

## Recommendations for company deployments

- Give each team its own role and source, with only the schemas they need. Use PostgreSQL
  row-level security for per-row rules.
- Make DuckDB data files read-only at the OS level.
- Treat the audit log as a security log, and ship stderr to your log pipeline.
- Remote (Streamable HTTP + OAuth) deployment with per-user database roles is planned for
  Phase 4. Until then, run the server locally per user over stdio.

## Reporting a vulnerability

Please open a private security advisory on GitHub rather than a public issue.
