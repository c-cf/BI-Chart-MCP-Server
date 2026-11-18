"""Per-dialect denylists for the SQL guard.

These are defence in depth. The primary control is the database itself: a
read-only PostgreSQL role, or a DuckDB connection locked to configured files.
Names are compared lower-case.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DialectRules:
    """Function and table rules for one SQL dialect."""

    denied_function_prefixes: tuple[str, ...]
    denied_functions: frozenset[str]
    allowed_functions: frozenset[str]  # exceptions to the prefix rules
    denied_schemas: frozenset[str]
    denied_table_prefixes: tuple[str, ...]

    def is_function_denied(self, name: str) -> bool:
        lowered = name.lower()
        if lowered in self.allowed_functions:
            return False
        return lowered in self.denied_functions or lowered.startswith(self.denied_function_prefixes)


POSTGRES_RULES = DialectRules(
    denied_function_prefixes=(
        "pg_",  # sleep, file access, advisory locks, backend signalling, notify, ...
        "lo_",  # large objects: import/export/write
        "dblink",  # runs SQL on a separate, non-read-only connection
        # Functions that execute a SQL string the guard never sees:
        "query_to_xml",
        "cursor_to_xml",
        "table_to_xml",
        "schema_to_xml",
        "database_to_xml",
    ),
    denied_functions=frozenset(
        {
            "lowrite",
            "loread",
            "set_config",  # session-level GUC changes leak into pooled connections
            "current_setting",  # reads server settings such as data_directory
            "nextval",
            "setval",
            "txid_current",
            "txid_current_if_assigned",
            "ts_stat",  # executes its text argument as SQL
        }
    ),
    allowed_functions=frozenset({"pg_typeof", "pg_size_pretty", "pg_column_size"}),
    denied_schemas=frozenset({"pg_catalog", "information_schema", "pg_toast"}),
    # Catalog views such as pg_settings resolve without a schema prefix.
    denied_table_prefixes=("pg_",),
)

DUCKDB_RULES = DialectRules(
    denied_function_prefixes=(
        "read_",  # read_csv, read_parquet, read_json, read_text, read_blob, ...
        "parquet_",
        "sqlite_",
        "postgres_",
        "mysql_",
        "iceberg_",
        "delta_",
        "st_read",
        "duckdb_",  # duckdb_settings, duckdb_secrets, duckdb_extensions, ...
        "pragma_",
        "json_execute_serialized_sql",
    ),
    denied_functions=frozenset(
        {
            "glob",
            "query",
            "query_table",
            "sniff_csv",
            "from_csv_auto",
            "getenv",
            "sleep_ms",
            "current_setting",
            "which_secret",
            "load_aws_credentials",
            "checkpoint",
            "force_checkpoint",
        }
    ),
    allowed_functions=frozenset(),
    denied_schemas=frozenset({"information_schema", "pg_catalog"}),
    denied_table_prefixes=("duckdb_", "pragma_", "sqlite_"),
)
