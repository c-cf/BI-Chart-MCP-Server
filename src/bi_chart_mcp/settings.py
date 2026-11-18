"""Configuration: a YAML file plus environment variables for secrets.

Database credentials never live in the config file: a PostgreSQL source names the
environment variable that holds its DSN (`dsn_env`).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Literal, cast

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from bi_chart_mcp.errors import ConfigError

CONFIG_ENV_VAR = "BI_CHART_MCP_CONFIG"
DEFAULT_CONFIG_FILE = "bi-chart-mcp.yml"

# Lower-case identifiers only: names are embedded (quoted) in SQL and shown to the model.
_IDENTIFIER = r"^[a-z][a-z0-9_]{0,62}$"
_ENV_NAME = r"^[A-Za-z_][A-Za-z0-9_]*$"

Identifier = Annotated[str, Field(pattern=_IDENTIFIER)]


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class Limits(_Frozen):
    """Caps that keep results small enough for a chat context."""

    row_cap: Annotated[int, Field(ge=1, le=100_000)] = 5000
    preview_rows: Annotated[int, Field(ge=1, le=500)] = 50
    chart_rows: Annotated[int, Field(ge=1, le=5000)] = 500
    max_cell_chars: Annotated[int, Field(ge=16, le=10_000)] = 500
    statement_timeout_ms: Annotated[int, Field(ge=100, le=600_000)] = 15_000


class PostgresSourceConfig(_Frozen):
    type: Literal["postgres"]
    name: Identifier
    description: str | None = None
    dsn_env: Annotated[str, Field(pattern=_ENV_NAME)]
    schemas: tuple[Identifier, ...] = ("public",)
    statement_timeout_ms: Annotated[int, Field(ge=100, le=600_000)] | None = None
    pool_max_size: Annotated[int, Field(ge=1, le=32)] = 4

    @field_validator("schemas")
    @classmethod
    def _non_empty_schemas(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value:
            raise ValueError("at least one schema is required")
        return value


class DuckDBTableConfig(_Frozen):
    name: Identifier
    path: Path
    description: str | None = None

    @field_validator("path")
    @classmethod
    def _supported_format(cls, value: Path) -> Path:
        if value.suffix.lower() not in {".csv", ".parquet", ".json", ".tsv"}:
            raise ValueError(
                f"unsupported file type {value.suffix!r}; use csv, tsv, parquet or json"
            )
        return value


class DuckDBSourceConfig(_Frozen):
    type: Literal["duckdb"]
    name: Identifier
    description: str | None = None
    tables: tuple[DuckDBTableConfig, ...]
    memory_limit: str = "1GB"
    statement_timeout_ms: Annotated[int, Field(ge=100, le=600_000)] | None = None

    @model_validator(mode="after")
    def _unique_tables(self) -> DuckDBSourceConfig:
        names = [table.name for table in self.tables]
        if not names:
            raise ValueError("at least one table is required")
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate table names: {', '.join(duplicates)}")
        return self


SourceConfig = Annotated[PostgresSourceConfig | DuckDBSourceConfig, Field(discriminator="type")]


class AppConfig(_Frozen):
    sources: tuple[SourceConfig, ...]
    limits: Limits = Limits()

    @model_validator(mode="after")
    def _unique_sources(self) -> AppConfig:
        names = [source.name for source in self.sources]
        if not names:
            raise ValueError("at least one source is required")
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate source names: {', '.join(duplicates)}")
        return self


def resolve_config_path(explicit: str | None, env: Mapping[str, str] = os.environ) -> Path:
    """`--config` wins, then $BI_CHART_MCP_CONFIG, then ./bi-chart-mcp.yml."""
    return Path(explicit or env.get(CONFIG_ENV_VAR) or DEFAULT_CONFIG_FILE).expanduser()


def load_config(path: Path) -> AppConfig:
    """Read and validate a config file. Relative table paths resolve against its directory."""
    try:
        raw: Any = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError(f"Config file not found: {path}") from None
    except yaml.YAMLError as exc:
        raise ConfigError(f"Config file {path} is not valid YAML: {exc}") from None
    if not isinstance(raw, dict):
        raise ConfigError(f"Config file {path} must contain a mapping with a 'sources' list")
    return parse_config(cast(dict[str, Any], raw), base_dir=path.resolve().parent)


def parse_config(raw: Mapping[str, Any], *, base_dir: Path) -> AppConfig:
    """Validate an already-loaded config mapping."""
    try:
        config = AppConfig.model_validate(raw)
    except ValidationError as exc:
        details = "; ".join(
            f"{'.'.join(str(p) for p in err['loc'])}: {err['msg']}" for err in exc.errors()
        )
        raise ConfigError(f"Invalid config: {details}") from None
    return _with_absolute_paths(config, base_dir)


def _with_absolute_paths(config: AppConfig, base_dir: Path) -> AppConfig:
    sources: list[PostgresSourceConfig | DuckDBSourceConfig] = []
    for source in config.sources:
        if isinstance(source, DuckDBSourceConfig):
            tables = tuple(
                table.model_copy(update={"path": (base_dir / table.path.expanduser()).resolve()})
                for table in source.tables
            )
            sources.append(source.model_copy(update={"tables": tables}))
        else:
            sources.append(source)
    return config.model_copy(update={"sources": tuple(sources)})
