"""Build and look up the configured data sources."""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping, Sequence

from bi_chart_mcp.errors import ConfigError, UnknownSourceError
from bi_chart_mcp.logs import get_logger
from bi_chart_mcp.settings import AppConfig, DuckDBSourceConfig, PostgresSourceConfig
from bi_chart_mcp.sources.base import Source
from bi_chart_mcp.sources.duckdb_source import DuckDBSource
from bi_chart_mcp.sources.postgres_source import PostgresSource

_log = get_logger("sources.registry")


class SourceRegistry:
    """The configured sources, by name."""

    def __init__(self, sources: Sequence[Source]) -> None:
        self._sources: dict[str, Source] = {source.name: source for source in sources}

    def get(self, name: str) -> Source:
        source = self._sources.get(name)
        if source is None:
            raise UnknownSourceError(
                f"Unknown source {name!r}. Configured sources: {', '.join(self.names())}."
            )
        return source

    def names(self) -> tuple[str, ...]:
        return tuple(self._sources)

    def __iter__(self) -> Iterator[Source]:
        return iter(self._sources.values())

    def __len__(self) -> int:
        return len(self._sources)

    def close(self) -> None:
        for source in self._sources.values():
            try:
                source.close()
            except Exception:
                _log.exception("failed to close source", extra={"source": source.name})


def build_sources(config: AppConfig, env: Mapping[str, str] = os.environ) -> SourceRegistry:
    """Create every configured source. On failure, close the ones already built."""
    built: list[Source] = []
    try:
        for source_config in config.sources:
            built.append(_build_one(source_config, config, env))
    except Exception:
        SourceRegistry(built).close()
        raise
    return SourceRegistry(built)


def _build_one(
    source_config: PostgresSourceConfig | DuckDBSourceConfig,
    config: AppConfig,
    env: Mapping[str, str],
) -> Source:
    if isinstance(source_config, DuckDBSourceConfig):
        return DuckDBSource(source_config, config.limits)
    dsn = env.get(source_config.dsn_env)
    if not dsn:
        raise ConfigError(
            f"Environment variable {source_config.dsn_env} is not set; it must hold the "
            f"PostgreSQL connection string for source {source_config.name!r}."
        )
    return PostgresSource(source_config, config.limits, dsn)
