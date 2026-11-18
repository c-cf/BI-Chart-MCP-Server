"""The application's services, built once from config and shared by all tools."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from bi_chart_mcp.catalog.service import CatalogService
from bi_chart_mcp.query.handles import HandleStore
from bi_chart_mcp.query.service import QueryService
from bi_chart_mcp.settings import AppConfig, Limits
from bi_chart_mcp.sources.registry import SourceRegistry, build_sources


@dataclass(frozen=True, slots=True)
class Services:
    registry: SourceRegistry
    catalog: CatalogService
    queries: QueryService
    limits: Limits

    @classmethod
    def from_config(cls, config: AppConfig, env: Mapping[str, str] = os.environ) -> Services:
        registry = build_sources(config, env)
        return cls(
            registry=registry,
            catalog=CatalogService(registry),
            queries=QueryService(registry, HandleStore()),
            limits=config.limits,
        )

    def close(self) -> None:
        self.registry.close()
