"""Loader for the YAML SQL-guard corpora under tests/guard_corpus/."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml

CORPUS_DIR = Path(__file__).resolve().parent.parent / "guard_corpus"

CorpusDialect = Literal["postgres", "duckdb"]


@dataclass(frozen=True, slots=True)
class CorpusCase:
    id: str
    sql: str
    code: str | None  # None for allow cases


def _load(dialect: CorpusDialect) -> dict[str, list[dict[str, str]]]:
    with (CORPUS_DIR / f"{dialect}.yaml").open(encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def allow_cases(dialect: CorpusDialect) -> list[CorpusCase]:
    return [CorpusCase(c["id"], c["sql"], None) for c in _load(dialect)["allow"]]


def deny_cases(dialect: CorpusDialect) -> list[CorpusCase]:
    return [CorpusCase(c["id"], c["sql"], c["code"]) for c in _load(dialect)["deny"]]
