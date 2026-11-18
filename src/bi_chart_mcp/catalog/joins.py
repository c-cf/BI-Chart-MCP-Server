"""Join hints from naming conventions, for tables without declared foreign keys.

File sources (DuckDB) never have foreign keys, and many warehouses do not declare
them either. A column `customer_id` very often references `customers.id`; telling the
model so saves it from guessing. Hints are labelled as inferred, never as facts.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from bi_chart_mcp.sources.base import ForeignKey, TableDetail, TableInfo

_ID_SUFFIX: Final = "_id"
_REFERENCED_COLUMN: Final = "id"


def _candidate_names(base: str) -> tuple[str, ...]:
    """customer -> customer, customers; box -> boxes; category -> categories."""
    names = [base, f"{base}s", f"{base}es"]
    if base.endswith("y"):
        names.append(f"{base[:-1]}ies")
    return tuple(names)


def _find_table(base: str, own: TableInfo, tables: Sequence[TableInfo]) -> TableInfo | None:
    candidates = _candidate_names(base)
    matches = [
        t for t in tables if t.name.lower() in candidates and t.qualified_name != own.qualified_name
    ]
    same_schema = [t for t in matches if t.schema == own.schema]
    found = same_schema or matches
    return found[0] if found else None


def infer_joins(detail: TableDetail, tables: Sequence[TableInfo]) -> tuple[ForeignKey, ...]:
    """Likely references for `<name>_id` columns that no declared foreign key covers."""
    declared = {column for fk in detail.foreign_keys for column in fk.columns}
    joins: list[ForeignKey] = []
    for column in detail.columns:
        name = column.name.lower()
        if not name.endswith(_ID_SUFFIX) or column.name in declared:
            continue
        target = _find_table(name.removesuffix(_ID_SUFFIX), detail.table, tables)
        if target is not None:
            joins.append(
                ForeignKey(
                    columns=(column.name,),
                    references_table=target.qualified_name,
                    references_columns=(_REFERENCED_COLUMN,),
                )
            )
    return tuple(joins)
