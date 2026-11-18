"""Compact text for the catalog tools: what the model reads.

Indented JSON spends most of its characters on braces, quotes and nulls. These
renderers say the same thing in roughly half the space.
"""

from __future__ import annotations

import json

from bi_chart_mcp.query.formatting import format_value
from bi_chart_mcp.tools.catalog_models import (
    ColumnOut,
    DescribeTableOutput,
    ForeignKeyOut,
    SearchCatalogOutput,
    SourceOverview,
    TableMatchOut,
    TableSummary,
)


def _table_line(table: TableSummary) -> str:
    rows = f" (~{format_value(table.row_estimate)} rows)" if table.row_estimate else ""
    about = f": {table.description}" if table.description else ""
    return f"- {table.name}{rows}{about}"


def _source_block(source: SourceOverview) -> list[str]:
    about = f": {source.description}" if source.description else ""
    listed = f", first {len(source.tables)} listed" if source.tables_truncated else ""
    return [
        f"Source {source.name} ({source.dialect} SQL){about}",
        f"{source.table_count} tables{listed}:",
        *(_table_line(t) for t in source.tables),
        "",
    ]


def _match_line(match: TableMatchOut) -> str:
    about = f": {match.description}" if match.description else ""
    columns = (
        f" [matching columns: {', '.join(match.matched_columns)}]" if match.matched_columns else ""
    )
    return f"- {match.table} in {match.source}{about}{columns}"


def search_text(output: SearchCatalogOutput) -> str:
    lines: list[str] = []
    for source in output.sources or ():
        lines += _source_block(source)
    if output.matches:
        lines += ["Matching tables, best first:", *(_match_line(m) for m in output.matches), ""]
    lines.append(output.hint)
    return "\n".join(lines)


def _top_values(column: ColumnOut) -> str:
    if not column.top_values:
        return ""
    values = ", ".join(
        f"{json.dumps(v.value, ensure_ascii=False)} ({format_value(v.count)})"
        for v in column.top_values
    )
    return f"; top values: {values}"


def _column_line(column: ColumnOut) -> str:
    required = " NOT NULL" if not column.nullable else ""
    about = f": {column.description}" if column.description else ""
    return f"- {column.name} {column.db_type}{required}{about}{_top_values(column)}"


def _join_line(join: ForeignKeyOut) -> str:
    return (
        f"- {', '.join(join.columns)} -> "
        f"{join.references_table}({', '.join(join.references_columns)})"
    )


def describe_text(output: DescribeTableOutput) -> str:
    facts = [output.kind]
    if output.row_estimate:
        facts.append(f"~{format_value(output.row_estimate)} rows")
    about = f": {output.description}" if output.description else ""
    lines = [
        f"Table {output.table} in source {output.source} ({', '.join(facts)}){about}",
        "Columns:",
        *(_column_line(c) for c in output.columns),
    ]
    if output.foreign_keys:
        lines += ["Foreign keys:", *(_join_line(fk) for fk in output.foreign_keys)]
    if output.inferred_joins:
        lines += [
            "Likely joins (guessed from column names, not declared; check before relying on them):",
            *(_join_line(j) for j in output.inferred_joins),
        ]
    return "\n".join(lines)
