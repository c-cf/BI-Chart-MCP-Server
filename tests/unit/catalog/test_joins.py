"""Join hints inferred from column names, for tables without declared foreign keys."""

from __future__ import annotations

from bi_chart_mcp.catalog.joins import infer_joins
from bi_chart_mcp.sources.base import ColumnInfo, ForeignKey, TableDetail, TableInfo


def _table(name: str, schema: str = "") -> TableInfo:
    return TableInfo(schema=schema, name=name, kind="table")


def _detail(table: TableInfo, columns: list[str], fks: tuple[ForeignKey, ...] = ()) -> TableDetail:
    return TableDetail(
        table=table,
        columns=tuple(ColumnInfo(name=c, db_type="INTEGER", nullable=True) for c in columns),
        foreign_keys=fks,
    )


TABLES = [
    _table("orders"),
    _table("order_items"),
    _table("customers"),
    _table("products"),
    _table("categories"),
    _table("address"),
    _table("boxes"),
]


def test_plural_forms_match() -> None:
    detail = _detail(
        _table("order_items"), ["order_id", "product_id", "category_id", "address_id", "box_id"]
    )
    joins = {(j.columns, j.references_table) for j in infer_joins(detail, TABLES)}
    assert joins == {
        (("order_id",), "orders"),
        (("product_id",), "products"),
        (("category_id",), "categories"),
        (("address_id",), "address"),
        (("box_id",), "boxes"),
    }


def test_referenced_column_is_id() -> None:
    (join,) = infer_joins(_detail(_table("orders"), ["id", "customer_id"]), TABLES)
    assert join.references_columns == ("id",)


def test_declared_foreign_keys_and_unknown_tables_are_skipped() -> None:
    declared = ForeignKey(("customer_id",), "customers", ("id",))
    detail = _detail(_table("orders"), ["customer_id", "warehouse_id", "id"], (declared,))
    assert infer_joins(detail, TABLES) == ()


def test_a_table_never_joins_itself() -> None:
    # The described table may carry a description the listed one lacks; still itself.
    described = TableInfo(schema="", name="orders", kind="table", description="Orders")
    detail = _detail(described, ["order_id"])
    assert infer_joins(detail, TABLES) == ()


def test_same_schema_is_preferred_and_names_are_qualified() -> None:
    tables = [_table("customers", "crm"), _table("customers", "sales"), _table("orders", "sales")]
    (join,) = infer_joins(_detail(_table("orders", "sales"), ["customer_id"]), tables)
    assert join.references_table == "sales.customers"
