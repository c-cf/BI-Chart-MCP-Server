"""Human-scale formatting of values for text the model reads."""

from __future__ import annotations

import math
import re
from decimal import ROUND_HALF_UP, Context, Decimal
from typing import Final

from bi_chart_mcp.query.values import JsonValue

_MIDNIGHT: Final = re.compile(r"^(\d{4}-\d{2}-\d{2})T00:00:00(?:\.0+)?(?:Z|[+-]00:00)?$")
_SIGNIFICANT: Final = Context(prec=4, rounding=ROUND_HALF_UP)
_THOUSAND: Final = 1000


def format_value(value: JsonValue) -> str:
    """Thousands separators, few decimals (rounded half up), dates without midnight."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}"
    if isinstance(value, float):
        return _format_float(value)
    if isinstance(value, str):
        return _MIDNIGHT.sub(r"\1", value)
    return str(value)


def _format_float(value: float) -> str:
    if not math.isfinite(value):
        return ""
    exact = Decimal(repr(value))
    if abs(value) >= _THOUSAND:
        return f"{exact.quantize(Decimal(1), rounding=ROUND_HALF_UP):,}"
    if abs(value) >= 1 or value == 0:
        rounded = exact.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    else:
        rounded = _SIGNIFICANT.create_decimal(exact)
    text = format(rounded, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text
