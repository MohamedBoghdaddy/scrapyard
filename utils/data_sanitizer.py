"""
Data sanitizer — make every product dict and DataFrame cell Excel-safe.

Problems this module prevents:
  • lxml.etree.SerialisationError   – caused by illegal XML control characters
  • openpyxl cell overflow           – cells exceeding 32,767 chars crash Excel
  • ValueError from mixed types      – NaN / inf / None in numeric columns
  • Unserializable objects           – dicts / lists / sets stored in cells

Usage::

    from utils.data_sanitizer import sanitize_products, sanitize_value

    clean = sanitize_products(raw_products)
"""
from __future__ import annotations

import json
import logging
import math
import re
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

# Excel cell limit (openpyxl enforces 32,767)
_MAX_CELL_CHARS = 32_000          # leave 767 chars headroom

# Control characters illegal in XML 1.0 (except TAB \x09, LF \x0a, CR \x0d)
# Also strip Unicode non-characters U+FFFE and U+FFFF
_ILLEGAL_XML_RE = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f￾￿\ud800-\udfff]"
)

# Surrogate pairs that slip through on Windows
_SURROGATE_RE = re.compile(r"[\ud800-\udfff]", re.UNICODE)


# ---------------------------------------------------------------------------
# Single-value sanitizer
# ---------------------------------------------------------------------------

def sanitize_value(value: Any, *, max_length: int = _MAX_CELL_CHARS) -> Any:
    """
    Return a scalar that is safe to write into an Excel cell.

    • dicts / lists  → compact JSON string
    • float NaN/inf  → None  (rendered as empty cell)
    • str            → illegal chars stripped, truncated to *max_length*
    • everything else → unchanged (int, bool, date, None)
    """
    # Dicts and lists → JSON string
    if isinstance(value, (dict, list, set, tuple)):
        try:
            value = json.dumps(
                value if not isinstance(value, (set, tuple)) else list(value),
                ensure_ascii=False,
                default=str,
            )
        except Exception:
            value = str(value)

    # Float NaN / inf → None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None

    # String cleanup
    if isinstance(value, str):
        # Remove illegal XML characters
        value = _ILLEGAL_XML_RE.sub("", value)
        value = _SURROGATE_RE.sub("", value)

        # Truncate to Excel cell limit
        if len(value) > max_length:
            value = value[: max_length - 4] + " ..."
            logger.debug("Truncated cell value to %d characters", max_length)

    return value


# ---------------------------------------------------------------------------
# DataFrame sanitizer
# ---------------------------------------------------------------------------

def sanitize_dataframe(
    df: pd.DataFrame,
    *,
    max_cell_chars: int = _MAX_CELL_CHARS,
) -> pd.DataFrame:
    """
    Return a copy of *df* with every cell passed through sanitize_value.

    Column names are also sanitized and limited to 31 characters (Excel
    sheet-name limit is shared with column names in some contexts).
    """
    result = df.copy()

    # Sanitize column names (Excel sheet names limited to 31 chars)
    result.columns = [
        _ILLEGAL_XML_RE.sub("", str(c))[:31] for c in result.columns
    ]

    for col in result.columns:
        result[col] = result[col].apply(
            lambda v: sanitize_value(v, max_length=max_cell_chars)
        )

    return result


# ---------------------------------------------------------------------------
# Product list sanitizer
# ---------------------------------------------------------------------------

_EXPECTED_TYPES: Dict[str, type] = {
    "price":            float,
    "compare_at_price": float,
    "price_egp":        float,
    "avg_price_egp":    float,
    "min_price_egp":    float,
    "max_price_egp":    float,
    "vendor_count":     int,
}


def sanitize_products(
    products: List[Dict[str, Any]],
    *,
    max_cell_chars: int = _MAX_CELL_CHARS,
) -> List[Dict[str, Any]]:
    """
    Return a new list of product dicts where every value is Excel-safe.

    Numeric fields are coerced to their expected types where possible.
    """
    cleaned: List[Dict[str, Any]] = []
    for product in products:
        row: Dict[str, Any] = {}
        for key, val in product.items():
            # Coerce expected numeric fields
            if key in _EXPECTED_TYPES and val is not None:
                try:
                    val = _EXPECTED_TYPES[key](val)
                except (TypeError, ValueError):
                    val = None

            row[key] = sanitize_value(val, max_length=max_cell_chars)
        cleaned.append(row)
    return cleaned


# ---------------------------------------------------------------------------
# Sheet-name sanitizer
# ---------------------------------------------------------------------------

_SHEET_ILLEGAL_RE = re.compile(r"[:\\/?*\[\]]")

def sanitize_sheet_name(name: str, max_len: int = 31) -> str:
    """Make a string a valid Excel sheet name (≤31 chars, no illegal chars)."""
    cleaned = _SHEET_ILLEGAL_RE.sub("_", str(name))
    cleaned = _ILLEGAL_XML_RE.sub("", cleaned).strip()
    return cleaned[:max_len] or "Sheet"
