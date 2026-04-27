"""
Tests for utils/data_sanitizer.py

Covers:
- Illegal XML characters removed from strings
- Cell values truncated at Excel limit
- dict / list / set converted to JSON strings
- float NaN / inf → None
- sanitize_dataframe cleans every column
- sanitize_products works on a list of dicts
"""
from __future__ import annotations

import json
import math
from typing import Any

import pandas as pd
import pytest

from utils.data_sanitizer import (
    sanitize_value,
    sanitize_dataframe,
    sanitize_products,
    sanitize_sheet_name,
    _MAX_CELL_CHARS,
)


# ---------------------------------------------------------------------------
# sanitize_value
# ---------------------------------------------------------------------------

def test_removes_null_bytes():
    assert "\x00" not in str(sanitize_value("hello\x00world"))


def test_removes_control_chars():
    bad = "text\x01\x02\x03\x08\x0e\x1f"
    result = sanitize_value(bad)
    assert result == "text"


def test_preserves_tab_lf_cr():
    """TAB (\x09), LF (\x0a), CR (\x0d) are legal in XML 1.0."""
    text = "line1\nline2\ttabbed\r"
    result = sanitize_value(text)
    assert result == text


def test_truncates_long_string():
    long = "x" * (_MAX_CELL_CHARS + 1000)
    result = sanitize_value(long)
    assert isinstance(result, str)
    assert len(result) <= _MAX_CELL_CHARS + 10  # allow for "..." suffix


def test_converts_dict_to_json():
    d = {"key": "value", "nested": [1, 2, 3]}
    result = sanitize_value(d)
    assert isinstance(result, str)
    parsed = json.loads(result)
    assert parsed["key"] == "value"


def test_converts_list_to_json():
    lst = ["a", "b", "c"]
    result = sanitize_value(lst)
    assert isinstance(result, str)
    assert json.loads(result) == lst


def test_converts_set_to_json():
    s = {"x", "y"}
    result = sanitize_value(s)
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    assert set(parsed) == s


def test_nan_becomes_none():
    assert sanitize_value(float("nan")) is None


def test_inf_becomes_none():
    assert sanitize_value(float("inf")) is None
    assert sanitize_value(float("-inf")) is None


def test_none_stays_none():
    assert sanitize_value(None) is None


def test_int_unchanged():
    assert sanitize_value(42) == 42


def test_bool_unchanged():
    assert sanitize_value(True) is True


# ---------------------------------------------------------------------------
# sanitize_dataframe
# ---------------------------------------------------------------------------

def test_sanitize_dataframe_cleans_all_columns(dirty_product):
    df = pd.DataFrame([dirty_product])
    clean = sanitize_dataframe(df)
    # No illegal characters in string columns
    for col in clean.select_dtypes(include="object").columns:
        for val in clean[col].dropna():
            assert "\x00" not in str(val)
            assert len(str(val)) <= _MAX_CELL_CHARS + 10


def test_sanitize_dataframe_handles_nan():
    df = pd.DataFrame([{"price": float("nan"), "name": "product"}])
    clean = sanitize_dataframe(df)
    assert clean["price"].iloc[0] is None


# ---------------------------------------------------------------------------
# sanitize_products
# ---------------------------------------------------------------------------

def test_sanitize_products_returns_same_length(product_list):
    result = sanitize_products(product_list)
    assert len(result) == len(product_list)


def test_sanitize_products_cleans_dirty(dirty_product):
    results = sanitize_products([dirty_product])
    assert len(results) == 1
    r = results[0]
    assert "\x00" not in str(r.get("name", ""))
    assert r.get("price") is None   # NaN → None


def test_sanitize_products_coerces_price():
    products = [{"name": "Part", "url": "https://x.com/p1", "price": "450"}]
    result = sanitize_products(products)
    assert result[0]["price"] == 450.0


# ---------------------------------------------------------------------------
# sanitize_sheet_name
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,expected_max_len", [
    ("products", 31),
    ("a" * 50, 31),
    ("sheet:with\\illegal?chars", 31),
])
def test_sanitize_sheet_name_length(name, expected_max_len):
    result = sanitize_sheet_name(name)
    assert len(result) <= expected_max_len


def test_sanitize_sheet_name_removes_illegal():
    assert ":" not in sanitize_sheet_name("my:sheet")
    assert "\\" not in sanitize_sheet_name("my\\sheet")
