"""
Tests for utils/quality_report.py

Covers:
- build_quality_report() with normal, empty, and NLP-enriched products
- Correct computation of coverage percentages
- Correct duplicate detection
- price_min / max / mean / median accuracy
- save_quality_report() writes valid JSON
- quality_report_to_dataframe() has metric and value columns
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest

from utils.quality_report import (
    build_quality_report,
    quality_report_to_dataframe,
    save_quality_report,
)


# ---------------------------------------------------------------------------
# Basic report
# ---------------------------------------------------------------------------

def test_build_quality_report_basic(product_list):
    report = build_quality_report(product_list)
    assert isinstance(report, dict)
    assert report["total_rows"] == len(product_list)
    assert 0 <= report["name_coverage_pct"] <= 100
    assert 0 <= report["price_coverage_pct"] <= 100


def test_build_quality_report_empty():
    report = build_quality_report([])
    assert report["total_rows"] == 0


def test_build_quality_report_all_fields_covered(sample_product):
    products = [sample_product] * 5
    report = build_quality_report(products)
    assert report["name_coverage_pct"] == 100.0
    assert report["price_coverage_pct"] == 100.0
    assert report["brand_coverage_pct"] == 100.0


def test_build_quality_report_missing_fields():
    products = [
        {"name": "A", "url": "https://x.com/1", "price": None, "vendor": ""},
        {"name": "B", "url": "https://x.com/2", "price": 100.0, "vendor": "Brand"},
    ]
    report = build_quality_report(products)
    assert report["missing_price"] == 1
    assert report["missing_brand"] == 1
    assert report["price_coverage_pct"] == 50.0


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def test_duplicate_url_detection():
    products = [
        {"name": "X", "url": "https://ex.com/p1", "price": 10.0},
        {"name": "Y", "url": "https://ex.com/p1", "price": 20.0},  # duplicate URL
        {"name": "Z", "url": "https://ex.com/p3", "price": 30.0},
    ]
    report = build_quality_report(products)
    assert report["duplicate_url_count"] == 1
    assert report["duplicate_rate_pct"] > 0


# ---------------------------------------------------------------------------
# Price statistics
# ---------------------------------------------------------------------------

def test_price_stats_correct():
    products = [
        {"name": f"P{i}", "url": f"https://x.com/{i}", "price": float(i * 100)}
        for i in range(1, 6)   # prices: 100, 200, 300, 400, 500
    ]
    report = build_quality_report(products)
    assert report["price_min"] == 100.0
    assert report["price_max"] == 500.0
    assert report["price_mean"] == 300.0
    assert report["price_median"] == 300.0


def test_price_stats_no_prices():
    products = [{"name": "X", "url": "https://x.com/1", "price": None}]
    report = build_quality_report(products)
    assert report["price_min"] is None
    assert report["price_max"] is None


# ---------------------------------------------------------------------------
# Distribution fields
# ---------------------------------------------------------------------------

def test_stock_distribution_in_report(product_list):
    report = build_quality_report(product_list)
    stock_dist = json.loads(report["stock_distribution"])
    assert isinstance(stock_dist, dict)


def test_language_distribution_in_nlp_report():
    products = [
        {"name": "Brake disc", "url": "https://x.com/1", "language": "en"},
        {"name": "قرص فرامل", "url": "https://x.com/2", "language": "ar"},
        {"name": "قرص فرامل 2", "url": "https://x.com/3", "language": "ar"},
    ]
    report = build_quality_report(products)
    lang_dist = json.loads(report["language_distribution"])
    assert lang_dist.get("ar") == 2
    assert lang_dist.get("en") == 1


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_save_quality_report(tmp_path, product_list):
    report = build_quality_report(product_list)
    path = save_quality_report(report, tmp_path / "quality_report.json")
    assert path.exists()
    with open(path, encoding="utf-8") as fh:
        loaded = json.load(fh)
    assert loaded["total_rows"] == report["total_rows"]


def test_quality_report_to_dataframe(product_list):
    report = build_quality_report(product_list)
    df = quality_report_to_dataframe(report)
    assert isinstance(df, pd.DataFrame)
    assert "metric" in df.columns
    assert "value" in df.columns
    assert len(df) == len(report)
