"""
Shared pytest fixtures for the Scrapyard test suite.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pandas as pd


# ---------------------------------------------------------------------------
# Sample product data
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_product() -> Dict[str, Any]:
    return {
        "name": "Front Brake Disc for Toyota Corolla 2018",
        "url": "https://egycarparts.com/products/brake-disc-corolla",
        "price": 450.0,
        "raw_price": "450.00",
        "vendor": "Brembo",
        "part_number": "09-A785C",
        "image_url": "https://cdn.shopify.com/s/files/1/brake_disc.jpg",
        "stock_status": "in_stock",
        "category": "Brakes",
        "source": "egycarparts",
        "description": "High quality front brake disc compatible with Toyota Corolla 2016-2020.",
        "specifications": {"Material": "Cast Iron", "Diameter": "256mm"},
        "compatibility_text": "Toyota Corolla 2016-2020",
        "oem_references": ["43512-12340"],
        "variants": [],
        "data_source": "shopify_products_json",
    }


@pytest.fixture
def arabic_product() -> Dict[str, Any]:
    return {
        "name": "وسادات الفرامل الأمامية لتويوتا كورولا",
        "url": "https://egycarparts.com/products/brake-pads-corolla-ar",
        "price": 320.0,
        "raw_price": "320",
        "vendor": "ATE",
        "part_number": "13-0460-9738-2",
        "image_url": "",
        "stock_status": "in_stock",
        "category": "وسادات الفرامل",
        "source": "egycarparts",
        "description": "فحمات فرامل أمامية عالية الجودة مناسبة لسيارة تويوتا كورولا.",
        "specifications": {},
        "compatibility_text": "",
        "oem_references": [],
        "variants": [],
        "data_source": "shopify_products_json",
    }


@pytest.fixture
def product_list(sample_product, arabic_product) -> List[Dict[str, Any]]:
    products = []
    for i in range(25):
        p = dict(sample_product)
        p["url"] = f"https://egycarparts.com/products/item-{i}"
        p["price"] = float(100 + i * 10)
        products.append(p)
    products.append(arabic_product)
    return products


@pytest.fixture
def invalid_product() -> Dict[str, Any]:
    """A product that fails QA validation (missing name and URL)."""
    return {
        "name": "",
        "url": "",
        "price": None,
        "vendor": "",
        "part_number": "",
        "stock_status": "unknown",
    }


@pytest.fixture
def dirty_product() -> Dict[str, Any]:
    """A product with illegal XML characters and overlong fields."""
    return {
        "name": "Brake Disc\x00\x01\x08",         # null + control chars
        "url": "https://example.com/product/1",
        "price": float("nan"),                      # NaN price
        "description": "A" * 40000,                # over 32K chars
        "specifications": {"key": "value"},          # dict → should become JSON string
        "oem_references": ["REF1", "REF2"],          # list → should become JSON string
        "stock_status": "in_stock",
        "vendor": "Brand",
        "part_number": "PN-001",
    }


# ---------------------------------------------------------------------------
# Temporary output directory
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_output(tmp_path: Path) -> Path:
    out = tmp_path / "output"
    out.mkdir()
    return out


# ---------------------------------------------------------------------------
# Mock site config
# ---------------------------------------------------------------------------

@pytest.fixture
def shopify_site_config() -> Dict[str, Any]:
    return {
        "site_id": "test_site",
        "display_name": "Test Site",
        "base_url": "https://test-shopify-store.com",
        "type": "shopify",
        "platform_type": "Shopify",
        "currency": "EGP",
        "engine": "http",
        "max_pages": 2,
        "max_retries": 2,
        "request_delay_min": 0.0,
        "request_delay_max": 0.0,
        "timeout": 10,
        "categories_selector": 'a[href*="/collections/"]',
        "category_link_patterns": ["/collections/"],
        "product_link_patterns": ["/products/"],
    }
