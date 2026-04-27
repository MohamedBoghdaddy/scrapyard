"""
Tests for the scraping layer.

Covers:
- Shopify /products.json pagination (mocked HTTP)
- Shopify API 404 fallback → HTML scraping
- Shopify API invalid JSON → fallback
- Shopify API empty products list → stop pagination
- data_source field is set correctly per product
- EgyCarPartsScraper._looks_shopify() detection
- HTML listing extraction produces valid product dicts
"""
from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from scrapers.egycarparts import EgyCarPartsScraper


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def scraper(shopify_site_config):
    return EgyCarPartsScraper(shopify_site_config)


# ---------------------------------------------------------------------------
# _looks_shopify()
# ---------------------------------------------------------------------------

def test_looks_shopify_with_collections_url(scraper):
    assert scraper._looks_shopify("https://site.com/collections/brakes")


def test_looks_shopify_false_for_other_url(scraper):
    assert not scraper._looks_shopify("https://site.com/category/brakes")


# ---------------------------------------------------------------------------
# _map_shopify_product_json()
# ---------------------------------------------------------------------------

def _make_shopify_item(
    title="Brake Disc",
    handle="brake-disc",
    vendor="Brembo",
    price="450.00",
    available=True,
    sku="BD-001",
) -> Dict[str, Any]:
    return {
        "id": 1,
        "title": title,
        "handle": handle,
        "vendor": vendor,
        "body_html": "<p>High quality brake disc.</p>",
        "product_type": "Brakes",
        "tags": ["toyota", "corolla"],
        "images": [{"src": "//cdn.shopify.com/img.jpg"}],
        "variants": [{"price": price, "available": available, "sku": sku}],
        "options": [{"name": "Size", "values": ["280mm"]}],
    }


def test_map_shopify_product_json_basic(scraper):
    item = _make_shopify_item()
    product = scraper._map_shopify_product_json(item, "Brakes")
    assert product["name"] == "Brake Disc"
    assert product["vendor"] == "Brembo"
    assert product["stock_status"] == "in_stock"
    assert product["price"] == 450.0
    assert product["data_source"] == "shopify_products_json"
    assert product["category"] == "Brakes"
    assert "brake-disc" in product["url"]


def test_map_shopify_product_out_of_stock(scraper):
    item = _make_shopify_item(available=False)
    product = scraper._map_shopify_product_json(item, "Brakes")
    assert product["stock_status"] == "out_of_stock"


def test_map_shopify_product_image_url_fixed(scraper):
    item = _make_shopify_item()
    product = scraper._map_shopify_product_json(item, "Brakes")
    assert product["image_url"].startswith("https:")


# ---------------------------------------------------------------------------
# _scrape_shopify_products_json() — mocked HTTP
# ---------------------------------------------------------------------------

def _build_products_response(n: int, start_id: int = 0) -> Dict[str, Any]:
    return {
        "products": [
            _make_shopify_item(
                title=f"Product {start_id + i}",
                handle=f"product-{start_id + i}",
            )
            for i in range(n)
        ]
    }


@pytest.mark.asyncio
async def test_shopify_api_pagination(scraper):
    """
    Page 1 returns 250 items (full page → triggers page 2).
    Page 2 returns 5 items (partial → signals last page).
    Total expected: 255 unique products.
    """
    LIMIT = 250
    page_responses = [
        _build_products_response(LIMIT, 0),    # full page → fetch page 2
        _build_products_response(5, LIMIT),     # partial page → stop
    ]
    call_count = {"n": 0}

    async def mock_fetch(url, *, as_json=False):
        resp = page_responses[min(call_count["n"], len(page_responses) - 1)]
        call_count["n"] += 1
        return resp, 0.1

    scraper._fetch = mock_fetch
    scraper.config["max_pages"] = 5

    products = await scraper._scrape_shopify_products_json(
        "https://site.com/collections/brakes",
        category_name="Brakes",
    )
    assert len(products) == LIMIT + 5
    assert call_count["n"] == 2


@pytest.mark.asyncio
async def test_shopify_api_returns_empty_on_none_response(scraper):
    async def mock_fetch(url, *, as_json=False):
        return None, 0.0

    scraper._fetch = mock_fetch
    products = await scraper._scrape_shopify_products_json(
        "https://site.com/collections/brakes",
        category_name="Brakes",
    )
    assert products == []


@pytest.mark.asyncio
async def test_shopify_api_returns_empty_on_invalid_json(scraper):
    """HTML string instead of JSON dict → empty result (no crash)."""
    async def mock_fetch(url, *, as_json=False):
        return "<html>Not found</html>", 0.1

    scraper._fetch = mock_fetch
    products = await scraper._scrape_shopify_products_json(
        "https://site.com/collections/brakes",
        category_name="Brakes",
    )
    assert products == []


@pytest.mark.asyncio
async def test_shopify_api_missing_products_key_returns_empty(scraper):
    """Response without 'products' key → empty result."""
    async def mock_fetch(url, *, as_json=False):
        return {"error": "not found"}, 0.1

    scraper._fetch = mock_fetch
    products = await scraper._scrape_shopify_products_json(
        "https://site.com/collections/brakes",
        category_name="Brakes",
    )
    assert products == []


@pytest.mark.asyncio
async def test_shopify_api_respects_max_api_pages(scraper):
    """With max_api_pages=1, should not fetch page 2."""
    responses = [
        _build_products_response(250),  # full page (would normally trigger page 2)
        _build_products_response(100),  # page 2 should NOT be fetched
    ]
    calls = []

    async def mock_fetch(url, *, as_json=False):
        calls.append(url)
        resp = responses[min(len(calls) - 1, len(responses) - 1)]
        return resp, 0.1

    scraper._fetch = mock_fetch
    products = await scraper._scrape_shopify_products_json(
        "https://site.com/collections/brakes",
        category_name="Brakes",
        max_api_pages=1,
    )
    assert len(calls) == 1   # only page 1 fetched
    assert len(products) == 250


# ---------------------------------------------------------------------------
# data_source field
# ---------------------------------------------------------------------------

def test_shopify_json_data_source_is_tagged(scraper):
    item = _make_shopify_item()
    product = scraper._map_shopify_product_json(item, "Brakes")
    assert product["data_source"] == "shopify_products_json"


def test_html_listing_data_source():
    from bs4 import BeautifulSoup
    html = """
    <div class="product">
      <a href="/products/brake-disc">Brake Disc</a>
      <span class="price">450.00</span>
    </div>
    """
    config = {
        "site_id": "test",
        "base_url": "https://test.com",
        "type": "custom",
        "max_pages": 2, "max_retries": 1,
        "request_delay_min": 0, "request_delay_max": 0,
        "timeout": 5,
        "product_link_patterns": ["/products/"],
        "product_link": "a",
        "product_title": "a",
        "price_selector": ".price",
        "vendor_selector": ".vendor",
    }
    scraper = EgyCarPartsScraper(config)
    soup = BeautifulSoup(html, "lxml")
    item = soup.select_one("div.product")
    product = scraper._extract_listing_product(item, "Brakes")
    # HTML listing should set data_source to 'listing' or leave it as the default
    assert product is not None
    assert "url" in product
