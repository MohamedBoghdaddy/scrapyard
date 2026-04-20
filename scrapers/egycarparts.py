"""
Scraper for EgyCarParts (Shopify-based store).

Strategy:
  1. Parse category links from the main navigation.
  2. For each category, paginate through product listing pages.
     - Supports start_page for checkpoint resume.
  3. For product detail pages, attempt to extract the embedded Shopify JSON
     (`var meta = {...}`) first; fall back to HTML parsing if absent.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

import aiohttp
from bs4 import BeautifulSoup

from .base import BaseScraper
from .utils import (
    clean_price,
    clean_part_number,
    extract_shopify_product_json,
    get_headers,
    random_delay,
)
from utils.proxies import ProxyManager

logger = logging.getLogger(__name__)


class EgyCarPartsScraper(BaseScraper):
    """Async scraper for egycarparts.com (Shopify)."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._session: Optional[aiohttp.ClientSession] = None
        self._proxy_manager = ProxyManager()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False, limit=10)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=get_headers(),
        )
        logger.info("EgyCarParts session initialised")

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("EgyCarParts session closed")

    # ------------------------------------------------------------------
    # Internal fetch with retry + exponential backoff
    # ------------------------------------------------------------------

    async def _fetch(
        self, url: str, *, as_json: bool = False
    ) -> tuple[Optional[Any], float]:
        """
        GET *url* with up to self.max_retries attempts.
        Returns (decoded_body, duration_seconds).
        Body is None on failure.
        """
        proxy = self._proxy_manager.get_proxy()
        t0 = time.monotonic()
        for attempt in range(1, self.max_retries + 1):
            try:
                async with self._session.get(
                    url,
                    headers=get_headers(),
                    proxy=proxy,
                    allow_redirects=True,
                ) as resp:
                    if resp.status == 429:
                        wait = 2 ** attempt * 5
                        logger.warning(
                            "Rate-limited on %s – sleeping %ds", url, wait
                        )
                        await asyncio.sleep(wait)
                        continue
                    resp.raise_for_status()
                    body = await resp.json(content_type=None) if as_json else await resp.text()
                    return body, time.monotonic() - t0
            except aiohttp.ClientError as exc:
                backoff = 2 ** attempt
                logger.warning(
                    "Attempt %d/%d failed for %s: %s – retrying in %ds",
                    attempt, self.max_retries, url, exc, backoff,
                )
                if proxy:
                    self._proxy_manager.mark_bad(proxy)
                    proxy = self._proxy_manager.get_proxy()
                await asyncio.sleep(backoff)
            except asyncio.TimeoutError:
                logger.warning("Timeout on %s (attempt %d)", url, attempt)
                await asyncio.sleep(2 ** attempt)

        logger.error("All retries exhausted for %s", url)
        return None, time.monotonic() - t0

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------

    async def scrape_categories(self) -> List[Dict[str, str]]:
        """Parse category links from the main site navigation."""
        html, _ = await self._fetch(self.base_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        selector = self.config.get("categories_selector", "nav ul li a")
        links = soup.select(selector)

        categories: List[Dict[str, str]] = []
        seen: set[str] = set()
        for tag in links:
            href = tag.get("href", "")
            name = tag.get_text(strip=True)
            if not href or not name:
                continue
            url = self._absolute_url(href)
            if url in seen or any(
                kw in url
                for kw in ("/account", "/cart", "/search", "/blogs", "/pages")
            ):
                continue
            seen.add(url)
            categories.append({"name": name, "url": url})
            logger.debug("Category found: %s -> %s", name, url)

        logger.info("Found %d categories", len(categories))
        return categories

    # ------------------------------------------------------------------
    # Product listing (one category, all pages)
    # ------------------------------------------------------------------

    async def scrape_products_from_category(
        self,
        category_url: str,
        category_name: Optional[str] = None,
        start_page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Paginate through a category and return all product stubs.
        *start_page* allows resuming from a checkpoint.
        """
        products: List[Dict[str, Any]] = []
        page_num = start_page

        # Build the URL for the first page to fetch
        page_url: Optional[str] = self._page_url(category_url, page_num)

        while page_url:
            logger.info("Scraping page %d: %s", page_num, page_url)
            html, _duration = await self._fetch(page_url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            container_sel = self.config.get(
                "product_container", "ul.product-grid li"
            )
            items = soup.select(container_sel)
            if not items:
                logger.debug("No product cards on %s", page_url)
                break

            for item in items:
                product = self._extract_listing_product(item, category_name)
                if product:
                    products.append(product)

            # Pagination
            next_sel = self.config.get("next_page", "a.pagination__next")
            next_tag = soup.select_one(next_sel)
            if next_tag and next_tag.get("href"):
                page_url = self._absolute_url(next_tag["href"])
                page_num += 1
                await random_delay(self.delay_min, self.delay_max)
            else:
                break

        logger.info(
            "Category '%s' yielded %d products",
            category_name or category_url,
            len(products),
        )
        return products

    def _page_url(self, base: str, page: int) -> str:
        """Construct a Shopify paginated URL (e.g. ?page=3)."""
        if page <= 1:
            return base
        # Preserve existing query params
        parts = list(urlparse(base))
        params = parse_qs(parts[4])
        params["page"] = [str(page)]
        parts[4] = urlencode({k: v[0] for k, v in params.items()})
        return urlunparse(parts)

    def _extract_listing_product(
        self, item: Any, category_name: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Extract lightweight product data from a listing card element."""
        link_sel = self.config.get("product_link", "a")
        title_sel = self.config.get("product_title", ".product-title, h3")
        price_sel = self.config.get("price_selector", ".price")
        vendor_sel = self.config.get("vendor_selector", ".vendor")

        link_tag = item.select_one(link_sel)
        if not link_tag:
            return None

        url = self._absolute_url(link_tag.get("href", ""))
        title_tag = item.select_one(title_sel)
        name = (
            title_tag.get_text(strip=True)
            if title_tag
            else link_tag.get_text(strip=True)
        )

        price_tag = item.select_one(price_sel)
        raw_price = price_tag.get_text(strip=True) if price_tag else ""

        vendor_tag = item.select_one(vendor_sel)
        vendor = vendor_tag.get_text(strip=True) if vendor_tag else ""

        img_tag = item.select_one("img")
        image_url = ""
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url

        return {
            "name": name,
            "url": url,
            "price": clean_price(raw_price),
            "raw_price": raw_price,
            "vendor": vendor,
            "part_number": "",
            "image_url": image_url,
            "stock_status": "",
            "category": category_name or "",
            "source": "egycarparts",
        }

    # ------------------------------------------------------------------
    # Product detail page
    # ------------------------------------------------------------------

    async def scrape_product_details(self, product_url: str) -> Dict[str, Any]:
        """
        Fetch a product page and return full details.
        Prioritises Shopify's embedded JSON; falls back to HTML parsing.
        """
        html, _duration = await self._fetch(product_url)
        if not html:
            return {"url": product_url, "error": "fetch_failed"}

        shopify_data = extract_shopify_product_json(html)
        if shopify_data:
            return self._parse_shopify_json(shopify_data, product_url)

        return self._parse_product_html(html, product_url)

    def _parse_shopify_json(
        self, data: Dict[str, Any], url: str
    ) -> Dict[str, Any]:
        """Map Shopify product JSON to the canonical schema."""
        variants = data.get("variants", [])
        first = variants[0] if variants else {}
        images = data.get("images", []) or data.get("media", [])
        image_url = ""
        if images:
            src = images[0].get("src") or images[0].get("original_src") or ""
            image_url = ("https:" + src) if src.startswith("//") else src

        specs: Dict[str, str] = {}
        for opt in data.get("options", []):
            specs[opt.get("name", "")] = ", ".join(opt.get("values", []))

        return {
            "url": url,
            "name": data.get("title", ""),
            "vendor": data.get("vendor", ""),
            "part_number": clean_part_number(
                (data.get("variants") or [{}])[0].get("sku", "")
            ),
            "price": clean_price(str(first.get("price", ""))),
            "raw_price": str(first.get("price", "")),
            "compare_at_price": clean_price(str(first.get("compare_at_price") or "")),
            "image_url": image_url,
            "stock_status": "in_stock" if first.get("available") else "out_of_stock",
            "description": _strip_html(data.get("body_html", "")),
            "specifications": specs,
            "variants": [
                {
                    "title": v.get("title"),
                    "sku": v.get("sku"),
                    "price": clean_price(str(v.get("price", ""))),
                    "available": v.get("available"),
                }
                for v in variants
            ],
            "tags": data.get("tags", []),
            "source": "egycarparts",
            "data_source": "shopify_json",
        }

    def _parse_product_html(self, html: str, url: str) -> Dict[str, Any]:
        """HTML fallback for product pages without embedded JSON."""
        soup = BeautifulSoup(html, "lxml")

        name = _text(soup, "h1.product-title, h1.product__title, h1")
        price_tag = soup.select_one(".price__current, .product__price, .price")
        raw_price = price_tag.get_text(strip=True) if price_tag else ""
        vendor = _text(soup, ".product__vendor, .vendor")
        sku = _text(soup, ".product__sku, .sku")
        description = _text(soup, ".product__description, #product-description")

        img = soup.select_one(".product__media img, .product-image img")
        image_url = ""
        if img:
            image_url = img.get("src") or img.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url

        specs: Dict[str, str] = {}
        for row in soup.select("table.product-specs tr, .specifications tr"):
            cells = row.select("td, th")
            if len(cells) >= 2:
                specs[cells[0].get_text(strip=True)] = cells[1].get_text(strip=True)

        stock_el = soup.select_one(".product__availability, .stock-status")
        stock_text = stock_el.get_text(strip=True).lower() if stock_el else ""
        stock_status = (
            "in_stock"
            if "in stock" in stock_text or "available" in stock_text
            else "out_of_stock"
            if "out of stock" in stock_text
            else "unknown"
        )

        return {
            "url": url,
            "name": name,
            "vendor": vendor,
            "part_number": clean_part_number(sku),
            "price": clean_price(raw_price),
            "raw_price": raw_price,
            "image_url": image_url,
            "stock_status": stock_status,
            "description": description,
            "specifications": specs,
            "variants": [],
            "source": "egycarparts",
            "data_source": "html_fallback",
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _text(soup: BeautifulSoup, selector: str) -> str:
    tag = soup.select_one(selector)
    return tag.get_text(strip=True) if tag else ""


def _strip_html(html_str: str) -> str:
    if not html_str:
        return ""
    return BeautifulSoup(html_str, "lxml").get_text(separator=" ", strip=True)
