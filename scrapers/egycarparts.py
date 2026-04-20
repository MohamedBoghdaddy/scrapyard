"""
Scraper for EgyCarParts (Shopify-based store).

Strategy:
  1. Parse category links from the main navigation.
  2. For each category, paginate through product listing pages.
     Supports start_page for checkpoint resume.
  3. For product detail pages, extract embedded Shopify JSON first;
     fall back to HTML parsing, then LLM extraction if --llm is enabled.
  4. Jina AI is used as a last resort if all HTTP retries are exhausted.

Brotli fix: ClientSession is created with auto_decompress=True.
Blocking detection: response body is scanned for bot-detection patterns.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple
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
from utils.cleaners import first_match
from utils.jina import fetch_via_jina
from utils.proxies import ProxyManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Blocking detection
# ---------------------------------------------------------------------------

_BLOCK_PATTERNS = [
    (re.compile(r"captcha|robot|challenge", re.I), 403),
    (re.compile(r"access denied|forbidden", re.I), 403),
    (re.compile(r"rate limit|too many requests", re.I), 429),
    (re.compile(r"please verify you are a human", re.I), 403),
]


class BlockedError(Exception):
    """Raised when a response body matches a bot-detection pattern."""

    def __init__(self, url: str, status: int) -> None:
        super().__init__(f"Blocked ({status}) at {url}")
        self.url = url
        self.status = status


def _check_content_blocking(body: str) -> Tuple[bool, int]:
    """Return (is_blocked, inferred_status) by scanning the response body."""
    sample = body[:2000]
    for pattern, status in _BLOCK_PATTERNS:
        if pattern.search(sample):
            return True, status
    return False, 0


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------


class EgyCarPartsScraper(BaseScraper):
    """Async scraper for egycarparts.com (Shopify)."""

    def __init__(self, config: Dict[str, Any], metrics=None) -> None:
        super().__init__(config, metrics)
        self._session: Optional[aiohttp.ClientSession] = None
        self._proxy_manager = ProxyManager()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        connector = aiohttp.TCPConnector(ssl=False, limit=10)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        # auto_decompress=True handles Brotli (br) encoding transparently
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=get_headers(),
            auto_decompress=True,
        )
        logger.info("EgyCarParts session initialised (auto_decompress=True)")

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("EgyCarParts session closed")

    # ------------------------------------------------------------------
    # Internal fetch with retry, blocking detection, and Jina fallback
    # ------------------------------------------------------------------

    async def _fetch(
        self, url: str, *, as_json: bool = False
    ) -> Tuple[Optional[Any], float]:
        """
        GET *url* with up to self.max_retries attempts.

        Attempt order:
          1. aiohttp with auto_decompress=True (handles Brotli).
          2. On Brotli/decode error, retry with Accept-Encoding: gzip, deflate.
          3. On BlockedError, rotate proxy and retry.
          4. After all retries, fall back to Jina AI if JINA_API_KEY is set.

        Returns (decoded_body, duration_seconds). Body is None on total failure.
        """
        proxy = self._proxy_manager.get_proxy()
        t0 = time.monotonic()

        for attempt in range(1, self.max_retries + 1):
            headers = get_headers()
            try:
                async with self._session.get(
                    url,
                    headers=headers,
                    proxy=proxy,
                    allow_redirects=True,
                ) as resp:
                    duration = time.monotonic() - t0
                    if resp.status == 429:
                        wait = 2 ** attempt * 5
                        logger.warning("Rate-limited on %s – sleeping %ds", url, wait)
                        if self._metrics:
                            self._metrics.record_request(
                                url=url, success=False, duration=duration,
                                status=429, proxy=proxy, attempt=attempt,
                            )
                        await asyncio.sleep(wait)
                        continue

                    resp.raise_for_status()
                    body = (
                        await resp.json(content_type=None)
                        if as_json
                        else await resp.text()
                    )

                    if not as_json:
                        blocked, block_status = _check_content_blocking(body)
                        if blocked:
                            logger.warning("Blocking pattern detected on %s", url)
                            if self._metrics:
                                self._metrics.record_request(
                                    url=url, success=False, duration=duration,
                                    status=block_status, proxy=proxy,
                                    attempt=attempt, blocked=True,
                                )
                            raise BlockedError(url, block_status)

                    if self._metrics:
                        self._metrics.record_request(
                            url=url, success=True, duration=duration,
                            status=resp.status, proxy=proxy, attempt=attempt,
                        )
                    return body, duration

            except BlockedError:
                if proxy:
                    self._proxy_manager.mark_bad(proxy)
                proxy = self._proxy_manager.get_proxy()
                await asyncio.sleep(2 ** attempt)

            except (UnicodeDecodeError, aiohttp.ContentTypeError):
                # Brotli decode failure fallback: retry with conservative encoding
                logger.warning(
                    "Decode error on %s (attempt %d) – retrying with gzip/deflate only",
                    url, attempt,
                )
                headers = {
                    **headers,
                    "Accept-Encoding": "gzip, deflate",
                }
                try:
                    async with self._session.get(
                        url, headers=headers, proxy=proxy, allow_redirects=True
                    ) as resp2:
                        body = await resp2.text(errors="replace")
                        duration = time.monotonic() - t0
                        if self._metrics:
                            self._metrics.record_request(
                                url=url, success=True, duration=duration,
                                status=resp2.status, proxy=proxy, attempt=attempt,
                            )
                        return body, duration
                except Exception:
                    pass
                await asyncio.sleep(2 ** attempt)

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

        duration = time.monotonic() - t0
        logger.error("All retries exhausted for %s", url)

        # Jina AI fallback
        jina_content = await fetch_via_jina(url)
        if jina_content:
            if self._metrics:
                self._metrics.record_jina_fallback()
            return jina_content, time.monotonic() - t0

        if self._metrics:
            self._metrics.record_request(
                url=url, success=False, duration=duration,
                status=0, proxy=proxy, attempt=self.max_retries,
            )
        return None, duration

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
            logger.debug("Category: %s -> %s", name, url)

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
        """Paginate through a category and return all product stubs."""
        products: List[Dict[str, Any]] = []
        page_num = start_page
        page_url: Optional[str] = self._page_url(category_url, page_num)

        while page_url and page_num <= self.max_pages:
            logger.info("Scraping page %d: %s", page_num, page_url)
            html, _ = await self._fetch(page_url)
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
            category_name or category_url, len(products),
        )
        return products

    def _page_url(self, base: str, page: int) -> str:
        """Construct a Shopify paginated URL (e.g. ?page=3)."""
        if page <= 1:
            return base
        parts = list(urlparse(base))
        params = parse_qs(parts[4])
        params["page"] = [str(page)]
        parts[4] = urlencode({k: v[0] for k, v in params.items()})
        return urlunparse(parts)

    def _extract_listing_product(
        self, item: Any, category_name: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Extract lightweight product data from a listing card using first_match."""
        title_sel = self.config.get("product_title", ".product-title, h3")
        price_sel = self.config.get("price_selector", ".price")
        vendor_sel = self.config.get("vendor_selector", ".vendor")
        link_sel = self.config.get("product_link", "a")

        link_tag = item.select_one(link_sel)
        if not link_tag:
            return None

        url = self._absolute_url(link_tag.get("href", ""))

        name = first_match(item, [
            (title_sel, None),
            ("h3", None),
            ("h2", None),
        ]) or link_tag.get_text(strip=True)

        raw_price = first_match(item, [
            (price_sel, None),
            (".money", None),
            ("[class*='price']", None),
        ]) or ""

        vendor = first_match(item, [
            (vendor_sel, None),
            (".brand", None),
        ]) or ""

        image_url = ""
        img_tag = item.select_one("img")
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

        Priority:
          1. Shopify embedded JSON (most accurate).
          2. HTML parsing with CSS selector cascade.
          3. LLM extraction (if --llm is enabled and selectors fail).
        """
        html, _ = await self._fetch(product_url)
        if not html:
            return {"url": product_url, "error": "fetch_failed"}

        shopify_data = extract_shopify_product_json(html)
        if shopify_data:
            return self._parse_shopify_json(shopify_data, product_url)

        result = self._parse_product_html(html, product_url)

        # LLM fallback when CSS selectors yielded no name
        if not result.get("name") and self.llm_enabled:
            logger.info("CSS selectors empty – trying LLM extraction for %s", product_url)
            from utils.llm_extractor import extract_with_llm
            llm_data = await extract_with_llm(html)
            if llm_data:
                if self._metrics:
                    self._metrics.record_llm_extraction()
                result.update({
                    "name": llm_data.get("name", result.get("name", "")),
                    "vendor": llm_data.get("brand", result.get("vendor", "")),
                    "part_number": llm_data.get("part_number", result.get("part_number", "")),
                    "price": llm_data.get("price") or result.get("price"),
                    "description": llm_data.get("description", result.get("description", "")),
                    "data_source": "llm_extraction",
                })

        return result

    def _parse_shopify_json(self, data: Dict[str, Any], url: str) -> Dict[str, Any]:
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

        name = first_match(soup, [
            ("h1.product-title", None),
            ("h1.product__title", None),
            ("h1", None),
            ('meta[property="og:title"]', "content"),
        ]) or ""

        raw_price = first_match(soup, [
            (".price__current", None),
            (".product__price", None),
            (".price", None),
            ('meta[property="og:price:amount"]', "content"),
        ]) or ""

        vendor = first_match(soup, [
            (".product__vendor", None),
            (".vendor", None),
        ]) or ""

        sku = first_match(soup, [
            (".product__sku", None),
            (".sku", None),
        ]) or ""

        description = first_match(soup, [
            (".product__description", None),
            ("#product-description", None),
            (".description", None),
        ]) or ""

        image_url = first_match(soup, [
            (".product__media img", "src"),
            (".product-image img", "src"),
            ('meta[property="og:image"]', "content"),
        ]) or ""
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
# Module helpers
# ---------------------------------------------------------------------------

def _strip_html(html_str: str) -> str:
    if not html_str:
        return ""
    return BeautifulSoup(html_str, "lxml").get_text(separator=" ", strip=True)
