"""
Scraper for Al Khaleeg auto parts (Arabic / WooCommerce-style site).

Uses Playwright for JavaScript-rendered content and handles bilingual
(Arabic/English) product data throughout.

Browserless pool: if PLAYWRIGHT_WS is set, connects to a remote Browserless
instance instead of launching a local Chromium browser.

Content blocking detection: page content is scanned for bot-detection patterns
after each navigation.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
)

from .base import BaseScraper
from .utils import (
    clean_part_number,
    clean_price,
    get_headers,
    normalise_arabic,
    random_delay,
)

logger = logging.getLogger(__name__)

NAV_TIMEOUT = 60_000
LOAD_TIMEOUT = 30_000


# ---------------------------------------------------------------------------
# Blocking detection
# ---------------------------------------------------------------------------

_BLOCK_PATTERNS = [
    re.compile(r"captcha|robot|challenge", re.I),
    re.compile(r"access denied|forbidden", re.I),
    re.compile(r"rate limit|too many requests", re.I),
    re.compile(r"please verify you are a human", re.I),
]


def _is_page_blocked(content: str) -> bool:
    """Return True when the page content matches a bot-detection pattern."""
    sample = content[:3000]
    return any(p.search(sample) for p in _BLOCK_PATTERNS)


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------


class AlKhaleegScraper(BaseScraper):
    """Playwright-powered scraper for Arabic car-parts sites."""

    def __init__(self, config: Dict[str, Any], metrics=None) -> None:
        super().__init__(config, metrics)
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._browserless_ws: str = os.environ.get("PLAYWRIGHT_WS", "")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        self._playwright = await async_playwright().start()

        if self._browserless_ws:
            logger.info(
                "Connecting to Browserless pool at %s", self._browserless_ws
            )
            self._browser = await self._playwright.chromium.connect(
                self._browserless_ws
            )
        else:
            logger.info("Launching local Chromium browser")
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )

        self._context = await self._browser.new_context(
            user_agent=get_headers()["User-Agent"],
            locale="ar-EG",
            extra_http_headers={"Accept-Language": "ar,en-US;q=0.9"},
        )
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("AlKhaleeg browser context ready")

    async def teardown(self) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("AlKhaleeg browser closed")

    # ------------------------------------------------------------------
    # Internal page helpers
    # ------------------------------------------------------------------

    async def _new_page(self) -> Page:
        page = await self._context.new_page()
        page.set_default_navigation_timeout(NAV_TIMEOUT)
        page.set_default_timeout(LOAD_TIMEOUT)
        return page

    async def _goto(self, page: Page, url: str) -> Tuple[bool, str]:
        """
        Navigate to *url* and wait for network idle.
        Returns (success, page_content).
        On blocking detection, logs a warning and returns (False, "").
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                await page.goto(url, wait_until="networkidle")
                content = await page.content()

                if _is_page_blocked(content):
                    logger.warning(
                        "Blocking pattern detected on %s (attempt %d)", url, attempt
                    )
                    if self._metrics:
                        self._metrics.record_request(
                            url=url, success=False, duration=0,
                            status=403, proxy=None, attempt=attempt, blocked=True,
                        )
                    await asyncio.sleep(2 ** attempt)
                    continue

                if self._metrics:
                    self._metrics.record_request(
                        url=url, success=True, duration=0,
                        status=200, proxy=None, attempt=attempt,
                    )
                return True, content

            except PlaywrightTimeout:
                logger.warning("Timeout on %s (attempt %d)", url, attempt)
                if self._metrics:
                    self._metrics.record_request(
                        url=url, success=False, duration=0,
                        status=408, proxy=None, attempt=attempt,
                    )
                await asyncio.sleep(2 ** attempt)
            except Exception as exc:
                logger.warning("Navigation error %s: %s", url, exc)
                await asyncio.sleep(2 ** attempt)

        return False, ""

    # ------------------------------------------------------------------
    # Categories
    # ------------------------------------------------------------------

    async def scrape_categories(self) -> List[Dict[str, str]]:
        """Extract brand/category links from the homepage navigation."""
        page = await self._new_page()
        try:
            ok, html = await self._goto(page, self.base_url)
            if not ok:
                logger.warning(
                    "Could not load %s – check base_url in config/sites.yaml",
                    self.base_url,
                )
                return []

            selector = self.config.get(
                "categories_selector", "div.menu-categories a, ul.brands-list a"
            )
            try:
                await page.wait_for_selector(selector, timeout=LOAD_TIMEOUT)
            except PlaywrightTimeout:
                logger.warning("Category selector '%s' not found on page", selector)

            html = await page.content()
        finally:
            await page.close()

        soup = BeautifulSoup(html, "lxml")
        categories: List[Dict[str, str]] = []
        seen: set[str] = set()
        for tag in soup.select(selector):
            href = tag.get("href", "")
            name = normalise_arabic(tag.get_text(strip=True))
            if not href or not name:
                continue
            url = self._absolute_url(href)
            if url not in seen:
                seen.add(url)
                categories.append({"name": name, "url": url})

        logger.info("Found %d categories", len(categories))
        return categories

    # ------------------------------------------------------------------
    # Product listing
    # ------------------------------------------------------------------

    async def scrape_products_from_category(
        self,
        category_url: str,
        category_name: Optional[str] = None,
        start_page: int = 1,
    ) -> List[Dict[str, Any]]:
        """Paginate through a category page (handles infinite scroll too)."""
        products: List[Dict[str, Any]] = []
        page = await self._new_page()
        current_url: Optional[str] = category_url
        page_num = start_page

        try:
            while current_url and page_num <= self.max_pages:
                logger.info("Scraping page %d: %s", page_num, current_url)
                ok, _ = await self._goto(page, current_url)
                if not ok:
                    break

                await self._scroll_to_bottom(page)
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")

                container_sel = self.config.get(
                    "product_container", "div.product-item, li.product"
                )
                items = soup.select(container_sel)
                if not items:
                    logger.debug("No products on %s", current_url)
                    break

                for item in items:
                    product = self._extract_listing_product(item, category_name)
                    if product:
                        products.append(product)

                next_sel = self.config.get("next_page", "a.next-page")
                next_tag = soup.select_one(next_sel)
                if next_tag and next_tag.get("href"):
                    current_url = self._absolute_url(next_tag["href"])
                    page_num += 1
                    await random_delay(self.delay_min, self.delay_max)
                else:
                    break
        finally:
            await page.close()

        logger.info(
            "Category '%s' yielded %d products",
            category_name or category_url, len(products),
        )
        return products

    async def _scroll_to_bottom(self, page: Page) -> None:
        """Scroll incrementally to trigger lazy loading / infinite scroll."""
        prev_height = 0
        for _ in range(10):
            height = await page.evaluate("document.body.scrollHeight")
            if height == prev_height:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(1.0)
            prev_height = height

    def _extract_listing_product(
        self, item: Any, category_name: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Extract product data from a single listing card."""
        link_sel = self.config.get("product_link", "a.product-img")
        title_sel = self.config.get("product_title", "h4.product-name")
        price_sel = self.config.get("price_selector", "span.price")
        vendor_sel = self.config.get("vendor_selector", "span.brand-name")

        link_tag = item.select_one(link_sel) or item.select_one("a")
        if not link_tag:
            return None

        url = self._absolute_url(link_tag.get("href", ""))
        title_tag = item.select_one(title_sel)
        name = normalise_arabic(
            title_tag.get_text(strip=True) if title_tag
            else link_tag.get("title", "") or link_tag.get_text(strip=True)
        )

        price_tag = item.select_one(price_sel)
        raw_price = price_tag.get_text(strip=True) if price_tag else ""

        vendor_tag = item.select_one(vendor_sel)
        vendor = normalise_arabic(vendor_tag.get_text(strip=True)) if vendor_tag else ""

        img_tag = item.select_one("img")
        image_url = ""
        if img_tag:
            image_url = (
                img_tag.get("data-src")
                or img_tag.get("data-lazy")
                or img_tag.get("src")
                or ""
            )
            if image_url.startswith("//"):
                image_url = "https:" + image_url

        stock_text = item.get_text().lower()
        if any(w in stock_text for w in ("out of stock", "نفد", "غير متاح")):
            stock_status = "out_of_stock"
        elif any(w in stock_text for w in ("in stock", "متاح", "موجود")):
            stock_status = "in_stock"
        else:
            stock_status = "unknown"

        return {
            "name": name,
            "url": url,
            "price": clean_price(raw_price),
            "raw_price": raw_price,
            "vendor": vendor,
            "part_number": "",
            "image_url": image_url,
            "stock_status": stock_status,
            "category": category_name or "",
            "source": "alkhaleeg",
        }

    # ------------------------------------------------------------------
    # Product detail page
    # ------------------------------------------------------------------

    async def scrape_product_details(self, product_url: str) -> Dict[str, Any]:
        """Scrape a full product detail page using Playwright."""
        page = await self._new_page()
        try:
            ok, _ = await self._goto(page, product_url)
            if not ok:
                return {"url": product_url, "error": "fetch_failed"}
            await self._scroll_to_bottom(page)
            html = await page.content()
        finally:
            await page.close()

        return self._parse_product_html(html, product_url)

    def _parse_product_html(self, html: str, url: str) -> Dict[str, Any]:
        """Parse the rendered product page HTML."""
        soup = BeautifulSoup(html, "lxml")

        name = normalise_arabic(_text(soup, "h1.product-title, h1"))
        price_tag = soup.select_one("span.price, .product-price")
        raw_price = price_tag.get_text(strip=True) if price_tag else ""

        vendor_tag = soup.select_one("span.brand-name, .vendor")
        vendor = normalise_arabic(vendor_tag.get_text(strip=True)) if vendor_tag else ""

        sku_tag = soup.select_one(".sku, .part-number")
        sku = sku_tag.get_text(strip=True) if sku_tag else ""

        desc_tag = soup.select_one(".product-description, #product-desc, .description")
        description = (
            normalise_arabic(desc_tag.get_text(separator=" ", strip=True))
            if desc_tag
            else ""
        )

        specs: Dict[str, str] = {}
        for row in soup.select("table.specs tr, .product-specs tr, .spec-table tr"):
            cells = row.select("td, th")
            if len(cells) >= 2:
                key = normalise_arabic(cells[0].get_text(strip=True))
                val = normalise_arabic(cells[1].get_text(strip=True))
                if key:
                    specs[key] = val

        img_tag = soup.select_one(
            ".product-image img, .main-image img, img.product-img"
        )
        image_url = ""
        if img_tag:
            image_url = img_tag.get("src") or img_tag.get("data-src") or ""
            if image_url.startswith("//"):
                image_url = "https:" + image_url

        stock_text = soup.get_text().lower()
        if any(w in stock_text for w in ("out of stock", "نفد", "غير متاح")):
            stock_status = "out_of_stock"
        elif any(w in stock_text for w in ("in stock", "متاح", "موجود", "add to cart")):
            stock_status = "in_stock"
        else:
            stock_status = "unknown"

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
            "source": "alkhaleeg",
            "data_source": "playwright_html",
        }


def _text(soup: BeautifulSoup, selector: str) -> str:
    tag = soup.select_one(selector)
    return tag.get_text(strip=True) if tag else ""
