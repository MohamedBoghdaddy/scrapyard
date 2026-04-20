"""
Generic Playwright scraper for JavaScript-rendered automotive catalog sites.

This powers browser-driven sites such as:
  - Arabic/custom storefronts
  - Next.js catalogs
  - Wix shops
  - Widget-style catalog pages that need button pagination or fragment-only links
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
    async_playwright,
)

from .base import BaseScraper
from .utils import clean_part_number, clean_price, get_headers, normalise_arabic, random_delay
from utils.cleaners import first_match

logger = logging.getLogger(__name__)

NAV_TIMEOUT = 60_000
LOAD_TIMEOUT = 30_000

_BLOCK_PATTERNS = [
    re.compile(r"captcha", re.I),
    re.compile(r"access denied|forbidden", re.I),
    re.compile(r"rate limit|too many requests", re.I),
    re.compile(r"please verify you are a human", re.I),
    re.compile(r"attention required", re.I),
    re.compile(r"cf-browser-verification|__cf_chl_|challenge-platform", re.I),
]


def _is_page_blocked(content: str) -> bool:
    sample = BeautifulSoup(content[:15000], "lxml").get_text(" ", strip=True)[:3000]
    return any(pattern.search(sample) for pattern in _BLOCK_PATTERNS)


class AlKhaleegScraper(BaseScraper):
    """Config-driven Playwright scraper for JS-heavy catalog sites."""

    def __init__(self, config: Dict[str, Any], metrics=None) -> None:
        super().__init__(config, metrics)
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
        self._browserless_ws: str = os.environ.get("PLAYWRIGHT_WS", "")
        self.source = self.site_id

    async def setup(self) -> None:
        self._playwright = await async_playwright().start()

        if self._browserless_ws:
            logger.info("Connecting to Browserless pool at %s", self._browserless_ws)
            self._browser = await self._playwright.chromium.connect(self._browserless_ws)
        else:
            logger.info("Launching local Chromium browser")
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=["--disable-blink-features=AutomationControlled"],
            )

        self._context = await self._browser.new_context(
            user_agent=get_headers()["User-Agent"],
            locale=self.config.get("locale", "en-US"),
            ignore_https_errors=bool(self.config.get("ignore_ssl")),
            extra_http_headers={
                "Accept-Language": self.config.get("accept_language", "en-US,en;q=0.9,ar;q=0.8")
            },
        )
        await self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        logger.info("%s browser context ready", self.source)

    async def teardown(self) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        logger.info("%s browser closed", self.source)

    async def _new_page(self) -> Page:
        page = await self._context.new_page()
        page.set_default_navigation_timeout(NAV_TIMEOUT)
        page.set_default_timeout(LOAD_TIMEOUT)
        return page

    async def _goto(self, page: Page, url: str) -> Tuple[bool, str]:
        """
        Navigate to *url* using a configurable wait strategy.
        Returns (success, page_content).
        """
        wait_until = self.config.get("wait_until", "domcontentloaded")
        post_nav_delay_ms = int(self.config.get("post_nav_delay_ms", 2000))

        for attempt in range(1, self.max_retries + 1):
            try:
                await page.goto(url, wait_until=wait_until)
                if post_nav_delay_ms:
                    await page.wait_for_timeout(post_nav_delay_ms)
                content = await page.content()

                if _is_page_blocked(content):
                    logger.warning(
                        "Blocking pattern detected on %s (attempt %d)",
                        url,
                        attempt,
                    )
                    self._record_request(
                        url=url,
                        success=False,
                        duration=0,
                        status=403,
                        attempt=attempt,
                        blocked=True,
                    )
                    await asyncio.sleep(2 ** attempt)
                    continue

                self._record_request(
                    url=url,
                    success=True,
                    duration=0,
                    status=200,
                    attempt=attempt,
                )
                return True, content

            except PlaywrightTimeout:
                logger.warning("Timeout on %s (attempt %d)", url, attempt)
                self._record_request(
                    url=url,
                    success=False,
                    duration=0,
                    status=408,
                    attempt=attempt,
                )
                await asyncio.sleep(2 ** attempt)
            except Exception as exc:
                logger.warning("Navigation error %s: %s", url, exc)
                await asyncio.sleep(2 ** attempt)

        return False, ""

    async def scrape_categories(self) -> List[Dict[str, str]]:
        """Extract categories from config seeds or rendered page heuristics."""
        seeded = self._seed_categories()
        if seeded:
            logger.info("Using %d seeded categories for %s", len(seeded), self.source)
            return seeded

        page = await self._new_page()
        try:
            ok, html = await self._goto(page, self.base_url)
            if not ok:
                logger.warning("Could not load %s", self.base_url)
                return []

            selector = self.config.get(
                "categories_selector",
                "div.menu-categories a, ul.brands-list a, nav a",
            )
            try:
                await page.wait_for_selector(selector, timeout=LOAD_TIMEOUT // 2)
            except PlaywrightTimeout:
                pass

            html = await page.content()
        finally:
            await page.close()

        soup = BeautifulSoup(html, "lxml")
        links = soup.select(selector)
        if not links:
            links = self._discover_category_links(soup)

        categories: List[Dict[str, str]] = []
        seen: set[str] = set()
        for tag in links:
            href = tag.get("href", "")
            name = normalise_arabic(tag.get_text(" ", strip=True))
            if not href or not name:
                continue
            url = self._absolute_url(href)
            lower = url.lower()
            if url in seen or any(pattern in lower for pattern in self._category_excludes()):
                continue
            seen.add(url)
            categories.append({"name": name, "url": url})

        logger.info("Found %d categories for %s", len(categories), self.source)
        return categories

    async def scrape_products_from_category(
        self,
        category_url: str,
        category_name: Optional[str] = None,
        start_page: int = 1,
    ) -> List[Dict[str, Any]]:
        """Scrape a rendered category page, including button-driven pagination."""
        products: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        page = await self._new_page()

        try:
            ok, _ = await self._goto(page, category_url)
            if not ok:
                return []

            if self.config.get("pagination_button_selector"):
                page_num = 1
                while page_num <= self.max_pages:
                    if page_num < start_page:
                        clicked = await self._click_pagination_button(page, page_num + 1)
                        if not clicked:
                            break
                        page_num += 1
                        continue

                    logger.info("Scraping rendered page %d: %s", page_num, category_url)
                    page_products = await self._extract_products_from_rendered_page(
                        page,
                        category_name,
                    )
                    if not page_products:
                        break
                    for product in page_products:
                        product_url = product.get("url", "")
                        if product_url and product_url not in seen_urls:
                            seen_urls.add(product_url)
                            products.append(product)

                    clicked = await self._click_pagination_button(page, page_num + 1)
                    if not clicked:
                        break
                    page_num += 1
                    await random_delay(self.delay_min, self.delay_max)
            else:
                current_url: Optional[str] = category_url
                page_num = start_page

                while current_url and page_num <= self.max_pages:
                    if page_num > start_page:
                        ok, _ = await self._goto(page, current_url)
                        if not ok:
                            break

                    logger.info("Scraping page %d: %s", page_num, current_url)
                    page_products = await self._extract_products_from_rendered_page(
                        page,
                        category_name,
                    )
                    if not page_products:
                        break

                    for product in page_products:
                        product_url = product.get("url", "")
                        if product_url and product_url not in seen_urls:
                            seen_urls.add(product_url)
                            products.append(product)

                    html = await page.content()
                    soup = BeautifulSoup(html, "lxml")
                    next_sel = self.config.get("next_page", "a.next, a[rel='next']")
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
            category_name or category_url,
            len(products),
        )
        return products

    async def _extract_products_from_rendered_page(
        self, page: Page, category_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        container_sel = self.config.get("product_container", "div.product-item, li.product")
        wait_timeout_ms = int(self.config.get("product_wait_timeout_ms", 5000))
        if container_sel:
            try:
                await page.wait_for_selector(container_sel, timeout=wait_timeout_ms)
            except Exception:
                logger.debug(
                    "Product selector '%s' was not visible yet on %s",
                    container_sel,
                    category_name or self.source,
                )

        await self._scroll_to_bottom(page)
        html = await page.content()
        soup = BeautifulSoup(html, "lxml")
        products = self._extract_products_from_page(soup, category_name)
        if products:
            return products

        retry_delay_ms = int(self.config.get("empty_retry_delay_ms", 2000))
        if retry_delay_ms > 0:
            await page.wait_for_timeout(retry_delay_ms)
            await self._scroll_to_bottom(page)
            html = await page.content()
            soup = BeautifulSoup(html, "lxml")
            products = self._extract_products_from_page(soup, category_name)
            if products:
                return products

        pre_extract_click_selector = self.config.get("pre_extract_click_selector")
        if pre_extract_click_selector:
            try:
                await page.click(pre_extract_click_selector, timeout=5000)
                await page.wait_for_timeout(
                    int(self.config.get("post_click_delay_ms", 1500))
                )
                await self._scroll_to_bottom(page)
                html = await page.content()
                soup = BeautifulSoup(html, "lxml")
                return self._extract_products_from_page(soup, category_name)
            except Exception:
                return []

        return []

    async def _scroll_to_bottom(self, page: Page) -> None:
        prev_height = 0
        steps = int(self.config.get("scroll_steps", 8))
        delay_ms = int(self.config.get("scroll_delay_ms", 800))
        for _ in range(steps):
            height = await page.evaluate("document.body.scrollHeight")
            if height == prev_height:
                break
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(delay_ms)
            prev_height = height

    async def _click_pagination_button(self, page: Page, target_page: int) -> bool:
        selector = self.config.get("pagination_button_selector", "")
        if not selector:
            return False

        buttons = page.locator(selector)
        count = await buttons.count()
        for idx in range(count):
            button = buttons.nth(idx)
            try:
                text = (await button.inner_text()).strip()
            except Exception:
                continue
            if text != str(target_page):
                continue
            try:
                await button.click()
                await page.wait_for_timeout(int(self.config.get("post_click_delay_ms", 1500)))
                return True
            except Exception:
                continue
        return False

    def _extract_products_from_page(
        self, soup: BeautifulSoup, category_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        container_sel = self.config.get("product_container", "div.product-item, li.product")
        items = soup.select(container_sel)

        products: List[Dict[str, Any]] = []
        if not items:
            logger.info(
                "No product containers matched '%s' on %s; falling back to anchor discovery",
                container_sel,
                category_name or self.source,
            )
            return self._extract_products_from_anchors(soup, category_name)

        for item in items:
            product = self.extract_product_from_listing(item, category_name)
            if product:
                products.append(product)
        if products:
            return products

        logger.info(
            "Product containers matched '%s' on %s but yielded no valid products; falling back to anchor discovery",
            container_sel,
            category_name or self.source,
        )

        return self._extract_products_from_anchors(soup, category_name)

    def _extract_listing_product(
        self, item: Any, category_name: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        link_sel = self.config.get("product_link", "a")
        title_sel = self.config.get("product_title", "h4.product-name, h3, h2")
        price_sel = self.config.get("price_selector", "span.price, .price")
        vendor_sel = self.config.get("vendor_selector", "span.brand-name, .vendor")
        title_selectors = [(selector, None) for selector in _split_selectors(title_sel)]
        price_selectors = [(selector, None) for selector in _split_selectors(price_sel)]
        vendor_selectors = [(selector, None) for selector in _split_selectors(vendor_sel)]

        if getattr(item, "name", None) == "a":
            candidate = item if item.get("href") else None
            if candidate and any(
                pattern in self._absolute_url(candidate.get("href", "")).lower()
                for pattern in self._product_patterns()
            ):
                link_tag = candidate
            else:
                link_tag = item.select_one(link_sel) or item.select_one("a[href]")
        else:
            link_tag = item.select_one(link_sel) or item.select_one("a")

        href = link_tag.get("href", "") if link_tag else ""
        if href.startswith("#"):
            url = self.build_synthetic_fragment_url(href)
        else:
            url = self._absolute_url(href) if link_tag else ""

        part_number = self._extract_part_number(item, link_tag)
        material_id = part_number or (
            item.get("data-material-id", "") if hasattr(item, "get") else ""
        )
        if not url and material_id:
            url = self.build_synthetic_fragment_url(f"#material-{material_id}")
        if not url:
            return None

        name = first_match(
            item,
            title_selectors + [("h3", None), ("h2", None), ("img", "alt")],
        ) or (
            link_tag.get("title", "") if link_tag else ""
        ) or (
            link_tag.get_text(" ", strip=True) if link_tag else ""
        )
        name = normalise_arabic(re.sub(r"\s+", " ", name).strip())
        image_alt_name = normalise_arabic(first_match(item, [("img", "alt")]) or "")
        if name.endswith("...") and image_alt_name:
            name = image_alt_name
        if not name and material_id:
            name = material_id

        raw_price = first_match(
            item,
            price_selectors
            + [
                (".money", None),
                ("[class*='price']", None),
                ("[itemprop='price']", "content"),
            ],
        ) or ""

        vendor = normalise_arabic(
            first_match(
                item,
                vendor_selectors + [(".brand", None), ("[itemprop='brand']", None)],
            )
            or ""
        )

        image_url = first_match(
            item,
            [
                ("img", "data-src"),
                ("img", "data-lazy"),
                ("img", "src"),
            ],
        ) or ""
        if image_url.startswith("data:image/"):
            image_url = ""
        if image_url.startswith("//"):
            image_url = "https:" + image_url

        lowered_name = name.lower()
        if not name and not raw_price and not material_id:
            return None
        if re.fullmatch(r"-?\d+%", name):
            return None
        if lowered_name in {"quick view", "عرض سريع"}:
            return None

        stock_text = item.get_text(" ", strip=True).lower()
        if any(word in stock_text for word in ("out of stock", "sold out", "نفد", "غير متاح")):
            stock_status = "out_of_stock"
        elif any(word in stock_text for word in ("in stock", "add to cart", "متاح", "موجود")):
            stock_status = "in_stock"
        else:
            stock_status = "unknown"

        return {
            "name": name,
            "url": url,
            "price": clean_price(raw_price),
            "raw_price": raw_price,
            "vendor": vendor,
            "part_number": clean_part_number(material_id),
            "image_url": image_url,
            "stock_status": stock_status,
            "category": category_name or "",
            "source": self.source,
            "data_source": "listing",
            "listing_only": bool(self.config.get("extract_from_listing") or "#material-" in url),
        }

    async def scrape_product_details(self, product_url: str) -> Dict[str, Any]:
        if "#material-" in product_url:
            return self._extract_details_from_listing_reference(product_url)

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

    def extract_details_from_listing(
        self,
        item: Any,
        product: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not (self.config.get("extract_from_listing") or "#material-" in product.get("url", "")):
            return {}

        description = normalise_arabic(re.sub(r"\s+", " ", item.get_text(" ", strip=True)).strip())
        if description == product.get("name", ""):
            description = ""

        return {
            "description": description,
            "specifications": {},
            "variants": [],
            "data_source": "listing_only",
            "listing_only": True,
        }

    def _parse_product_html(self, html: str, url: str) -> Dict[str, Any]:
        soup = BeautifulSoup(html, "lxml")
        jsonld_product = self._extract_jsonld_product(soup, url)
        if jsonld_product:
            return jsonld_product

        name = normalise_arabic(
            first_match(
                soup,
                [
                    ("h1.product-title", None),
                    ("h1", None),
                    ('meta[property="og:title"]', "content"),
                ],
            )
            or ""
        )

        raw_price = first_match(
            soup,
            [
                ("span.price", None),
                (".product-price", None),
                (".price", None),
                ("[itemprop='price']", "content"),
                ('meta[property="product:price:amount"]', "content"),
            ],
        ) or ""

        vendor = normalise_arabic(
            first_match(
                soup,
                [
                    ("span.brand-name", None),
                    (".vendor", None),
                    ("[itemprop='brand']", None),
                ],
            )
            or ""
        )

        sku = first_match(
            soup,
            [
                (".sku", None),
                (".part-number", None),
                ("[itemprop='sku']", None),
            ],
        ) or ""

        description = normalise_arabic(
            first_match(
                soup,
                [
                    (".product-description", None),
                    ("#product-desc", None),
                    (".description", None),
                    ("[itemprop='description']", None),
                ],
            )
            or ""
        )

        specs: Dict[str, str] = {}
        for row in soup.select("table.specs tr, .product-specs tr, .spec-table tr, table tr"):
            cells = row.select("td, th")
            if len(cells) >= 2:
                key = normalise_arabic(cells[0].get_text(" ", strip=True))
                value = normalise_arabic(cells[1].get_text(" ", strip=True))
                if key and value:
                    specs[key] = value

        image_url = first_match(
            soup,
            [
                (".product-image img", "src"),
                (".main-image img", "src"),
                ("img.product-img", "src"),
                ('meta[property="og:image"]', "content"),
            ],
        ) or ""
        if image_url.startswith("//"):
            image_url = "https:" + image_url

        stock_text = soup.get_text(" ", strip=True).lower()
        if any(word in stock_text for word in ("out of stock", "sold out", "نفد", "غير متاح")):
            stock_status = "out_of_stock"
        elif any(word in stock_text for word in ("in stock", "add to cart", "متاح", "موجود")):
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
            "source": self.source,
            "data_source": "playwright_html",
        }

    def _seed_categories(self) -> List[Dict[str, str]]:
        return super()._seed_categories()

    def _category_excludes(self) -> List[str]:
        return [
            pattern.lower()
            for pattern in self.config.get(
                "category_exclude_patterns",
                [
                    "/account",
                    "/cart",
                    "/search",
                    "/wishlist",
                    "/compare",
                    "/product-page/",
                ],
            )
        ]

    def _product_patterns(self) -> List[str]:
        return [
            pattern.lower()
            for pattern in self.config.get(
                "product_link_patterns",
                [
                    "/products/",
                    "/product/",
                    "/product-detail/",
                    "/product-page/",
                    "-parts-",
                ],
            )
        ]

    def _discover_category_links(self, soup: BeautifulSoup) -> List[Any]:
        patterns = [
            pattern.lower()
            for pattern in self.config.get(
                "category_link_patterns",
                [
                    "/collections/",
                    "/category/",
                    "/product-category/",
                    "/shop?category=",
                    "/products?product_type=",
                    "/products?category_id=",
                    "-parts-",
                    "/categories/",
                    "/shop-1",
                ],
            )
        ]

        discovered: List[Any] = []
        for tag in soup.select("a[href]"):
            href = tag.get("href", "")
            url = self._absolute_url(href)
            lower = url.lower()
            if any(excluded in lower for excluded in self._category_excludes()):
                continue
            if any(pattern in lower for pattern in patterns):
                if any(pattern in lower for pattern in self._product_patterns()):
                    continue
                discovered.append(tag)
        return discovered

    def _extract_products_from_anchors(
        self, soup: BeautifulSoup, category_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        products: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for tag in soup.select("a[href]"):
            href = tag.get("href", "")
            url = self._absolute_url(href)
            lower = url.lower()
            if not any(pattern in lower for pattern in self._product_patterns()):
                continue
            if any(excluded in lower for excluded in self._category_excludes()):
                continue
            if url in seen:
                continue
            product = self.extract_product_from_listing(tag, category_name)
            if product:
                seen.add(url)
                products.append(product)
        return products

    def _extract_part_number(self, item: Any, link_tag: Optional[Any] = None) -> str:
        selector = self.config.get("part_number_selector", "")
        attribute = self.config.get("part_number_attribute")
        selectors = _split_selectors(selector)
        if selectors:
            pairs = [(sel, attribute) for sel in selectors] if attribute else [
                (sel, None) for sel in selectors
            ]
            value = first_match(item, pairs) or ""
            if value:
                return clean_part_number(value)

        for candidate in (item, link_tag):
            if candidate is None or not hasattr(candidate, "get"):
                continue
            for attr in ("data-material-id", "data-part-number", "data-sku", "data-sku-id"):
                value = candidate.get(attr, "")
                if value:
                    return clean_part_number(value)

        text = item.get_text(" ", strip=True) if hasattr(item, "get_text") else ""
        match = re.search(
            r"(?:part\s*(?:no\.?|number)?|sku|oem|ref\.?)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]{2,})",
            text,
            re.I,
        )
        if match:
            return clean_part_number(match.group(1))
        return ""

    def _extract_details_from_listing_reference(self, product_url: str) -> Dict[str, Any]:
        fragment = product_url.split("#material-", 1)[-1] if "#material-" in product_url else ""
        return {
            "url": product_url,
            "part_number": clean_part_number(fragment),
            "description": "",
            "specifications": {},
            "variants": [],
            "source": self.source,
            "data_source": "listing_only",
            "listing_only": True,
            "notes": self._listing_only_note(product_url),
        }

    def _extract_jsonld_product(
        self, soup: BeautifulSoup, url: str
    ) -> Optional[Dict[str, Any]]:
        for script in soup.select('script[type="application/ld+json"]'):
            raw = script.string or script.get_text(strip=True)
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                continue

            stack = [payload]
            while stack:
                current = stack.pop()
                if isinstance(current, list):
                    stack.extend(current)
                    continue
                if not isinstance(current, dict):
                    continue
                if "@graph" in current:
                    stack.extend(current["@graph"])
                    continue

                current_type = current.get("@type", [])
                if isinstance(current_type, str):
                    current_types = [current_type.lower()]
                else:
                    current_types = [str(item).lower() for item in current_type]

                if "product" not in current_types:
                    continue

                offers = current.get("offers", {})
                if isinstance(offers, list):
                    offers = offers[0] if offers else {}

                brand = current.get("brand", "")
                if isinstance(brand, dict):
                    brand = brand.get("name", "")

                image = current.get("image", "")
                if isinstance(image, list):
                    image = image[0] if image else ""

                availability = str(offers.get("availability", "")).lower()
                if "instock" in availability:
                    stock_status = "in_stock"
                elif "outofstock" in availability:
                    stock_status = "out_of_stock"
                else:
                    stock_status = "unknown"

                return {
                    "url": url,
                    "name": normalise_arabic(current.get("name", "")),
                    "vendor": normalise_arabic(str(brand)),
                    "part_number": clean_part_number(
                        current.get("sku")
                        or current.get("mpn")
                        or current.get("productID", "")
                    ),
                    "price": clean_price(str(offers.get("price", ""))),
                    "raw_price": str(offers.get("price", "")),
                    "image_url": image,
                    "stock_status": stock_status,
                    "description": normalise_arabic(_strip_html(current.get("description", ""))),
                    "specifications": {},
                    "variants": [],
                    "source": self.source,
                    "data_source": "jsonld_product",
                }
        return None

    def _record_request(
        self,
        *,
        url: str,
        success: bool,
        duration: float,
        status: int,
        attempt: int,
        blocked: bool = False,
    ) -> None:
        if self._metrics:
            self._metrics.record_request(
                url=url,
                success=success,
                duration=duration,
                status=status,
                proxy=None,
                attempt=attempt,
                blocked=blocked,
            )


def _strip_html(html_str: str) -> str:
    if not html_str:
        return ""
    return BeautifulSoup(html_str, "lxml").get_text(separator=" ", strip=True)


def _split_selectors(value: str) -> List[str]:
    return [selector.strip() for selector in value.split(",") if selector.strip()]
