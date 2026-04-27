"""
Generic HTTP scraper for automotive catalog sites.

This scraper now powers non-JavaScript sites across multiple storefront styles:
  - Shopify-style stores (/collections/, /products/)
  - WooCommerce/custom stores (/product-category/, /product/)
  - Custom catalog pages (/shop?category=, /product-detail/)
  - Inventory-style sites that expose product-like links in plain HTML

It keeps the stronger retry/blocking behavior that was already present for the
original EgyCarParts implementation, but resolves selectors and link patterns
from config so one scraper can cover many sites. Product detail parsing is
hybrid: if a page exposes Shopify JSON, that payload wins even when the site
config is marked as `type: custom`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import aiohttp
from bs4 import BeautifulSoup

from . import detect_storefront_type
from .base import BaseScraper
from .detail_helpers import enrich_product_fields, extract_best_text, extract_specifications
from .utils import (
    clean_part_number,
    clean_price,
    extract_shopify_product_json,
    get_headers,
    random_delay,
)
from utils.cleaners import (
    clean_text,
    extract_compatibility_text,
    extract_oem_references,
    extract_part_number,
    first_match,
)
from utils.jina import fetch_via_jina
from utils.proxies import ProxyManager

logger = logging.getLogger(__name__)


_BLOCK_PATTERNS = [
    (re.compile(r"captcha", re.I), 403),
    (re.compile(r"access denied|forbidden", re.I), 403),
    (re.compile(r"rate limit|too many requests", re.I), 429),
    (re.compile(r"please verify you are a human", re.I), 403),
    (re.compile(r"attention required", re.I), 403),
    (re.compile(r"cf-browser-verification|__cf_chl_|challenge-platform", re.I), 403),
]


class BlockedError(Exception):
    """Raised when a response body matches a bot-detection pattern."""

    def __init__(self, url: str, status: int) -> None:
        super().__init__(f"Blocked ({status}) at {url}")
        self.url = url
        self.status = status


def _check_content_blocking(body: str) -> Tuple[bool, int]:
    """Return (is_blocked, inferred_status) by scanning the response body."""
    sample = BeautifulSoup(body[:15000], "lxml").get_text(" ", strip=True)[:3000]
    for pattern, status in _BLOCK_PATTERNS:
        if pattern.search(sample):
            return True, status
    return False, 0


class EgyCarPartsScraper(BaseScraper):
    """Config-driven HTTP scraper for catalog-style sites."""

    def __init__(self, config: Dict[str, Any], metrics=None) -> None:
        super().__init__(config, metrics)
        self._session: Optional[aiohttp.ClientSession] = None
        self._proxy_manager = ProxyManager()
        self.source = self.site_id

    async def setup(self) -> None:
        connector_kwargs: Dict[str, Any] = {"limit": 10}
        if self.config.get("ignore_ssl"):
            connector_kwargs["ssl"] = False
        connector = aiohttp.TCPConnector(**connector_kwargs)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=get_headers(),
            auto_decompress=True,
        )
        logger.info("%s HTTP session initialised", self.source)

    async def teardown(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
        logger.info("%s HTTP session closed", self.source)

    async def _fetch(
        self, url: str, *, as_json: bool = False
    ) -> Tuple[Optional[Any], float]:
        """
        GET *url* with retries, content blocking detection, and Jina fallback.
        Returns (body, duration_seconds). Body is None on failure.
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
                        logger.warning("Rate-limited on %s - sleeping %ds", url, wait)
                        self._record_request(
                            url=url,
                            success=False,
                            duration=duration,
                            status=429,
                            proxy=proxy,
                            attempt=attempt,
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
                            self._record_request(
                                url=url,
                                success=False,
                                duration=duration,
                                status=block_status,
                                proxy=proxy,
                                attempt=attempt,
                                blocked=True,
                            )
                            raise BlockedError(url, block_status)

                    self._record_request(
                        url=url,
                        success=True,
                        duration=duration,
                        status=resp.status,
                        proxy=proxy,
                        attempt=attempt,
                    )
                    return body, duration

            except BlockedError:
                if proxy:
                    self._proxy_manager.mark_bad(proxy)
                proxy = self._proxy_manager.get_proxy()
                await asyncio.sleep(2 ** attempt)

            except (UnicodeDecodeError, aiohttp.ContentTypeError):
                logger.warning(
                    "Decode error on %s (attempt %d) - retrying with gzip/deflate only",
                    url,
                    attempt,
                )
                retry_headers = {**headers, "Accept-Encoding": "gzip, deflate"}
                try:
                    async with self._session.get(
                        url,
                        headers=retry_headers,
                        proxy=proxy,
                        allow_redirects=True,
                    ) as resp2:
                        body = await resp2.text(errors="replace")
                        duration = time.monotonic() - t0
                        self._record_request(
                            url=url,
                            success=True,
                            duration=duration,
                            status=resp2.status,
                            proxy=proxy,
                            attempt=attempt,
                        )
                        return body, duration
                except Exception:
                    pass
                await asyncio.sleep(2 ** attempt)

            except aiohttp.ClientError as exc:
                backoff = 2 ** attempt
                logger.warning(
                    "Attempt %d/%d failed for %s: %s - retrying in %ds",
                    attempt,
                    self.max_retries,
                    url,
                    exc,
                    backoff,
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

        jina_content = await fetch_via_jina(url)
        if jina_content:
            if self._metrics:
                self._metrics.record_jina_fallback()
            return jina_content, time.monotonic() - t0

        self._record_request(
            url=url,
            success=False,
            duration=duration,
            status=0,
            proxy=proxy,
            attempt=self.max_retries,
        )
        return None, duration

    async def scrape_categories(self) -> List[Dict[str, str]]:
        """Parse category links from config seeds or page heuristics."""
        seeded = self._seed_categories()
        if seeded:
            logger.info("Using %d seeded categories for %s", len(seeded), self.source)
            return seeded

        html, _ = await self._fetch(self.base_url)
        if not html:
            return []

        soup = BeautifulSoup(html, "lxml")
        selector = self.config.get("categories_selector", "nav ul li a")
        links = soup.select(selector)
        if not links:
            links = self._discover_category_links(soup)

        categories: List[Dict[str, str]] = []
        seen: set[str] = set()
        for tag in links:
            href = tag.get("href", "")
            name = tag.get_text(" ", strip=True)
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
        """
        Paginate through a category and return all product stubs.

        For Shopify sites this first tries the /products.json bulk API which
        returns full product data (vendor, description, stock, variants) in a
        single request per page – no detail-page visits needed.
        """
        # ── Shopify bulk API fast-path ────────────────────────────────────
        if self.config.get("type") == "shopify" or self._looks_shopify(category_url):
            products = await self._scrape_shopify_products_json(
                category_url, category_name=category_name, start_page=start_page
            )
            if products:
                logger.info(
                    "Category '%s' yielded %d products (Shopify JSON)",
                    category_name or category_url,
                    len(products),
                )
                return products
            logger.debug(
                "Shopify JSON endpoint unavailable for %s – falling back to HTML",
                category_url,
            )

        # ── HTML fallback ─────────────────────────────────────────────────
        products_html: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        page_num = start_page
        page_url: Optional[str] = self._page_url(category_url, page_num)

        while page_url and page_num <= self.max_pages:
            logger.info("Scraping page %d: %s", page_num, page_url)
            html, _ = await self._fetch(page_url)
            if not html:
                break

            soup = BeautifulSoup(html, "lxml")
            page_products = self._extract_products_from_page(soup, category_name)
            if not page_products:
                logger.debug("No product cards on %s", page_url)
                break

            for product in page_products:
                product_url = product.get("url", "")
                if product_url and product_url not in seen_urls:
                    seen_urls.add(product_url)
                    products_html.append(product)

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
            len(products_html),
        )
        return products_html

    def _looks_shopify(self, url: str) -> bool:
        """Heuristic: URL contains /collections/ — strong Shopify indicator."""
        return "/collections/" in url

    async def _scrape_shopify_products_json(
        self,
        category_url: str,
        *,
        category_name: Optional[str],
        start_page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Fetch /products.json?limit=250&page=N for the given Shopify collection.

        Returns an empty list if the endpoint is unavailable so the caller can
        fall back to HTML scraping.
        """
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(category_url)
        # Build the products.json URL: /collections/{slug}/products.json
        json_path = parsed.path.rstrip("/") + "/products.json"
        products: List[Dict[str, Any]] = []
        seen_urls: set[str] = set()
        limit = 250

        for page in range(start_page, start_page + self.max_pages):
            query = f"limit={limit}&page={page}"
            api_url = urlunparse(
                (parsed.scheme, parsed.netloc, json_path, "", query, "")
            )
            body, _ = await self._fetch(api_url, as_json=True)
            if not body or not isinstance(body, dict):
                break

            raw_products = body.get("products", [])
            if not raw_products:
                break

            for item in raw_products:
                if not isinstance(item, dict):
                    continue
                product = self._map_shopify_product_json(item, category_name)
                url = product.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    products.append(product)

            if len(raw_products) < limit:
                break  # last page

            await random_delay(self.delay_min, self.delay_max)

        return products

    def _map_shopify_product_json(
        self,
        item: Dict[str, Any],
        category_name: Optional[str],
    ) -> Dict[str, Any]:
        """Map a single entry from /products.json to the canonical product schema."""
        from scrapers.utils import clean_part_number, clean_price
        from utils.cleaners import (
            extract_compatibility_text,
            extract_oem_references,
            extract_part_number,
        )

        handle = item.get("handle", "")
        url = f"{self.base_url}/products/{handle}" if handle else ""

        variants = item.get("variants", [])
        first = variants[0] if variants else {}

        # Price from first variant (Shopify stores prices in cents as strings)
        raw_price = str(first.get("price", ""))
        price = _normalise_shopify_money(first.get("price"))

        # Stock: available flag on first variant
        available = first.get("available", None)
        if available is True:
            stock_status = "in_stock"
        elif available is False:
            stock_status = "out_of_stock"
        else:
            stock_status = "unknown"

        # Image
        images = item.get("images", [])
        image_url = ""
        if images:
            src = images[0].get("src", "") if isinstance(images[0], dict) else str(images[0])
            image_url = ("https:" + src) if src.startswith("//") else src

        # Description
        description = _strip_html(item.get("body_html", ""))

        # Part number
        sku = str(first.get("sku", "") or "")
        part_number = clean_part_number(sku) or extract_part_number(description)

        # Tags → OEM references and compatibility
        raw_tags = item.get("tags", [])
        if isinstance(raw_tags, str):
            tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
        else:
            tags = [str(t) for t in raw_tags if str(t).strip()]

        metadata_text = "\n".join(filter(None, [description, "\n".join(tags)]))
        compatibility_text = extract_compatibility_text(metadata_text)
        oem_references = extract_oem_references(metadata_text)

        # Specifications from options
        specs: Dict[str, str] = {}
        for opt in item.get("options", []):
            if isinstance(opt, dict):
                specs[opt.get("name", "")] = ", ".join(opt.get("values", []))
        product_type = str(item.get("product_type", "") or "")
        if product_type:
            specs["Product type"] = product_type

        return {
            "name": str(item.get("title", "")),
            "url": url,
            "price": price,
            "raw_price": raw_price,
            "vendor": str(item.get("vendor", "")),
            "part_number": part_number,
            "image_url": image_url,
            "stock_status": stock_status,
            "category": category_name or "",
            "source": self.source,
            "description": description,
            "specifications": specs,
            "variants": [
                {
                    "title": v.get("title"),
                    "sku": v.get("sku"),
                    "price": _normalise_shopify_money(v.get("price")),
                    "available": v.get("available"),
                }
                for v in variants
                if isinstance(v, dict)
            ],
            "tags": tags,
            "compatibility_text": compatibility_text,
            "oem_references": oem_references,
            "data_source": "shopify_products_json",
            "listing_only": False,
        }

    def _page_url(self, base: str, page: int) -> str:
        """Construct a paginated URL using config-driven rules."""
        if page <= 1:
            return base

        template = self.config.get("page_url_template")
        if template:
            return template.format(url=base, page=page)

        if self.config.get("pagination_style") == "path":
            return base.rstrip("/") + f"/page/{page}/"

        parts = list(urlparse(base))
        params = parse_qs(parts[4])
        params[self.config.get("page_param_name", "page")] = [str(page)]
        parts[4] = urlencode({k: v[0] for k, v in params.items()})
        return urlunparse(parts)

    def _extract_products_from_page(
        self, soup: BeautifulSoup, category_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        container_sel = self.config.get("product_container", "ul.product-grid li")
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
        """Extract lightweight product data from a card or direct anchor."""
        title_sel = self.config.get("product_title", ".product-title, h3")
        price_sel = self.config.get("price_selector", ".price")
        vendor_sel = self.config.get("vendor_selector", ".vendor")
        link_sel = self.config.get("product_link", "a")
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
        name = re.sub(r"\s+", " ", name).strip()
        image_alt_name = first_match(item, [("img", "alt")]) or ""
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

        vendor = first_match(
            item,
            vendor_selectors + [(".brand", None), ("[itemprop='brand']", None)],
        ) or ""

        image_url = first_match(
            item,
            [
                ("img", "src"),
                ("img", "data-src"),
                ("img", "data-lazy"),
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

        # Stock detection: CSS classes (reliable on Shopify) then text fallback
        item_classes = " ".join(item.get("class", []) if hasattr(item, "get") else []).lower()
        stock_text = item.get_text(" ", strip=True).lower()
        if (
            "sold-out" in item_classes
            or "sold_out" in item_classes
            or "out-of-stock" in item_classes
            or "out_of_stock" in item_classes
            or "out of stock" in stock_text
            or "sold out" in stock_text
            or "نفذ" in stock_text
            or "غير متوفر" in stock_text
        ):
            stock_status = "out_of_stock"
        elif (
            "in-stock" in item_classes
            or "in_stock" in item_classes
            or "available" in item_classes
            or "in stock" in stock_text
            or "add to cart" in stock_text
            or "اضف للسلة" in stock_text
            or "أضف للسلة" in stock_text
        ):
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
        """Fetch a product page and return full details."""
        if "#material-" in product_url:
            return self._extract_details_from_listing_reference(product_url)

        html, _ = await self._fetch(product_url)
        if not html:
            return {"url": product_url, "error": "fetch_failed"}

        storefront_type = detect_storefront_type(html, self.config)
        shopify_data = extract_shopify_product_json(html)
        if storefront_type == "shopify" and shopify_data:
            return self._parse_shopify_json(shopify_data, product_url)

        result = self._parse_product_html(html, product_url)

        if not result.get("name") and self.llm_enabled:
            logger.info("CSS selectors empty - trying LLM extraction for %s", product_url)
            from utils.llm_extractor import extract_with_llm

            llm_data = await extract_with_llm(html)
            if llm_data:
                if self._metrics:
                    self._metrics.record_llm_extraction()
                result.update(
                    {
                        "name": llm_data.get("name", result.get("name", "")),
                        "vendor": llm_data.get("brand", result.get("vendor", "")),
                        "part_number": llm_data.get(
                            "part_number",
                            result.get("part_number", ""),
                        ),
                        "price": llm_data.get("price") or result.get("price"),
                        "description": llm_data.get(
                            "description",
                            result.get("description", ""),
                        ),
                        "data_source": "llm_extraction",
                    }
                )

        return result

    def extract_details_from_listing(
        self,
        item: Any,
        product: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not (self.config.get("extract_from_listing") or "#material-" in product.get("url", "")):
            return {}

        description = re.sub(r"\s+", " ", item.get_text(" ", strip=True)).strip()
        if description == product.get("name", ""):
            description = ""
        compatibility_text = extract_compatibility_text(description)
        oem_references = extract_oem_references(description)
        listing_part_number = (
            clean_part_number(product.get("part_number", ""))
            or extract_part_number(description)
        )

        return {
            "description": description,
            "part_number": listing_part_number,
            "compatibility_text": compatibility_text,
            "oem_references": oem_references,
            "specifications": {},
            "variants": [],
            "data_source": "listing_only",
            "listing_only": True,
        }

    def _parse_shopify_json(self, data: Dict[str, Any], url: str) -> Dict[str, Any]:
        """Map Shopify product JSON to the canonical schema."""
        variants = data.get("variants", [])
        first = variants[0] if variants else {}
        product_name = (
            data.get("title")
            or data.get("name")
            or first.get("name")
            or first.get("title")
            or str(data.get("handle", "")).replace("-", " ").strip().title()
        )
        images = data.get("images", []) or data.get("media", [])
        image_url = ""
        if images:
            first_image = images[0]
            if isinstance(first_image, dict):
                src = first_image.get("src") or first_image.get("original_src") or ""
            else:
                src = str(first_image)
            image_url = ("https:" + src) if src.startswith("//") else src

        specs: Dict[str, str] = {}
        for opt in data.get("options", []):
            if isinstance(opt, dict):
                specs[opt.get("name", "")] = ", ".join(opt.get("values", []))
        product_type = clean_text(str(data.get("product_type", "") or ""))
        if product_type:
            specs["Product type"] = product_type

        description = _strip_html(data.get("body_html", ""))
        raw_tags = data.get("tags", [])
        if isinstance(raw_tags, str):
            tags = [tag.strip() for tag in raw_tags.split(",") if tag.strip()]
        elif isinstance(raw_tags, list):
            tags = [clean_text(str(tag)) for tag in raw_tags if str(tag).strip()]
        else:
            tags = []
        metadata_text = "\n".join(
            value
            for value in (
                description,
                "\n".join(tags),
                json.dumps(data, ensure_ascii=False, default=str),
            )
            if value
        )
        part_number = clean_part_number(
            (data.get("variants") or [{}])[0].get("sku", "")
        ) or extract_part_number(metadata_text)

        return {
            "url": url,
            "name": product_name,
            "vendor": clean_text(str(data.get("vendor", "") or "")),
            "part_number": part_number,
            "price": _normalise_shopify_money(first.get("price")),
            "raw_price": str(first.get("price", "")),
            "compare_at_price": _normalise_shopify_money(first.get("compare_at_price")),
            "image_url": image_url,
            "stock_status": "in_stock" if first.get("available") else "out_of_stock",
            "description": description,
            "specifications": specs,
            "variants": [
                {
                    "title": variant.get("title") or variant.get("name"),
                    "sku": variant.get("sku"),
                    "price": _normalise_shopify_money(variant.get("price")),
                    "available": variant.get("available"),
                }
                for variant in variants
                if isinstance(variant, dict)
            ],
            "tags": tags,
            "compatibility_text": extract_compatibility_text(
                "\n".join(filter(None, [description, "\n".join(tags)]))
            ),
            "oem_references": extract_oem_references(metadata_text),
            "source": self.source,
            "data_source": "shopify_json",
        }

    def _parse_product_html(self, html: str, url: str) -> Dict[str, Any]:
        """HTML fallback for product pages without embedded JSON."""
        soup = BeautifulSoup(html, "lxml")
        jsonld_product = self._extract_jsonld_product(soup, url)
        if jsonld_product:
            return jsonld_product

        name = first_match(
            soup,
            [
                ("h1.product-title", None),
                ("h1.product__title", None),
                ("h1", None),
                ('meta[property="og:title"]', "content"),
            ],
        ) or ""

        description_selector = self.config.get("description_selector", "")
        description_selectors = _split_selectors(description_selector)
        raw_price = first_match(
            soup,
            [
                (".price__current", None),
                (".product__price", None),
                (".price", None),
                ("[class*='price']", None),
                ("[itemprop='price']", "content"),
                ('meta[property="og:price:amount"]', "content"),
            ],
        ) or ""

        vendor = first_match(
            soup,
            [
                (".product__vendor", None),
                (".vendor", None),
                ("[itemprop='brand']", None),
            ],
        ) or ""

        sku = first_match(
            soup,
            [
                (".product__sku", None),
                (".sku", None),
                ("[itemprop='sku']", None),
            ],
        ) or ""

        description = first_match(
            soup,
            [(selector, None) for selector in description_selectors]
            + [
                (".product__description", None),
                ("#product-description", None),
                (".product-description", None),
                (".description", None),
                ("[itemprop='description']", None),
                ('meta[name="description"]', "content"),
                ('meta[property="og:description"]', "content"),
            ],
        ) or extract_best_text(
            soup,
            description_selectors
            + [
                ".product__description",
                "#product-description",
                ".product-description",
                ".description",
                "[itemprop='description']",
                "meta[name='description']",
                "meta[property='og:description']",
            ],
            min_length=20,
        )

        image_url = first_match(
            soup,
            [
                (".product__media img", "src"),
                (".product-image img", "src"),
                ('meta[property="og:image"]', "content"),
            ],
        ) or ""
        if image_url.startswith("//"):
            image_url = "https:" + image_url

        specs = extract_specifications(soup)
        enrichment = enrich_product_fields(
            soup,
            description=description,
            vendor=vendor,
            part_number=sku,
            specs=specs,
        )

        stock_text = soup.get_text(" ", strip=True).lower()
        if "out of stock" in stock_text or "sold out" in stock_text:
            stock_status = "out_of_stock"
        elif "in stock" in stock_text or "add to cart" in stock_text:
            stock_status = "in_stock"
        else:
            stock_status = "unknown"

        return {
            "url": url,
            "name": name,
            "vendor": enrichment.get("vendor", vendor),
            "part_number": enrichment.get("part_number") or clean_part_number(sku),
            "price": clean_price(raw_price),
            "raw_price": raw_price,
            "image_url": image_url,
            "stock_status": stock_status,
            "description": enrichment.get("description", description),
            "specifications": enrichment.get("specifications", specs),
            "compatibility_text": enrichment.get("compatibility_text", ""),
            "oem_references": enrichment.get("oem_references", []),
            "variants": [],
            "source": self.source,
            "data_source": "html_fallback",
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
                    "/blogs",
                    "/pages",
                    "/compare",
                    "/wishlist",
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

                specs = extract_specifications(soup)
                additional_props = current.get("additionalProperty", [])
                if isinstance(additional_props, dict):
                    additional_props = [additional_props]
                if isinstance(additional_props, list):
                    for prop in additional_props:
                        if not isinstance(prop, dict):
                            continue
                        key = clean_text(
                            str(prop.get("name") or prop.get("propertyID") or "")
                        )
                        value = clean_text(
                            str(prop.get("value") or prop.get("valueReference") or "")
                        )
                        if key and value and key not in specs:
                            specs[key] = value

                description = _strip_html(current.get("description", ""))
                enrichment = enrich_product_fields(
                    soup,
                    description=description,
                    vendor=str(brand or ""),
                    part_number=str(
                        current.get("sku")
                        or current.get("mpn")
                        or current.get("productID", "")
                    ),
                    specs=specs,
                )

                return {
                    "url": url,
                    "name": current.get("name", ""),
                    "vendor": enrichment.get("vendor", str(brand or "")),
                    "part_number": enrichment.get("part_number")
                    or clean_part_number(
                        current.get("sku")
                        or current.get("mpn")
                        or current.get("productID", "")
                    ),
                    "price": clean_price(str(offers.get("price", ""))),
                    "raw_price": str(offers.get("price", "")),
                    "image_url": image,
                    "stock_status": stock_status,
                    "description": enrichment.get("description", description),
                    "specifications": enrichment.get("specifications", specs),
                    "compatibility_text": enrichment.get("compatibility_text", ""),
                    "oem_references": enrichment.get("oem_references", []),
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
        proxy: Optional[str],
        attempt: int,
        blocked: bool = False,
    ) -> None:
        if self._metrics:
            self._metrics.record_request(
                url=url,
                success=success,
                duration=duration,
                status=status,
                proxy=proxy,
                attempt=attempt,
                blocked=blocked,
            )


def _strip_html(html_str: str) -> str:
    if not html_str:
        return ""
    return BeautifulSoup(html_str, "lxml").get_text(separator=" ", strip=True)


def _normalise_shopify_money(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric.is_integer() and abs(numeric) >= 1000:
            return numeric / 100.0
        return numeric
    return clean_price(str(value))


def _split_selectors(value: str) -> List[str]:
    return [selector.strip() for selector in value.split(",") if selector.strip()]
