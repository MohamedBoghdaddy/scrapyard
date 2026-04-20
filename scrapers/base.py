"""
Abstract base class for all Scrapyard scrapers.

Concrete scrapers still implement the three core async methods, but the base
class now centralises a few cross-cutting behaviors:
  - seeded category handling via `seed_categories` or legacy `categories`
  - fragment URL synthesis for JS/widget catalogs
  - listing-only extraction hooks for sites without real detail pages
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib.parse import urljoin, urlparse

if TYPE_CHECKING:
    from utils.metrics import MetricsTracker

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """
    All scrapers inherit from this class and must implement:
      - scrape_categories()
      - scrape_products_from_category(category_url)
      - scrape_product_details(product_url)
    """

    def __init__(
        self,
        config: Dict[str, Any],
        metrics: Optional["MetricsTracker"] = None,
    ) -> None:
        self.config = config
        self.base_url: str = config.get("base_url", "")
        self.site_id: str = (
            config.get("site_id")
            or urlparse(self.base_url).netloc.replace("www.", "")
            or "site"
        )
        self.max_retries: int = config.get("max_retries", 3)
        self.timeout: int = config.get("timeout", 30)
        self.delay_min: float = config.get("request_delay_min", 1.0)
        self.delay_max: float = config.get("request_delay_max", 3.0)
        self.max_pages: int = config.get("max_pages", 10)
        self.llm_enabled: bool = config.get("llm_enabled", False)
        self._metrics = metrics
        self.logger = logging.getLogger(self.__class__.__name__)

    # ------------------------------------------------------------------
    # Lifecycle hooks
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "BaseScraper":
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.teardown()

    async def setup(self) -> None:
        """Initialise HTTP sessions, browser contexts, etc."""

    async def teardown(self) -> None:
        """Release resources."""

    # ------------------------------------------------------------------
    # Core scraping interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def scrape_categories(self) -> List[Dict[str, str]]:
        """
        Return a list of category dicts, each with at minimum:
          { "name": str, "url": str }
        """

    @abstractmethod
    async def scrape_products_from_category(
        self,
        category_url: str,
        category_name: Optional[str] = None,
        start_page: int = 1,
    ) -> List[Dict[str, Any]]:
        """
        Return a list of product dicts scraped from all pages of a category.
        Minimum keys: name, price, url, vendor, part_number, image_url, stock_status.
        *start_page* enables resume: skip to a previously checkpointed page number.
        """

    @abstractmethod
    async def scrape_product_details(self, product_url: str) -> Dict[str, Any]:
        """
        Return a rich dict for a single product page.
        Keys include those from scrape_products_from_category plus:
          description, specifications (dict), variants (list).
        """

    # ------------------------------------------------------------------
    # Convenience helpers shared by all scrapers
    # ------------------------------------------------------------------

    def _absolute_url(self, href: str, *, base_url: Optional[str] = None) -> str:
        """Resolve a potentially relative URL against the site base_url."""
        if not href:
            return ""
        if href.startswith("http"):
            return href
        root = (base_url or self.base_url or "").strip()
        if not root:
            return href
        root = root if root.endswith("/") else root + "/"
        return urljoin(root, href)

    def build_synthetic_fragment_url(
        self,
        fragment: str,
        *,
        base_url: Optional[str] = None,
    ) -> str:
        """Build a stable synthetic URL for fragment-only product references."""
        if not fragment:
            return ""
        fragment = fragment.strip()
        if fragment.startswith("http"):
            return fragment
        if not fragment.startswith("#"):
            fragment = "#" + fragment.lstrip("#")
        root = (base_url or self.base_url or "").split("#", 1)[0].rstrip("/")
        return f"{root}/{fragment}" if root else fragment

    def _seed_categories(self) -> List[Dict[str, str]]:
        """
        Return configured seed categories.

        Supports both the newer `seed_categories` key and the legacy `categories`
        key so manual seeding remains backwards-compatible.
        """
        seeded = self.config.get("seed_categories") or self.config.get("categories") or []
        categories: List[Dict[str, str]] = []
        for entry in seeded:
            if isinstance(entry, str):
                url = entry
                name = urlparse(url).path.strip("/") or self.site_id
            elif isinstance(entry, dict):
                url = entry.get("url", "")
                name = entry.get("name") or urlparse(url).path.strip("/") or self.site_id
            else:
                continue
            if url:
                categories.append({"name": name, "url": self._absolute_url(url)})
        return categories

    def extract_product_from_listing(
        self,
        item: Any,
        category_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Shared hook used by category scrapers to build a product row from a card.

        Subclasses provide `_extract_listing_product()`. They can optionally
        override `extract_details_from_listing()` to keep richer data for sites
        where the listing card is the best source of truth.
        """
        extractor = getattr(self, "_extract_listing_product", None)
        if extractor is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} must implement _extract_listing_product()"
            )

        product = extractor(item, category_name)
        if not product:
            return None

        listing_details = self.extract_details_from_listing(item, product) or {}
        if listing_details:
            product = {**product, **listing_details}

        product.setdefault("source", getattr(self, "source", self.site_id))

        notes = [str(product.get("notes", "")).strip()]
        config_notes = str(self.config.get("notes", "")).strip()
        if config_notes:
            notes.append(config_notes)
        if "#material-" in str(product.get("url", "")):
            notes.append(self._listing_only_note(product["url"]))
        product["notes"] = self._merge_notes(notes)
        return product

    def extract_details_from_listing(
        self,
        item: Any,
        product: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Optional override for sites where the card already contains the useful
        data and visiting a detail page is unnecessary or impossible.
        """
        return {}

    def _listing_only_note(self, product_url: str) -> str:
        fragment = product_url.split("#", 1)[-1] if "#" in product_url else ""
        if fragment:
            return (
                "Detail page uses fragment-based navigation; listing data was kept "
                f"for {fragment}."
            )
        return "Detail page uses fragment-based navigation; listing data was kept."

    @staticmethod
    def _merge_notes(parts: List[str]) -> str:
        seen: List[str] = []
        for part in parts:
            cleaned = part.strip()
            if cleaned and cleaned not in seen:
                seen.append(cleaned)
        return " ".join(seen)
