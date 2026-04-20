"""
Abstract base class for all Scrapyard scrapers.
Each concrete scraper must implement all three core methods.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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

    def _absolute_url(self, href: str) -> str:
        """Resolve a potentially relative URL against the site base_url."""
        if not href:
            return ""
        if href.startswith("http"):
            return href
        return self.base_url.rstrip("/") + "/" + href.lstrip("/")
