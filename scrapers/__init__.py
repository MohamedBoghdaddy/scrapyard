"""
Lazy scraper registry and storefront helpers.

The HTTP scraper is intentionally hybrid: it can scrape generic HTML pages, but
it will also switch to Shopify JSON extraction when a product page exposes the
embedded `var meta = {...}` / `var product = {...}` payload.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from .utils import extract_shopify_product_json

if TYPE_CHECKING:
    from .base import BaseScraper
    from .egycarparts import EgyCarPartsScraper
    from .alkhaleeg import AlKhaleegScraper


def _detect_shopify_json(html: str) -> bool:
    """Return True when a page exposes Shopify-style embedded product JSON."""
    return bool(html and extract_shopify_product_json(html))


def detect_storefront_type(
    html: str,
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Infer the best parsing mode for a specific response body.

    Even when a site is configured as `type: custom`, Shopify JSON on the page
    should win so detail extraction can use the richer payload automatically.
    """
    if _detect_shopify_json(html):
        return "shopify"
    cfg = config or {}
    return str(cfg.get("type") or "custom")


def get_scraper(name: str, config: Optional[Dict[str, Any]] = None):
    """Return the scraper class for *name* based on its configured engine."""
    cfg = config or {}
    engine = cfg.get("engine")
    if not engine:
        engine = "playwright" if cfg.get("use_javascript") else "http"

    if engine == "playwright":
        from .alkhaleeg import AlKhaleegScraper

        return AlKhaleegScraper

    from .egycarparts import EgyCarPartsScraper

    return EgyCarPartsScraper


__all__ = ["detect_storefront_type", "get_scraper"]
