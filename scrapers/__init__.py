"""
Lazy scraper registry — avoids importing playwright/aiohttp at package load time.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .egycarparts import EgyCarPartsScraper
    from .alkhaleeg import AlKhaleegScraper


def get_scraper(name: str):
    """Return the scraper class for *name* (imported on demand)."""
    if name == "egycarparts":
        from .egycarparts import EgyCarPartsScraper
        return EgyCarPartsScraper
    if name == "alkhaleeg":
        from .alkhaleeg import AlKhaleegScraper
        return AlKhaleegScraper
    raise ValueError(f"Unknown scraper: {name!r}")


__all__ = ["get_scraper"]
