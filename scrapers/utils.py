"""
Shared scraping utilities: request helpers, data cleaners, delays.
Imports cleaning logic from utils.cleaners (no circular dependency).
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from typing import Any, Dict, Optional

# Import from utils package (no circular risk – cleaners has no scrapers imports)
from utils.cleaners import (
    clean_price,
    clean_part_number,
    normalise_arabic,
)
from utils.user_agents import get_random_user_agent

# Re-export so scraper modules can do `from scrapers.utils import clean_price`
__all__ = [
    "clean_price",
    "clean_part_number",
    "normalise_arabic",
    "extract_shopify_product_json",
    "get_headers",
    "random_delay",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def get_headers(extra: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Return realistic browser-like request headers."""
    headers = {
        "User-Agent": get_random_user_agent(),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;"
            "q=0.9,image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9,ar;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }
    if extra:
        headers.update(extra)
    return headers


async def random_delay(min_s: float = 1.0, max_s: float = 3.0) -> None:
    """Async sleep for a random duration to be respectful to target servers."""
    delay = random.uniform(min_s, max_s)
    logger.debug("Sleeping %.2fs", delay)
    await asyncio.sleep(delay)


# ---------------------------------------------------------------------------
# Shopify JSON extraction
# ---------------------------------------------------------------------------

_SHOPIFY_META_RE = re.compile(r"var\s+meta\s*=\s*(\{.*?\});", re.DOTALL)
_SHOPIFY_PRODUCT_RE = re.compile(r"var\s+product\s*=\s*(\{.*?\});", re.DOTALL)


def extract_shopify_product_json(html: str) -> Optional[Dict[str, Any]]:
    """
    Pull the embedded `var meta = {...}` or `var product = {...}` JSON blob
    that Shopify themes inject into product pages.
    Returns the parsed dict or None when not found.
    """
    for pattern in (_SHOPIFY_META_RE, _SHOPIFY_PRODUCT_RE):
        match = pattern.search(html)
        if match:
            try:
                data = json.loads(match.group(1))
                if "product" in data:
                    return data["product"]
                return data
            except json.JSONDecodeError:
                continue
    return None
