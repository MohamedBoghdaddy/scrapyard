"""
Proxy management stub.

In production, populate PROXY_LIST from environment variables, a database,
or a commercial proxy API (e.g. BrightData, Oxylabs).  The interface is kept
intentionally simple so it can be swapped without touching scraper code.
"""
from __future__ import annotations

import logging
import os
import random
from typing import Optional

logger = logging.getLogger(__name__)

# Read proxy list from env var: "http://user:pass@host:port,..."
_RAW = os.environ.get("PROXY_LIST", "")
PROXY_LIST: list[str] = [p.strip() for p in _RAW.split(",") if p.strip()]


class ProxyManager:
    """Round-robin proxy selection with a bad-proxy blacklist."""

    def __init__(self, proxies: Optional[list[str]] = None) -> None:
        self._pool: list[str] = list(proxies or PROXY_LIST)
        self._bad: set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_proxy(self) -> Optional[str]:
        """Return a random healthy proxy, or None to use direct connection."""
        healthy = [p for p in self._pool if p not in self._bad]
        if not healthy:
            if self._bad:
                logger.warning("All proxies marked bad – resetting blacklist")
                self._bad.clear()
                healthy = list(self._pool)
            else:
                return None  # no proxies configured – direct connection
        return random.choice(healthy)

    def mark_bad(self, proxy: str) -> None:
        """Temporarily blacklist a proxy that returned an error."""
        logger.warning("Marking proxy bad: %s", proxy)
        self._bad.add(proxy)

    def add_proxy(self, proxy: str) -> None:
        if proxy not in self._pool:
            self._pool.append(proxy)

    def remove_proxy(self, proxy: str) -> None:
        self._pool = [p for p in self._pool if p != proxy]
        self._bad.discard(proxy)

    @property
    def pool_size(self) -> int:
        return len(self._pool)

    @property
    def healthy_count(self) -> int:
        return len([p for p in self._pool if p not in self._bad])
