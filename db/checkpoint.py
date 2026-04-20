"""
CheckpointManager – resume-capable scraping state storage.

Backends
--------
SQLite  (default, zero-config): pass a file path or ``sqlite:///path/to/db``
PostgreSQL                     : pass ``postgresql://user:pass@host:port/dbname``

The interface is identical regardless of backend.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from db.models import (
    SQLITE_ALL_DDL,
    POSTGRES_CREATE_SCRAPED_URLS,
    POSTGRES_CREATE_CHECKPOINTS,
)

logger = logging.getLogger(__name__)

_SQLITE_PREFIX = ("sqlite:///", "sqlite://")


def _is_postgres(url: str) -> bool:
    return url.startswith("postgresql") or url.startswith("postgres")


def _sqlite_path(url: str) -> str:
    for prefix in _SQLITE_PREFIX:
        if url.startswith(prefix):
            return url[len(prefix):]
    return url  # treat bare path as SQLite file


class CheckpointManager:
    """
    Tracks which URLs have been scraped and where each category left off.

    Usage::

        mgr = CheckpointManager()          # SQLite in scraper_state.db
        await mgr.setup()

        if not await mgr.is_scraped(url):
            data = await scrape(url)
            await mgr.mark_scraped(url, 'success')

        await mgr.save_progress('egycarparts', 'brake-pads', last_page=3)
        await mgr.close()
    """

    def __init__(self, db_url: str = "scraper_state.db") -> None:
        self.db_url = db_url
        self._backend: str = "postgres" if _is_postgres(db_url) else "sqlite"
        self._conn: Any = None
        logger.debug("CheckpointManager using %s backend", self._backend)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Create connection and initialise schema."""
        if self._backend == "sqlite":
            await self._setup_sqlite()
        else:
            await self._setup_postgres()
        logger.info("CheckpointManager ready (%s)", self._backend)

    async def _setup_sqlite(self) -> None:
        try:
            import aiosqlite
        except ImportError as exc:
            raise RuntimeError(
                "aiosqlite is required for SQLite checkpointing. "
                "Run: pip install aiosqlite"
            ) from exc

        path = _sqlite_path(self.db_url)
        self._conn = await aiosqlite.connect(path)
        self._conn.row_factory = aiosqlite.Row
        for ddl in SQLITE_ALL_DDL:
            await self._conn.execute(ddl)
        await self._conn.commit()

    async def _setup_postgres(self) -> None:
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for PostgreSQL checkpointing. "
                "Run: pip install asyncpg"
            ) from exc

        self._conn = await asyncpg.connect(self.db_url)
        for ddl in (POSTGRES_CREATE_SCRAPED_URLS, POSTGRES_CREATE_CHECKPOINTS):
            await self._conn.execute(ddl)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Scraped URL tracking
    # ------------------------------------------------------------------

    async def is_scraped(self, url: str) -> bool:
        """Return True when *url* was previously scraped with status='success'."""
        if self._backend == "sqlite":
            cursor = await self._conn.execute(
                "SELECT 1 FROM scraped_urls WHERE url = ? AND status = 'success'",
                (url,),
            )
            row = await cursor.fetchone()
            return row is not None
        else:
            row = await self._conn.fetchrow(
                "SELECT 1 FROM scraped_urls WHERE url = $1 AND status = 'success'",
                url,
            )
            return row is not None

    async def mark_scraped(
        self,
        url: str,
        status: str = "success",
        site: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Record that *url* has been processed."""
        meta = metadata or {}
        if self._backend == "sqlite":
            await self._conn.execute(
                """INSERT INTO scraped_urls (url, site, status, metadata)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(url) DO UPDATE SET
                       status = excluded.status,
                       metadata = excluded.metadata,
                       scraped_at = CURRENT_TIMESTAMP""",
                (url, site, status, json.dumps(meta)),
            )
            await self._conn.commit()
        else:
            await self._conn.execute(
                """INSERT INTO scraped_urls (url, site, status, metadata)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (url) DO UPDATE SET
                       status   = EXCLUDED.status,
                       metadata = EXCLUDED.metadata,
                       scraped_at = NOW()""",
                url,
                site,
                status,
                meta,  # asyncpg maps dict -> JSONB
            )

    # ------------------------------------------------------------------
    # Category / page progress
    # ------------------------------------------------------------------

    async def save_progress(
        self,
        site: str,
        category: str,
        last_page: int = 1,
        last_product_index: int = 0,
    ) -> None:
        """Upsert the scraping progress for a (site, category) pair."""
        if self._backend == "sqlite":
            await self._conn.execute(
                """INSERT INTO checkpoints
                       (site, category, last_page, last_product_index, updated_at)
                   VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                   ON CONFLICT(site, category) DO UPDATE SET
                       last_page           = excluded.last_page,
                       last_product_index  = excluded.last_product_index,
                       updated_at          = CURRENT_TIMESTAMP""",
                (site, category, last_page, last_product_index),
            )
            await self._conn.commit()
        else:
            await self._conn.execute(
                """INSERT INTO checkpoints (site, category, last_page, last_product_index)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (site, category) DO UPDATE SET
                       last_page           = $3,
                       last_product_index  = $4,
                       updated_at          = NOW()""",
                site,
                category,
                last_page,
                last_product_index,
            )

    async def get_progress(self, site: str, category: str) -> Dict[str, int]:
        """
        Return the last saved progress for *(site, category)*.
        Returns ``{}`` if no checkpoint exists (i.e., start from scratch).
        """
        if self._backend == "sqlite":
            cursor = await self._conn.execute(
                """SELECT last_page, last_product_index
                   FROM checkpoints
                   WHERE site = ? AND category = ?""",
                (site, category),
            )
            row = await cursor.fetchone()
        else:
            row = await self._conn.fetchrow(
                """SELECT last_page, last_product_index
                   FROM checkpoints
                   WHERE site = $1 AND category = $2""",
                site,
                category,
            )

        if row:
            return {
                "last_page": row[0],
                "last_product_index": row[1],
            }
        return {}

    async def clear(self, site: Optional[str] = None) -> None:
        """
        Delete checkpoint state.  If *site* is given, only that site is cleared.
        Useful for a full re-scrape: ``python main.py --site X`` (without --resume).
        """
        if self._backend == "sqlite":
            if site:
                await self._conn.execute(
                    "DELETE FROM checkpoints WHERE site = ?", (site,)
                )
                await self._conn.execute(
                    "DELETE FROM scraped_urls WHERE site = ?", (site,)
                )
            else:
                await self._conn.execute("DELETE FROM checkpoints")
                await self._conn.execute("DELETE FROM scraped_urls")
            await self._conn.commit()
        else:
            if site:
                await self._conn.execute(
                    "DELETE FROM checkpoints WHERE site = $1", site
                )
                await self._conn.execute(
                    "DELETE FROM scraped_urls WHERE site = $1", site
                )
            else:
                await self._conn.execute("DELETE FROM checkpoints")
                await self._conn.execute("DELETE FROM scraped_urls")
