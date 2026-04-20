"""
Jina AI Reader API fallback for fetching page content.

Used as a last resort after all aiohttp retries are exhausted.
Activated only when the JINA_API_KEY environment variable is set.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)


async def fetch_via_jina(url: str, api_key: Optional[str] = None) -> Optional[str]:
    """
    Fetch URL content via Jina AI Reader API (https://r.jina.ai/).

    Returns the extracted text/markdown content, or None on failure.
    The API converts any URL to clean, LLM-friendly text.
    """
    key = api_key or os.environ.get("JINA_API_KEY", "")
    if not key:
        logger.debug("Jina AI fetch skipped – JINA_API_KEY not set")
        return None

    logger.info("Attempting Jina AI fallback for: %s", url)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                "https://r.jina.ai/",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json={"url": url},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning(
                        "Jina AI returned HTTP %d for %s: %s",
                        resp.status, url, body[:200],
                    )
                    return None
                data = await resp.json(content_type=None)
                content = data.get("data", {}).get("content")
                if content:
                    logger.info("Jina AI successfully fetched %s (%d chars)", url, len(content))
                return content
    except Exception as exc:
        logger.warning("Jina AI fetch failed for %s: %s", url, exc)
        return None
