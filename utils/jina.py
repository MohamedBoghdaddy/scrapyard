"""
Jina AI Reader API fallback for fetching page content.

Used as a last resort after all aiohttp retries are exhausted.
Activated only when the JINA_API_KEY environment variable is set (loaded from
.env automatically via utils.env_loader).

Expected request format::

    POST https://r.jina.ai/
    Authorization: Bearer <JINA_API_KEY>
    Accept: application/json
    Content-Type: application/json
    {"url": "<target_url>"}
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

from utils.env_loader import get_jina_api_key, get_jina_base_url, is_jina_enabled

logger = logging.getLogger(__name__)


async def fetch_via_jina(
    url: str,
    api_key: Optional[str] = None,
    *,
    force_disabled: bool = False,
) -> Optional[str]:
    """
    Fetch URL content via Jina AI Reader API.

    Returns the extracted text/markdown content, or None on failure /
    when Jina is not configured.

    Parameters
    ----------
    url:
        Target URL to read.
    api_key:
        Override API key (defaults to JINA_API_KEY from environment).
    force_disabled:
        If True, skip even if JINA_API_KEY is set (honours --disable-jina flag).
    """
    if force_disabled:
        logger.debug("Jina AI fetch skipped – force_disabled=True")
        return None

    key = api_key or get_jina_api_key()
    if not key:
        logger.debug("Jina AI fetch skipped – JINA_API_KEY not set")
        return None

    base_url = get_jina_base_url()
    logger.info("Attempting Jina AI fallback for: %s", url)

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{base_url}/",
                headers={
                    "Authorization": f"Bearer {key}",
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                json={"url": url},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 401:
                    logger.warning(
                        "Jina AI: 401 Unauthorized – check JINA_API_KEY value"
                    )
                    return None
                if resp.status == 429:
                    logger.warning("Jina AI: 429 rate-limited for %s", url)
                    return None
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
                    logger.info(
                        "Jina AI fetched %s (%d chars)",
                        url, len(content),
                    )
                return content

    except aiohttp.ClientConnectorError as exc:
        logger.warning("Jina AI connection error for %s: %s", url, exc)
        return None
    except Exception as exc:
        logger.warning("Jina AI fetch failed for %s: %s", url, exc)
        return None


def log_jina_status() -> None:
    """
    Emit a startup log line indicating whether Jina is available.
    Called once at the start of each scrape run.
    """
    if is_jina_enabled():
        logger.info(
            "Jina AI fallback: ENABLED  (key present, base_url=%s)",
            get_jina_base_url(),
        )
    else:
        logger.info(
            "Jina AI fallback: DISABLED  (set JINA_API_KEY in .env to enable)"
        )
