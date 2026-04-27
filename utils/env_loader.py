"""
Environment / .env loader for Scrapyard.

Loads .env (and .env.local) via python-dotenv, then exposes typed accessors
for every env variable the platform uses.  All accessors return safe defaults
so the rest of the code never crashes on a missing variable.

Usage::

    from utils.env_loader import get_jina_api_key, is_jina_enabled

    if is_jina_enabled():
        key = get_jina_api_key()
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── dotenv loading ────────────────────────────────────────────────────────────

def _load_dotenv() -> bool:
    """
    Try to load .env and .env.local from the repository root.
    Returns True if at least one file was loaded.
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except ImportError:
        logger.debug("python-dotenv not installed; reading environment variables directly")
        return False

    repo_root = Path(__file__).resolve().parents[1]
    loaded = False
    for name in (".env", ".env.local"):
        env_file = repo_root / name
        if env_file.exists():
            load_dotenv(env_file, override=False)
            logger.debug("Loaded env from %s", env_file)
            loaded = True

    return loaded


# Load once at import time — idempotent
_loaded = _load_dotenv()

# ── Accessors ─────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def get_jina_api_key() -> str:
    """Return JINA_API_KEY or empty string if not set."""
    return os.environ.get("JINA_API_KEY", "").strip()


@lru_cache(maxsize=1)
def get_jina_base_url() -> str:
    """Return JINA_BASE_URL (defaults to https://r.jina.ai/)."""
    return os.environ.get("JINA_BASE_URL", "https://r.jina.ai/").rstrip("/")


@lru_cache(maxsize=1)
def is_jina_enabled() -> bool:
    """True when a non-empty JINA_API_KEY is present in the environment."""
    enabled = bool(get_jina_api_key())
    if enabled:
        logger.info("Jina AI fallback: ENABLED (key loaded from env)")
    else:
        logger.debug("Jina AI fallback: DISABLED (JINA_API_KEY not set)")
    return enabled


@lru_cache(maxsize=1)
def get_openai_api_key() -> str:
    return os.environ.get("OPENAI_API_KEY", "").strip()


@lru_cache(maxsize=1)
def get_slack_webhook_url() -> str:
    return os.environ.get("SLACK_WEBHOOK_URL", "").strip()


@lru_cache(maxsize=1)
def get_database_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


@lru_cache(maxsize=1)
def get_playwright_ws() -> str:
    """Remote Browserless/Playwright endpoint (optional)."""
    return os.environ.get("PLAYWRIGHT_WS", "").strip()


def validate_env() -> dict:
    """
    Return a diagnostic dict showing which keys are present.
    Safe to log (never reveals actual key values).
    """
    return {
        "dotenv_loaded": _loaded,
        "JINA_API_KEY":      "set" if get_jina_api_key()      else "missing",
        "OPENAI_API_KEY":    "set" if get_openai_api_key()    else "missing",
        "SLACK_WEBHOOK_URL": "set" if get_slack_webhook_url() else "missing",
        "DATABASE_URL":      "set" if get_database_url()      else "missing",
        "PLAYWRIGHT_WS":     "set" if get_playwright_ws()     else "missing",
    }
