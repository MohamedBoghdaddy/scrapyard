"""
Language detection using langdetect with a fast regex pre-pass for Arabic.
Falls back gracefully when the library is unavailable.
"""
from __future__ import annotations

import re
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

_ARABIC_RE = re.compile(r"[؀-ۿ]")
_MIN_CHARS = 15


def detect_language(text: str) -> str:
    """Return an ISO 639-1 language code ('ar', 'en', …) or 'unknown'."""
    if not text or len(text.strip()) < _MIN_CHARS:
        return "unknown"

    sample = text.strip()[:800]

    # Fast pre-pass: if >30 % of alphabetic chars are Arabic, skip langdetect
    alpha = [c for c in sample if c.isalpha()]
    if alpha:
        arabic_ratio = sum(1 for c in alpha if _ARABIC_RE.match(c)) / len(alpha)
        if arabic_ratio > 0.30:
            return "ar"

    try:
        from langdetect import detect, LangDetectException  # type: ignore

        return detect(sample)
    except ImportError:
        logger.debug("langdetect not installed; using regex-only language detection")
        return "ar" if _ARABIC_RE.search(sample) else "en"
    except Exception:  # LangDetectException or any other
        return "unknown"


@lru_cache(maxsize=1)
def _langdetect_available() -> bool:
    try:
        import langdetect  # noqa: F401
        return True
    except ImportError:
        return False
