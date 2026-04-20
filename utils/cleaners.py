"""
General-purpose data cleaning utilities (no imports from scrapers/).
"""
from __future__ import annotations

import json
import re
import unicodedata
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Price cleaning
# ---------------------------------------------------------------------------

_CURRENCY_RE = re.compile(
    r"(LE|EGP|USD|EUR|SAR|AED|£|\$|€|﷼|,(?=\d{3}))", re.IGNORECASE
)
_SALE_RE = re.compile(r"sale\s+price[:\s]*", re.IGNORECASE)
_ARABIC_DIGIT_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def _arabic_to_ascii_digits(text: str) -> str:
    return text.translate(_ARABIC_DIGIT_MAP)


def clean_price(price_str: str) -> Optional[float]:
    """
    Parse a raw price string into a float.

    Handles:
      - "LE 750.00 EGP"     -> 750.0
      - "Sale Price: 1,200"  -> 1200.0
      - "From $19.99"        -> 19.99
      - Arabic-Indic digits (٠١٢٣٤٥٦٧٨٩)
    Returns None when parsing fails.
    """
    if not price_str:
        return None
    text = price_str.strip()
    text = _arabic_to_ascii_digits(text)
    text = _SALE_RE.sub("", text)
    text = _CURRENCY_RE.sub("", text)
    text = re.sub(r"\s+", "", text)
    match = re.search(r"\d+\.?\d*", text)
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Part number cleaning
# ---------------------------------------------------------------------------

_PART_LABEL_RE = re.compile(
    r"(part\s*(no\.?|number|#)|sku|oem|ref\.?)[:\s]*", re.IGNORECASE
)


def clean_part_number(part_str: str) -> str:
    """Strip common labels and normalise whitespace from a part number string."""
    if not part_str:
        return ""
    text = _PART_LABEL_RE.sub("", part_str).strip()
    return re.sub(r"\s+", " ", text)


# ---------------------------------------------------------------------------
# Arabic text utilities
# ---------------------------------------------------------------------------


def normalise_arabic(text: str) -> str:
    """
    Lightly normalise Arabic text:
      - Strip diacritics (tashkeel)
      - Normalise whitespace
    """
    if not text:
        return ""
    cleaned = "".join(
        ch for ch in unicodedata.normalize("NFC", text)
        if unicodedata.category(ch) != "Mn"
    )
    return re.sub(r"\s+", " ", cleaned).strip()


# ---------------------------------------------------------------------------
# General text
# ---------------------------------------------------------------------------


def clean_text(text: str, *, arabic: bool = False) -> str:
    """Strip and collapse whitespace; optionally run Arabic normalisation."""
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    if arabic:
        text = normalise_arabic(text)
    return text


def clean_url(url: str) -> str:
    """Strip query tracking params from a URL."""
    cleaned = re.sub(r"[?&](utm_\w+|ref|source|fbclid|gclid)=[^&]*", "", url)
    return cleaned.rstrip("?&")


def to_slug(text: str) -> str:
    """Convert text to a URL-safe slug."""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s-]", "", text).strip().lower()
    return re.sub(r"[\s_-]+", "-", text)


# ---------------------------------------------------------------------------
# CSS selector cascade helper
# ---------------------------------------------------------------------------


def first_match(
    soup,
    selectors: List[Tuple[str, Optional[str]]],
) -> Optional[str]:
    """
    Try CSS selectors in order; return the first non-empty result.

    Each entry is a (selector, attribute) pair:
      - If attribute is None, return the element's text content.
      - If attribute is a string (e.g. "href", "src"), return that attribute.

    Example::

        title = first_match(soup, [
            (".product-title", None),
            ("h1.title", None),
            ('meta[property="og:title"]', "content"),
        ])
    """
    for sel, attr in selectors:
        el = soup.select_one(sel)
        if el:
            result = el.get(attr) if attr else el.get_text(strip=True)
            if result:
                return str(result).strip()
    return None
