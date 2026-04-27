"""
General-purpose data cleaning utilities (no imports from scrapers/).
"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

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
_LABEL_NORMALISER_RE = re.compile(r"[^0-9A-Za-z\u0621-\u064A]+")
_PART_TOKEN_RE = re.compile(r"[A-Z0-9][A-Z0-9._/-]{2,}")
_YEAR_RE = re.compile(r"\b(?:19|20)\d{2}\b")

_DEFAULT_PART_LABELS = (
    "part number",
    "part no",
    "part #",
    "sku",
    "mpn",
    "oem",
    "oe",
    "reference",
    "ref",
    "رقم القطعة",
    "رقم الصنف",
    "رقم المنتج",
    "رقم المرجع",
    "الكود",
)

_DEFAULT_OEM_LABELS = (
    "oem",
    "oe",
    "oem number",
    "oe number",
    "reference",
    "ref",
    "cross reference",
    "manufacturer part number",
    "رقم المرجع",
    "رقم oem",
)

_DEFAULT_COMPATIBILITY_LABELS = (
    "compatibility",
    "fitment",
    "fits",
    "suitable for",
    "vehicle",
    "application",
    "applicable models",
    "compatible with",
    "car model",
    "التوافق",
    "متوافق",
    "يناسب",
    "مناسب",
    "السيارات",
    "الموديلات",
)

_COMPATIBILITY_KEYWORDS = (
    "compatibility",
    "fitment",
    "fits",
    "suitable for",
    "vehicle",
    "application",
    "compatible with",
    "car model",
    "التوافق",
    "متوافق",
    "يناسب",
    "مناسب",
    "السيارات",
    "الموديلات",
)

_COMPATIBILITY_EXCLUDE_KEYWORDS = (
    "rights reserved",
    "all rights reserved",
    "copyright",
    "privacy policy",
    "terms and conditions",
    "wishlist",
    "quick view",
    "add to cart",
    "Ø¬Ù…ÙŠØ¹ Ø§Ù„Ø­Ù‚ÙˆÙ‚ Ù…Ø­ÙÙˆØ¸Ø©",
    "Ø³ÙŠØ§Ø³Ø© Ø§Ù„Ø®ØµÙˆØµÙŠØ©",
    "Ø§Ù„Ø´Ø±ÙˆØ· ÙˆØ§Ù„Ø§Ø­ÙƒØ§Ù…",
    "Ø§Ù„Ø´Ø±ÙˆØ· ÙˆØ§Ù„Ø£Ø­ÙƒØ§Ù…",
)


def clean_part_number(part_str: str) -> str:
    """Normalize part numbers for cross-vendor matching."""
    if not part_str:
        return ""
    text = _arabic_to_ascii_digits(str(part_str))
    text = _PART_LABEL_RE.sub("", text)
    text = re.sub(r"[\u200e\u200f\u061c]", "", text)
    text = re.sub(r"[\s\-_/]+", "", text)
    return text.upper().strip()


def generate_canonical_id(part_number: str, category: str = "") -> str:
    """Generate a stable identifier for product matching."""
    key = f"{clean_part_number(part_number)}|{normalise_arabic(category).lower()}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()[:16]


def _normalise_multiline_text(text: str) -> str:
    raw = _arabic_to_ascii_digits(str(text or ""))
    raw = re.sub(r"[\u200e\u200f\u061c]", "", raw)
    lines = [normalise_arabic(chunk) for chunk in re.split(r"[\r\n]+", raw)]
    return "\n".join(line for line in lines if line)


def _normalise_label(text: str) -> str:
    cleaned = _normalise_multiline_text(text).lower().replace("\n", " ")
    cleaned = _LABEL_NORMALISER_RE.sub(" ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _make_label_pattern(label: str) -> str:
    tokens = re.findall(r"[0-9A-Za-z\u0621-\u064A]+", _arabic_to_ascii_digits(str(label)))
    return r"[\s._-]*".join(re.escape(token) for token in tokens)


def _dedupe_strings(values: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    unique: List[str] = []
    for value in values:
        if not value:
            continue
        cleaned = str(value).strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        unique.append(cleaned)
    return unique


def _trim_labeled_value(value: str) -> str:
    trimmed = re.split(
        r"\s{2,}|[|•]+|(?:\b(?:price|brand|vendor|sku|mpn|oem|reference|compatibility|fitment|description)\b\s*[:#-])",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return trimmed.strip(" :-#|,;")


def _split_candidate_tokens(value: str) -> List[str]:
    parts = re.split(r"[,;/|\u060C]|\b(?:and|or)\b", value, flags=re.IGNORECASE)
    if len(parts) == 1:
        parts = re.split(r"\s{2,}", value)
    return [part.strip() for part in parts if part.strip()]


def find_spec_values(specs: Dict[str, str], labels: Sequence[str]) -> List[str]:
    """Return all matching specification values for the provided label aliases."""
    if not specs:
        return []

    aliases = [_normalise_label(label) for label in labels if label]
    matches: List[str] = []
    for key, value in specs.items():
        key_norm = _normalise_label(str(key))
        if not key_norm:
            continue
        if any(alias == key_norm or alias in key_norm or key_norm in alias for alias in aliases):
            cleaned = clean_text(str(value), arabic=bool(re.search(r"[\u0621-\u064A]", str(value))))
            if cleaned:
                matches.append(cleaned)
    return _dedupe_strings(matches)


def find_spec_value(specs: Dict[str, str], labels: Sequence[str]) -> str:
    """Return the first specification value matching one of the provided labels."""
    values = find_spec_values(specs, labels)
    return values[0] if values else ""


def extract_labeled_values(
    text: str,
    labels: Sequence[str],
    *,
    max_length: int = 180,
) -> List[str]:
    """Extract values that appear after common product labels such as SKU or OEM."""
    if not text or not labels:
        return []

    cleaned = _normalise_multiline_text(text)
    chunks = [chunk.strip() for chunk in re.split(r"[\r\n|•]+", cleaned) if chunk.strip()]
    if len(chunks) == 1:
        chunks.extend(part.strip() for part in re.split(r"\s{3,}", cleaned) if part.strip())

    patterns = []
    for label in labels:
        label_pattern = _make_label_pattern(label)
        if label_pattern:
            patterns.append(
                re.compile(
                    rf"(?:^|[\s(]){label_pattern}\s*[:#-]?\s*(.+)$",
                    re.IGNORECASE,
                )
            )

    values: List[str] = []
    for chunk in chunks:
        for pattern in patterns:
            match = pattern.search(chunk)
            if not match:
                continue
            value = _trim_labeled_value(match.group(1))
            if not value:
                continue
            if len(value) > max_length:
                value = value[:max_length].rsplit(" ", 1)[0].strip()
            if value:
                values.append(value)
    return _dedupe_strings(values)


def extract_part_number(text: str, labels: Optional[Sequence[str]] = None) -> str:
    """Extract a likely part number from labeled text or specification prose."""
    search_labels = labels or _DEFAULT_PART_LABELS
    for value in extract_labeled_values(text, search_labels, max_length=80):
        for token in _PART_TOKEN_RE.findall(_arabic_to_ascii_digits(value).upper()):
            cleaned = clean_part_number(token)
            if len(cleaned) >= 4 and not cleaned.isdigit():
                return cleaned

    cleaned_text = _normalise_multiline_text(text)
    match = re.search(
        r"(?:part\s*(?:no\.?|number|#)|sku|mpn|oem|oe|ref\.?|reference|رقم\s*(?:القطعة|الصنف|المنتج|المرجع)|الكود)\s*[:#-]?\s*([A-Z0-9][A-Z0-9._/-]{2,})",
        cleaned_text,
        re.IGNORECASE,
    )
    if match:
        return clean_part_number(match.group(1))
    return ""


def extract_oem_references(
    text: str,
    *,
    extra_values: Optional[Sequence[str]] = None,
) -> List[str]:
    """Extract normalized OEM/reference numbers from labeled content."""
    values: List[str] = []
    values.extend(extract_labeled_values(text, _DEFAULT_OEM_LABELS, max_length=220))
    if extra_values:
        values.extend(str(value) for value in extra_values if value)

    references: List[str] = []
    for value in values:
        for part in _split_candidate_tokens(_normalise_multiline_text(value)):
            for token in _PART_TOKEN_RE.findall(_arabic_to_ascii_digits(part).upper()):
                cleaned = clean_part_number(token)
                if len(cleaned) < 4:
                    continue
                references.append(cleaned)
    return _dedupe_strings(references)[:10]


def _looks_like_compatibility(text: str) -> bool:
    cleaned = _normalise_multiline_text(text)
    if not cleaned:
        return False
    normalised = _normalise_label(cleaned)
    if any(keyword in normalised for keyword in _COMPATIBILITY_EXCLUDE_KEYWORDS):
        return False
    word_count = len(normalised.split())
    has_year = bool(_YEAR_RE.search(cleaned))
    has_keyword = any(keyword in normalised for keyword in _COMPATIBILITY_KEYWORDS)
    if has_keyword and word_count >= 3:
        return True
    if not has_year:
        return False
    return bool(
        re.search(
            r"[A-Za-z\u0621-\u064A]{2,}\s+[A-Za-z0-9\u0621-\u064A-]{1,}\s+(?:19|20)\d{2}",
            cleaned,
        )
    )


def extract_compatibility_text(text: str, *, max_length: int = 320) -> str:
    """Extract the most likely raw fitment/compatibility text block."""
    if not text:
        return ""

    for value in extract_labeled_values(
        text,
        _DEFAULT_COMPATIBILITY_LABELS,
        max_length=max_length,
    ):
        if _looks_like_compatibility(value):
            return value[:max_length].strip()

    cleaned = _normalise_multiline_text(text)
    chunks = [chunk.strip() for chunk in re.split(r"[\r\n|•]+", cleaned) if chunk.strip()]
    for chunk in chunks:
        if _looks_like_compatibility(chunk):
            return chunk[:max_length].strip(" :-#|")
    return ""


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
