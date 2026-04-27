"""
Shared helpers for enriching product detail pages across scraper engines.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Sequence

from bs4 import BeautifulSoup

from utils.cleaners import (
    clean_part_number,
    clean_text,
    extract_compatibility_text,
    extract_labeled_values,
    extract_oem_references,
    extract_part_number,
    find_spec_value,
    find_spec_values,
)

_BRAND_LABELS = (
    "brand",
    "manufacturer",
    "make",
    "vendor",
    "الماركة",
    "العلامة التجارية",
    "الشركة المصنعة",
)

_PART_NUMBER_LABELS = (
    "part number",
    "part no",
    "part #",
    "sku",
    "mpn",
    "oem",
    "reference",
    "ref",
    "رقم القطعة",
    "رقم الصنف",
    "رقم المنتج",
    "الكود",
)

_OEM_LABELS = (
    "oem",
    "oe",
    "oem number",
    "oe number",
    "reference",
    "ref",
    "cross reference",
    "رقم المرجع",
)

_COMPATIBILITY_LABELS = (
    "compatibility",
    "fitment",
    "fits",
    "suitable for",
    "vehicle",
    "application",
    "compatible with",
    "التوافق",
    "متوافق",
    "يناسب",
    "السيارات",
)

_DEFAULT_DESCRIPTION_SELECTORS = (
    ".product__description",
    "#product-description",
    ".product-description",
    "#product-desc",
    ".description",
    "[itemprop='description']",
    ".accordion-body",
    ".tab-pane",
    "meta[name='description']",
    "meta[property='og:description']",
)

_DEFAULT_COMPATIBILITY_SELECTORS = (
    ".compatibility",
    "#compatibility",
    "[class*='compatib']",
    "[id*='compatib']",
    ".fitment",
    "#fitment",
    "[class*='fitment']",
    "[id*='fitment']",
    ".vehicle-compatibility",
    ".applications",
)

_DEFAULT_OEM_SELECTORS = (
    ".oem",
    "#oem",
    "[class*='oem']",
    "[id*='oem']",
    ".reference",
    "#reference",
    "[class*='refer']",
    "[id*='refer']",
    ".part-number",
    ".sku",
)

_DESCRIPTION_EXCLUDE_KEYWORDS = (
    "quick view",
    "add to cart",
    "add to wishlist",
    "add to compare",
    "ask a question",
    "wishlist",
    "compare",
    "نظرة سريعة",
    "اضافة للسلة",
    "إضافة للسلة",
    "اضافة لرغباتي",
    "إضافة لرغباتي",
    "اضافة للمقارنة",
    "إضافة للمقارنة",
    "اطرح سؤالا",
    "أطرح سؤالا",
)


def has_meaningful_value(value: Any) -> bool:
    """Return True when *value* should overwrite an existing product field."""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) > 0
    return True


def merge_product_payloads(
    base: Optional[Dict[str, Any]],
    extra: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge two product dicts without allowing empty values to erase good data."""
    merged = dict(base or {})
    if not extra:
        return merged

    for key, value in extra.items():
        if key == "specifications" and isinstance(value, dict):
            existing = merged.get(key)
            combined = dict(existing) if isinstance(existing, dict) else {}
            for spec_key, spec_value in value.items():
                if has_meaningful_value(spec_value):
                    combined[str(spec_key)] = spec_value
            if combined:
                merged[key] = combined
            continue

        if key in {"oem_references", "variants", "tags"} and isinstance(value, list):
            existing_items = merged.get(key)
            combined_items: List[Any] = []
            if isinstance(existing_items, list):
                combined_items.extend(existing_items)
            combined_items.extend(item for item in value if has_meaningful_value(item))
            if combined_items:
                unique: List[Any] = []
                for item in combined_items:
                    if item not in unique:
                        unique.append(item)
                merged[key] = unique
            continue

        if has_meaningful_value(value):
            merged[key] = value

    return merged


def _dedupe_texts(values: Iterable[str]) -> List[str]:
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


def _clean_node_text(value: Any, *, arabic: bool) -> str:
    return clean_text(str(value or ""), arabic=arabic)


def _first_non_empty(values: Sequence[str]) -> str:
    for value in values:
        if value and str(value).strip():
            return str(value).strip()
    return ""


def _is_noisy_description(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(keyword in lowered for keyword in _DESCRIPTION_EXCLUDE_KEYWORDS)


def build_page_text(soup: BeautifulSoup, *, arabic: bool = False) -> str:
    """Preserve line-like text chunks for downstream labeled extraction."""
    chunks = [_clean_node_text(text, arabic=arabic) for text in soup.stripped_strings]
    return "\n".join(_dedupe_texts(chunks))


def collect_text_candidates(
    soup: BeautifulSoup,
    selectors: Sequence[str],
    *,
    arabic: bool = False,
    min_length: int = 6,
    max_length: int = 4000,
) -> List[str]:
    values: List[str] = []
    for selector in selectors:
        for element in soup.select(selector):
            raw = element.get("content") if getattr(element, "name", "") == "meta" else element.get_text(" ", strip=True)
            cleaned = _clean_node_text(raw, arabic=arabic)
            if min_length <= len(cleaned) <= max_length:
                values.append(cleaned)
    return _dedupe_texts(values)


def extract_best_text(
    soup: BeautifulSoup,
    selectors: Sequence[str],
    *,
    arabic: bool = False,
    min_length: int = 20,
) -> str:
    candidates = collect_text_candidates(
        soup,
        selectors,
        arabic=arabic,
        min_length=min_length,
    )
    filtered = [candidate for candidate in candidates if not _is_noisy_description(candidate)]
    if filtered:
        candidates = filtered
    if not candidates:
        return ""
    return max(candidates, key=len)


def extract_specifications(
    soup: BeautifulSoup,
    *,
    arabic: bool = False,
) -> Dict[str, str]:
    """Collect key/value specs from common table, definition-list, and list layouts."""
    specs: Dict[str, str] = {}

    table_selector = (
        "table.product-specs tr, .specifications tr, table.specs tr, "
        ".product-specs tr, .spec-table tr, .shop_attributes tr, "
        ".additional-information tr, table tr"
    )
    for row in soup.select(table_selector):
        cells = row.select("td, th")
        if len(cells) < 2:
            continue
        key = _clean_node_text(cells[0].get_text(" ", strip=True), arabic=arabic)
        value = _clean_node_text(cells[1].get_text(" ", strip=True), arabic=arabic)
        if key and value and key not in specs:
            specs[key] = value

    for dl in soup.select("dl"):
        terms = dl.find_all("dt")
        definitions = dl.find_all("dd")
        for term, definition in zip(terms, definitions):
            key = _clean_node_text(term.get_text(" ", strip=True), arabic=arabic)
            value = _clean_node_text(definition.get_text(" ", strip=True), arabic=arabic)
            if key and value and key not in specs:
                specs[key] = value

    list_selector = (
        "ul.product-meta li, .product-meta li, .product-info li, .product-details li, "
        ".accordion-body li, .tab-pane li, .product__info li"
    )
    for item in soup.select(list_selector):
        text = _clean_node_text(item.get_text(" ", strip=True), arabic=arabic)
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        key = key.strip()
        value = value.strip()
        if key and value and key not in specs:
            specs[key] = value

    return specs


def enrich_product_fields(
    soup: BeautifulSoup,
    *,
    description: str = "",
    vendor: str = "",
    part_number: str = "",
    specs: Optional[Dict[str, str]] = None,
    extra_texts: Optional[Sequence[str]] = None,
    arabic: bool = False,
) -> Dict[str, Any]:
    """Infer richer product fields from the page body, specs, and metadata text."""
    resolved_specs = specs or extract_specifications(soup, arabic=arabic)
    page_text = build_page_text(soup, arabic=arabic)
    extra_text_values = [str(value) for value in (extra_texts or []) if value]
    human_text_values = [
        value
        for value in extra_text_values
        if not value.lstrip().startswith(("{", "["))
    ]

    description_candidates = [description]
    description_candidates.extend(
        collect_text_candidates(
            soup,
            _DEFAULT_DESCRIPTION_SELECTORS,
            arabic=arabic,
            min_length=20,
        )
    )
    description_candidates = _dedupe_texts(
        _clean_node_text(value, arabic=arabic) for value in description_candidates if value
    )
    filtered_description_candidates = [
        candidate
        for candidate in description_candidates
        if not _is_noisy_description(candidate)
    ]
    if filtered_description_candidates:
        description_candidates = filtered_description_candidates
    resolved_description = max(description_candidates, key=len) if description_candidates else ""

    vendor_candidates = [vendor]
    vendor_candidates.append(find_spec_value(resolved_specs, _BRAND_LABELS))
    vendor_candidates.extend(extract_labeled_values(page_text, _BRAND_LABELS, max_length=120))
    vendor_candidates = [
        _clean_node_text(value, arabic=arabic)
        for value in vendor_candidates
        if value
    ]
    resolved_vendor = _first_non_empty(vendor_candidates)

    resolved_part_number = clean_part_number(part_number)
    if not resolved_part_number:
        spec_part_values = find_spec_values(resolved_specs, _PART_NUMBER_LABELS)
        for candidate in spec_part_values:
            resolved_part_number = clean_part_number(candidate) or extract_part_number(candidate)
            if resolved_part_number:
                break
    if not resolved_part_number:
        part_sources = extra_text_values + [resolved_description, page_text]
        for source in part_sources:
            resolved_part_number = extract_part_number(source)
            if resolved_part_number:
                break

    compatibility_sources: List[str] = []
    compatibility_sources.extend(find_spec_values(resolved_specs, _COMPATIBILITY_LABELS))
    compatibility_sources.extend(
        collect_text_candidates(
            soup,
            _DEFAULT_COMPATIBILITY_SELECTORS,
            arabic=arabic,
            min_length=20,
        )
    )
    compatibility_sources.extend(human_text_values)
    compatibility_sources.extend([resolved_description, page_text])
    resolved_compatibility = ""
    for source in compatibility_sources:
        resolved_compatibility = extract_compatibility_text(source)
        if resolved_compatibility:
            break

    oem_sources: List[str] = []
    oem_sources.extend(find_spec_values(resolved_specs, _OEM_LABELS))
    oem_sources.extend(
        collect_text_candidates(
            soup,
            _DEFAULT_OEM_SELECTORS,
            arabic=arabic,
            min_length=6,
        )
    )
    oem_sources.extend(human_text_values)
    resolved_oem_references = extract_oem_references(
        "\n".join([resolved_description, page_text]),
        extra_values=oem_sources,
    )

    return {
        "description": resolved_description,
        "vendor": resolved_vendor,
        "part_number": resolved_part_number,
        "compatibility_text": resolved_compatibility,
        "oem_references": resolved_oem_references,
        "specifications": resolved_specs,
    }
