"""
Page-type auto-detector.

Classifies a raw HTML response body into one of three categories:

  static      – server-rendered HTML, fully parseable with BeautifulSoup
  spa         – Single-Page Application shell (React / Vue / Angular / Next.js)
                The visible product content lives in JS bundles, not the HTML.
  api_driven  – Page embeds structured data (Next.js __NEXT_DATA__, Nuxt,
                Shopify JSON, JSON-LD) that can be parsed directly without
                executing JS.

Decision order (first match wins):
  1. api_driven  – embedded JSON payloads with rich product data
  2. spa         – empty root div + SPA framework fingerprints
  3. static      – everything else
"""
from __future__ import annotations

import re
from enum import Enum
from typing import Optional


class PageType(str, Enum):
    STATIC = "static"
    SPA = "spa"
    API_DRIVEN = "api_driven"
    UNKNOWN = "unknown"


# ── API / embedded-data patterns ────────────────────────────────────────────
_API_PATTERNS = [
    # Next.js data island
    re.compile(r'<script[^>]+id=["\']__NEXT_DATA__["\']', re.I),
    # Nuxt
    re.compile(r'window\.__NUXT__\s*=', re.I),
    # Shopify embedded product/cart JSON
    re.compile(r'var\s+meta\s*=\s*\{[^}]*"product"', re.I),
    re.compile(r'var\s+product\s*=\s*\{[^}]*"variants"', re.I),
    re.compile(r'"product":\s*\{[^}]*"variants"', re.I),
    # JSON-LD product schema
    re.compile(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>.*?"@type"\s*:\s*"Product"',
        re.I | re.S,
    ),
    # Embedded Redux/Vuex store with product data
    re.compile(r'window\.__REDUX_STATE__\s*=', re.I),
    re.compile(r'window\.__INITIAL_STATE__\s*=', re.I),
    # Generic pageProps (Next.js SSR)
    re.compile(r'"pageProps"\s*:\s*\{[^}]{0,200}"product', re.I | re.S),
]

# ── SPA skeleton patterns ────────────────────────────────────────────────────
_SPA_PATTERNS = [
    # React / CRA root
    re.compile(r'<div\s+id=["\']root["\']>\s*</div>', re.I),
    # Vue / Nuxt app root
    re.compile(r'<div\s+id=["\']app["\']>\s*</div>', re.I),
    # Angular
    re.compile(r'<app-root[^>]*>\s*</app-root>', re.I),
    re.compile(r'ng-app\s*=', re.I),
    # Webpack / Vite bundles with no SSR content
    re.compile(r'<script[^>]+src=["\'][^"\']*(?:main\.|app\.|chunk\.|bundle\.)[^"\']*\.js["\']', re.I),
    # Generic empty content area
    re.compile(r'<div\s+id=["\'](?:root|app|main|content)["\']>\s*</div>', re.I),
]

# How many chars from the beginning of the HTML to scan (faster than full body)
_SCAN_LIMIT = 30_000


def detect_page_type(
    html: str,
    config: Optional[dict] = None,
) -> PageType:
    """
    Infer the rendering strategy used by an HTML page.

    Parameters
    ----------
    html:
        Raw HTML string (full page or partial).
    config:
        Optional site config dict; if ``use_javascript`` is set it biases
        the result toward SPA when no other signals are found.

    Returns
    -------
    PageType enum member.
    """
    if not html:
        return PageType.UNKNOWN

    cfg = config or {}
    sample = html[:_SCAN_LIMIT]

    # 1. API-driven: embedded structured data is the richest signal
    for pattern in _API_PATTERNS:
        if pattern.search(sample):
            return PageType.API_DRIVEN

    # 2. SPA: empty shell + framework fingerprints
    spa_hits = sum(1 for pat in _SPA_PATTERNS if pat.search(sample))
    if spa_hits >= 2:
        return PageType.SPA

    # 3. Config hint: site explicitly requires JS rendering
    if cfg.get("use_javascript") or cfg.get("engine") == "playwright":
        # But only call it SPA if the page looks thin on text
        visible_text_len = len(re.sub(r"<[^>]+>", " ", sample).split())
        if visible_text_len < 80:
            return PageType.SPA

    return PageType.STATIC


def recommend_engine(page_type: PageType) -> str:
    """Return the recommended scraping engine for a given page type."""
    return {
        PageType.STATIC: "http",
        PageType.API_DRIVEN: "http",       # parse embedded JSON, no JS needed
        PageType.SPA: "playwright",
        PageType.UNKNOWN: "http",
    }[page_type]
