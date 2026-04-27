"""
Scrapyard extractors package.

Intelligent content targeting helpers:
  dom_scorer         – content quality / density scoring for BeautifulSoup trees
  page_type_detector – classify a raw HTML body as static / spa / api_driven
"""
from .dom_scorer import score_content_quality, ContentQuality
from .page_type_detector import detect_page_type, PageType

__all__ = ["score_content_quality", "ContentQuality", "detect_page_type", "PageType"]
