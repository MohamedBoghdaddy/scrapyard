"""
NLP enrichment pipeline for Scrapyard.

Stages (all toggle-able via NLPConfig):
  1. language detection  → product["language"]
  2. keyword extraction  → product["keywords"]  (comma-joined string)
  3. summarisation       → product["ai_summary"]
  4. topic classification → product["topic_category"]

The pipeline is deliberately synchronous so it can be called from both sync
and async contexts without needing an event loop.  For large batches, wrap
the call in asyncio.to_thread() or a ThreadPoolExecutor.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .language_detector import detect_language
from .keyword_extractor import extract_keywords
from .summarizer import summarize
from .classifier import classify_topic, confidence_score

logger = logging.getLogger(__name__)


@dataclass
class NLPConfig:
    """Feature toggles and tuning knobs for the NLP pipeline."""
    language: bool = True
    keywords: bool = True
    summarize: bool = True
    classify: bool = True
    max_keywords: int = 8
    summary_sentences: int = 2
    min_text_length: int = 30   # skip NLP when combined text is shorter


def _build_input_text(product: Dict[str, Any]) -> str:
    """Concatenate the richest textual fields available in a product dict."""
    parts = [
        str(product.get("name") or ""),
        str(product.get("description") or ""),
        str(product.get("compatibility_text") or ""),
        str(product.get("category") or ""),
    ]
    return " ".join(p for p in parts if p.strip())


def enrich_product_nlp(
    product: Dict[str, Any],
    config: Optional[NLPConfig] = None,
) -> Dict[str, Any]:
    """
    Run the NLP pipeline on a single product dict and return a *new* dict with
    extra fields added.  Original fields are never overwritten.

    New fields added:
      - language        : ISO 639-1 code ('ar', 'en', …) or 'unknown'
      - keywords        : comma-separated keyword phrases
      - ai_summary      : 1-2 sentence extractive summary
      - topic_category  : automotive sub-topic label
    """
    cfg = config or NLPConfig()
    text = _build_input_text(product)

    if len(text.strip()) < cfg.min_text_length:
        return {
            **product,
            "language": "unknown",
            "keywords": "",
            "ai_summary": "",
            "topic_category": "general_auto_parts",
        }

    enriched: Dict[str, Any] = dict(product)

    # 1. Language detection
    language = "unknown"
    if cfg.language:
        try:
            language = detect_language(text)
        except Exception as exc:
            logger.debug("Language detection error: %s", exc)
    enriched["language"] = language

    # 2. Keyword extraction
    keywords: List[str] = []
    if cfg.keywords:
        try:
            keywords = extract_keywords(text, language=language, max_keywords=cfg.max_keywords)
        except Exception as exc:
            logger.debug("Keyword extraction error: %s", exc)
    enriched["keywords"] = ", ".join(keywords)

    # 3. Summarisation
    if cfg.summarize:
        try:
            enriched["ai_summary"] = summarize(
                str(product.get("description") or text),
                language=language,
                num_sentences=cfg.summary_sentences,
            )
        except Exception as exc:
            logger.debug("Summarisation error: %s", exc)
            enriched["ai_summary"] = ""
    else:
        enriched["ai_summary"] = ""

    # 4. Topic classification
    if cfg.classify:
        try:
            enriched["topic_category"] = classify_topic(
                text,
                category=str(product.get("category") or ""),
                part_number=str(product.get("part_number") or ""),
            )
        except Exception as exc:
            logger.debug("Classification error: %s", exc)
            enriched["topic_category"] = "general_auto_parts"
    else:
        enriched["topic_category"] = "general_auto_parts"

    return enriched


def enrich_batch_nlp(
    products: List[Dict[str, Any]],
    config: Optional[NLPConfig] = None,
) -> List[Dict[str, Any]]:
    """Apply enrich_product_nlp to every item in *products*."""
    cfg = config or NLPConfig()
    enriched: List[Dict[str, Any]] = []
    for product in products:
        try:
            enriched.append(enrich_product_nlp(product, cfg))
        except Exception as exc:
            logger.warning("NLP pipeline failed for product %s: %s", product.get("url"), exc)
            enriched.append(product)
    return enriched
