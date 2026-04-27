"""
Scrapyard NLP package.

Modular text-intelligence layer:
  language_detector  – ISO language code detection
  keyword_extractor  – YAKE statistical keyword extraction
  summarizer         – extractive sentence summarisation (sumy)
  classifier         – rule-based topic / category tagging
  pipeline           – orchestrates all steps into a single enrich() call
"""
from .pipeline import enrich_product_nlp, NLPConfig

__all__ = ["enrich_product_nlp", "NLPConfig"]
