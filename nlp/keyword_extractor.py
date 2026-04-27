"""
Statistical keyword extraction via YAKE (Yet Another Keyword Extractor).

YAKE is dependency-light (pure Python), language-agnostic, and requires
no pre-trained model downloads, making it ideal for offline or edge deployments.

Falls back to a simple frequency-based extractor when yake is not installed.
"""
from __future__ import annotations

import logging
import re
from collections import Counter
from typing import List

logger = logging.getLogger(__name__)

# Characters that delimit token boundaries
_PUNCT_RE = re.compile(r"[^\wء-ي\s]")
# Very common stop words (English + Arabic) for the frequency fallback
_STOP_EN = frozenset(
    "a an the in on at of to for is are was were be been being have has had "
    "do does did will would could should may might shall can this that these "
    "those and or but not with from by as up out if it its we our you your he "
    "his she her they their all also more about over after".split()
)
_STOP_AR = frozenset(
    "من إلى في على عن مع هذا هذه ذلك تلك التي الذي التى الذين هو هي هم نحن "
    "أنت أنتم كل بعض قبل بعد حتى عند لدى ثم أو لكن لا ما".split()
)
_STOPWORDS = _STOP_EN | _STOP_AR


def extract_keywords(
    text: str,
    language: str = "en",
    max_keywords: int = 10,
    max_ngram: int = 2,
) -> List[str]:
    """
    Return up to *max_keywords* keyword strings ranked by relevance.

    Tries YAKE first; degrades to frequency-based extraction if not installed.
    """
    if not text or len(text.strip()) < 20:
        return []

    cleaned = text.strip()[:4000]

    try:
        import yake  # type: ignore

        lang = "ar" if language == "ar" else "en"
        extractor = yake.KeywordExtractor(
            lan=lang,
            n=max_ngram,
            dedupLim=0.7,
            dedupFunc="seqm",
            windowsSize=1,
            top=max_keywords,
        )
        keywords = extractor.extract_keywords(cleaned)
        # YAKE scores: lower = more relevant
        return [kw for kw, _score in keywords]

    except ImportError:
        logger.debug("yake not installed; using frequency fallback for keywords")
        return _frequency_keywords(cleaned, max_keywords=max_keywords)
    except Exception as exc:
        logger.debug("YAKE keyword extraction failed: %s", exc)
        return _frequency_keywords(cleaned, max_keywords=max_keywords)


def _frequency_keywords(text: str, *, max_keywords: int = 10) -> List[str]:
    """Simple term-frequency fallback (unigrams only)."""
    tokens = _PUNCT_RE.sub(" ", text.lower()).split()
    filtered = [t for t in tokens if len(t) > 3 and t not in _STOPWORDS]
    counts = Counter(filtered)
    return [word for word, _count in counts.most_common(max_keywords)]
