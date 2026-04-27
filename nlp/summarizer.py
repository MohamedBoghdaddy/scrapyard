"""
Lightweight extractive summarisation.

Primary: sumy LsaSummarizer (no model downloads, pure Python).
Fallback: first-N-sentences heuristic so the pipeline never crashes.
"""
from __future__ import annotations

import logging
import re
from typing import List

logger = logging.getLogger(__name__)

_SENTENCE_RE = re.compile(r"(?<=[.!?؟])\s+")


def summarize(
    text: str,
    language: str = "en",
    num_sentences: int = 2,
) -> str:
    """
    Return a *num_sentences*-sentence extractive summary of *text*.

    Works for both English and Arabic text (Arabic via sentence splitting).
    """
    if not text or len(text.strip()) < 60:
        return text.strip()[:200] if text else ""

    cleaned = text.strip()[:5000]

    try:
        return _sumy_summarize(cleaned, language=language, num_sentences=num_sentences)
    except ImportError:
        logger.debug("sumy not installed; using sentence-split fallback")
        return _sentence_fallback(cleaned, num_sentences=num_sentences)
    except Exception as exc:
        logger.debug("sumy summarization failed: %s", exc)
        return _sentence_fallback(cleaned, num_sentences=num_sentences)


def _sumy_summarize(text: str, *, language: str, num_sentences: int) -> str:
    from sumy.parsers.plaintext import PlaintextParser  # type: ignore
    from sumy.nlp.tokenizers import Tokenizer  # type: ignore
    from sumy.summarizers.lsa import LsaSummarizer  # type: ignore
    from sumy.nlp.stemmers import Stemmer  # type: ignore
    from sumy.utils import get_stop_words  # type: ignore

    # sumy doesn't ship Arabic stop words; fall back to English tokeniser
    sumy_lang = "arabic" if language == "ar" else "english"
    try:
        tokenizer = Tokenizer(sumy_lang)
        stop_words = get_stop_words(sumy_lang)
    except LookupError:
        tokenizer = Tokenizer("english")
        stop_words = get_stop_words("english")

    parser = PlaintextParser.from_string(text, tokenizer)
    stemmer = Stemmer(tokenizer.language)
    summarizer = LsaSummarizer(stemmer)
    summarizer.stop_words = stop_words

    sentences = summarizer(parser.document, num_sentences)
    return " ".join(str(s) for s in sentences)


def _sentence_fallback(text: str, *, num_sentences: int) -> str:
    sentences: List[str] = [s.strip() for s in _SENTENCE_RE.split(text) if s.strip()]
    chosen = sentences[:num_sentences]
    return " ".join(chosen)
