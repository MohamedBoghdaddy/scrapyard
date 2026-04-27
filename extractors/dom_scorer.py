"""
DOM density & content quality scorer.

Given a BeautifulSoup parse tree this module scores how "valuable" a page's
main content is across several heuristics, then exposes a single
score_content_quality() function that returns a ContentQuality dataclass.

Usage::

    from bs4 import BeautifulSoup
    from extractors.dom_scorer import score_content_quality

    soup = BeautifulSoup(html, "lxml")
    quality = score_content_quality(soup)
    if quality.is_high_value:
        ...  # prioritise this page for extraction
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional

from bs4 import BeautifulSoup

# Tags whose text is almost always noise
_NOISE_TAGS = {"nav", "footer", "aside", "script", "style", "noscript", "iframe"}
# Structural tags that indicate real content
_CONTENT_TAGS = {"article", "main", "section", "div", "p", "li", "td"}
_BOILERPLATE_RE = re.compile(
    r"(cookie\s*policy|privacy\s*policy|terms\s*(of\s*service|and\s*conditions)|"
    r"all\s*rights\s*reserved|copyright\s*©|powered\s*by|sign\s*in|log\s*in|"
    r"subscribe|newsletter|quick\s*view|add\s*to\s*cart|back\s*to\s*top)",
    re.IGNORECASE,
)


@dataclass
class ContentQuality:
    """Structured quality report for a single parsed HTML page."""

    text_density: float         # ratio of visible text chars to total HTML chars
    word_count: int             # visible word count after noise removal
    heading_count: int          # number of h1-h3 tags
    paragraph_count: int        # number of <p> tags with meaningful content
    link_density: float         # proportion of words inside <a> tags (high = nav page)
    structured_data: bool       # True when JSON-LD or microdata is present
    boilerplate_ratio: float    # fraction of text lines that look like boilerplate
    content_score: float        # 0-100 composite quality score
    is_high_value: bool         # True when content_score >= threshold
    dominant_blocks: List[str]  # CSS selectors / tag names of the richest blocks

    @property
    def summary(self) -> str:
        return (
            f"score={self.content_score:.1f} words={self.word_count} "
            f"density={self.text_density:.2f} link_density={self.link_density:.2f} "
            f"high_value={self.is_high_value}"
        )


def score_content_quality(
    soup: BeautifulSoup,
    *,
    high_value_threshold: float = 35.0,
) -> ContentQuality:
    """
    Compute a ContentQuality score for *soup*.

    Parameters
    ----------
    soup:
        A parsed BeautifulSoup tree.
    high_value_threshold:
        Minimum composite score to set is_high_value=True.
    """
    html_len = len(str(soup))

    # Remove noise tags in-place on a working copy
    working = BeautifulSoup(str(soup), "lxml")
    for tag in working.find_all(_NOISE_TAGS):
        tag.decompose()

    visible_text = working.get_text(" ", strip=True)
    text_len = len(visible_text)
    words = visible_text.split()
    word_count = len(words)

    # --- Heading count ---
    headings = len(working.find_all(["h1", "h2", "h3"]))

    # --- Paragraph count (non-trivial paragraphs only) ---
    paragraphs = sum(
        1
        for p in working.find_all("p")
        if len(p.get_text(strip=True).split()) >= 6
    )

    # --- Link density ---
    link_words = sum(
        len(a.get_text(strip=True).split()) for a in working.find_all("a")
    )
    link_density = link_words / max(word_count, 1)

    # --- Text density ---
    text_density = text_len / max(html_len, 1)

    # --- Boilerplate ratio ---
    lines = [line.strip() for line in visible_text.splitlines() if line.strip()]
    boilerplate_count = sum(1 for line in lines if _BOILERPLATE_RE.search(line))
    boilerplate_ratio = boilerplate_count / max(len(lines), 1)

    # --- Structured data ---
    structured_data = bool(
        working.find("script", {"type": "application/ld+json"})
        or working.find(attrs={"itemtype": True})
        or working.find(attrs={"itemprop": True})
    )

    # --- Dominant blocks ---
    dominant_blocks = _find_dominant_blocks(working)

    # --- Composite score (0-100) ---
    score = 0.0
    score += min(text_density * 120, 25)          # text density contribution
    score += min(word_count / 20, 20)             # raw word count
    score += min(headings * 4, 12)                # heading richness
    score += min(paragraphs * 3, 15)              # paragraph density
    score += 10 if structured_data else 0         # bonus for structured data
    score -= min(link_density * 40, 20)           # penalty for nav-heavy pages
    score -= min(boilerplate_ratio * 30, 15)      # penalty for boilerplate
    score = max(0.0, min(100.0, score))

    return ContentQuality(
        text_density=round(text_density, 4),
        word_count=word_count,
        heading_count=headings,
        paragraph_count=paragraphs,
        link_density=round(link_density, 4),
        structured_data=structured_data,
        boilerplate_ratio=round(boilerplate_ratio, 4),
        content_score=round(score, 2),
        is_high_value=score >= high_value_threshold,
        dominant_blocks=dominant_blocks,
    )


def _find_dominant_blocks(soup: BeautifulSoup, top_n: int = 3) -> List[str]:
    """Return CSS-style identifiers of the text-richest containers."""
    candidates = []
    for tag in soup.find_all(["article", "main", "section", "div"]):
        text = tag.get_text(" ", strip=True)
        wc = len(text.split())
        if wc < 20:
            continue
        # Build a simple selector label
        tag_id = tag.get("id", "")
        tag_class = " ".join(tag.get("class", []))[:40]
        label = tag.name
        if tag_id:
            label += f"#{tag_id}"
        elif tag_class:
            label += f".{tag_class.replace(' ', '.')}"
        candidates.append((wc, label))

    candidates.sort(key=lambda t: t[0], reverse=True)
    return [label for _wc, label in candidates[:top_n]]
