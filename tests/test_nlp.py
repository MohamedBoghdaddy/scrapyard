"""
Tests for the NLP enrichment pipeline.

Covers:
- enrich_product_nlp() with normal, missing, empty, and very short text
- Arabic classifier correctness
- Language detection (English vs Arabic)
- Keyword extraction fallback when yake is unavailable
- Summariser fallback when sumy is unavailable
- enrich_batch_nlp() over a list of products
"""
from __future__ import annotations

import sys
from typing import Any, Dict
from unittest.mock import patch

import pytest

from nlp.pipeline import NLPConfig, enrich_product_nlp, enrich_batch_nlp
from nlp.classifier import classify_topic
from nlp.language_detector import detect_language


# ---------------------------------------------------------------------------
# Language detection
# ---------------------------------------------------------------------------

def test_detect_english():
    text = "Front brake disc compatible with Toyota Corolla 2018 model year."
    lang = detect_language(text)
    assert lang == "en"


def test_detect_arabic():
    text = "قرص فرامل أمامي متوافق مع تويوتا كورولا موديل 2018."
    lang = detect_language(text)
    assert lang == "ar"


def test_detect_empty_returns_unknown():
    assert detect_language("") == "unknown"
    assert detect_language("   ") == "unknown"


def test_detect_very_short_returns_unknown():
    # Below _MIN_CHARS threshold
    assert detect_language("hi") == "unknown"


def test_detect_arabic_dominant_ratio():
    """Pre-pass should catch Arabic without calling langdetect."""
    text = "وسادات فرامل عالية الجودة"
    lang = detect_language(text)
    assert lang == "ar"


# ---------------------------------------------------------------------------
# Arabic classifier correctness
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name,category,expected_topic", [
    ("Brake pad Toyota Corolla", "Brakes", "brakes"),
    ("Brake disc", "وسادات الفرامل", "brakes"),
    ("شمعات الإشعال", "شمعات إشعال", "electrical_ignition"),
    ("مرشحات الزيت", "مرشحات الزيت", "filters_fluids"),
    ("رادياتير تبريد", "مشعات السيارات", "cooling_hvac"),
    ("Shock absorber front", "Suspension", "suspension_steering"),
    ("جنوط سبائك مقاس 20 بوصة", "فهرس", "wheels_tyres"),
    ("Alternator 12V", "Electrical", "electrical_ignition"),
    ("Engine piston set", "Engine Parts", "engine"),
    ("مضخات المياه والحشيات", "مضخات المياه", "engine"),
    ("Oil filter kit", "Filters", "filters_fluids"),
    ("مرشحات الهواء", "مرشحات الهواء", "filters_fluids"),
    ("Clutch kit 6-speed", "Transmission", "transmission_drivetrain"),
    ("ممتصات الصدمات الأمامية", "ممتصات الصدمات", "suspension_steering"),
])
def test_classifier_correctness(name, category, expected_topic):
    result = classify_topic(name, category=category)
    assert result == expected_topic, (
        f"Expected '{expected_topic}' for name='{name}' cat='{category}', got '{result}'"
    )


def test_classifier_unrecognised_returns_general():
    result = classify_topic("XYZ-9999 unknown part", category="misc")
    assert result == "general_auto_parts"


def test_classifier_empty_returns_general():
    result = classify_topic("", category="")
    assert result == "general_auto_parts"


# ---------------------------------------------------------------------------
# NLP pipeline — enrich_product_nlp()
# ---------------------------------------------------------------------------

def test_enrich_full_product(sample_product):
    result = enrich_product_nlp(sample_product)
    assert result["language"] in ("en", "ar", "unknown")
    assert "topic_category" in result
    assert isinstance(result["keywords"], str)
    assert isinstance(result["ai_summary"], str)
    assert result["topic_category"] == "brakes"


def test_enrich_arabic_product(arabic_product):
    result = enrich_product_nlp(arabic_product)
    assert result["language"] in ("ar", "unknown")
    assert result["topic_category"] == "brakes"


def test_enrich_missing_description():
    product = {
        "name": "Engine oil filter",
        "url": "https://example.com/p1",
        "price": 50.0,
        "category": "Filters",
    }
    result = enrich_product_nlp(product)
    assert result["topic_category"] in ("filters_fluids", "engine", "general_auto_parts")
    assert "language" in result


def test_enrich_empty_product():
    """Empty product should not crash and should return safe defaults."""
    result = enrich_product_nlp({})
    assert result["language"] == "unknown"
    assert result["topic_category"] == "general_auto_parts"
    assert result["keywords"] == ""
    assert result["ai_summary"] == ""


def test_enrich_very_short_text():
    """Text below min_text_length should return defaults without running NLP."""
    product = {"name": "AB", "url": "https://ex.com/p", "price": 10.0}
    cfg = NLPConfig(min_text_length=50)
    result = enrich_product_nlp(product, cfg)
    assert result["language"] == "unknown"
    assert result["topic_category"] == "general_auto_parts"


def test_enrich_preserves_original_fields(sample_product):
    result = enrich_product_nlp(sample_product)
    # Original fields must not be overwritten
    assert result["name"] == sample_product["name"]
    assert result["price"] == sample_product["price"]
    assert result["url"] == sample_product["url"]


def test_enrich_disabled_features(sample_product):
    cfg = NLPConfig(language=False, keywords=False, summarize=False, classify=False)
    result = enrich_product_nlp(sample_product, cfg)
    assert result["language"] == "unknown"
    assert result["keywords"] == ""
    assert result["ai_summary"] == ""
    assert result["topic_category"] == "general_auto_parts"


def test_enrich_batch_nlp(product_list):
    results = enrich_batch_nlp(product_list)
    assert len(results) == len(product_list)
    for r in results:
        assert "language" in r
        assert "topic_category" in r
        assert "keywords" in r


def test_enrich_batch_does_not_crash_on_bad_product(product_list):
    bad = {"name": None, "url": None, "price": "not-a-number"}
    results = enrich_batch_nlp([bad])
    assert len(results) == 1


# ---------------------------------------------------------------------------
# Fallback behaviour when optional NLP libraries are unavailable
# ---------------------------------------------------------------------------

def test_keyword_extractor_fallback_without_yake(sample_product):
    """When yake is not installed, frequency-based fallback should still return keywords."""
    with patch.dict(sys.modules, {"yake": None}):
        from nlp import keyword_extractor
        text = "brake disc rotor caliper toyota corolla front wheel"
        kws = keyword_extractor._frequency_keywords(text, max_keywords=5)
        assert isinstance(kws, list)
        assert len(kws) <= 5


def test_summarizer_fallback_without_sumy():
    """When sumy is not installed, sentence-split fallback should still return text."""
    with patch.dict(sys.modules, {"sumy": None, "sumy.parsers.plaintext": None}):
        from nlp.summarizer import _sentence_fallback
        text = "This is the first sentence. This is the second. This is the third."
        result = _sentence_fallback(text, num_sentences=2)
        assert "first" in result


def test_language_detector_fallback_without_langdetect():
    """Without langdetect, Arabic regex pre-pass should still detect Arabic."""
    with patch.dict(sys.modules, {"langdetect": None}):
        # Re-import to pick up the patched module map
        import importlib
        import nlp.language_detector as ld
        importlib.reload(ld)
        # Arabic text should still be detected
        result = ld.detect_language("قرص فرامل أمامي متوافق مع تويوتا")
        assert result == "ar"
