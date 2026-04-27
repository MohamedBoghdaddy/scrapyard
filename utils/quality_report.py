"""
Data quality report generator for Scrapyard.

Computes per-run statistics and returns a structured dict that can be:
  - Saved as JSON  (always)
  - Written as an Excel sheet  (when exporting Excel)
  - Written as a CSV summary   (when exporting CSV)

Usage::

    from utils.quality_report import build_quality_report, save_quality_report

    report = build_quality_report(products, run_meta={"site": "egycarparts"})
    save_quality_report(report, Path("output/run_meta.json"))
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report builder
# ---------------------------------------------------------------------------

def build_quality_report(
    products: List[Dict[str, Any]],
    *,
    run_meta: Optional[Dict[str, Any]] = None,
    output_paths: Optional[List[str]] = None,
    export_format: str = "excel",
) -> Dict[str, Any]:
    """
    Compute a data-quality report from a list of product dicts.

    Returns a flat dict suitable for JSON serialisation, Excel row,
    or CSV summary.
    """
    total = len(products)
    if total == 0:
        return _empty_report(run_meta, export_format, output_paths)

    # ── Field coverage ────────────────────────────────────────────────────
    def _filled(field: str) -> int:
        return sum(1 for p in products if _is_filled(p.get(field)))

    def _filled_pct(field: str) -> float:
        return round(100 * _filled(field) / total, 1)

    # ── Duplicate detection ───────────────────────────────────────────────
    urls = [str(p.get("url", "")) for p in products]
    names = [str(p.get("name", "")).strip().lower() for p in products]
    url_dupes = total - len(set(u for u in urls if u))
    name_dupes = total - len(set(n for n in names if n))
    duplicate_rate = round(100 * url_dupes / max(total, 1), 1)

    # ── Price stats ───────────────────────────────────────────────────────
    prices = [float(p["price"]) for p in products
              if _is_numeric(p.get("price"))]
    price_stats = _numeric_stats(prices)

    # ── Distribution counters ─────────────────────────────────────────────
    stock_dist    = _count_field(products, "stock_status")
    lang_dist     = _count_field(products, "language")
    topic_dist    = _count_field(products, "topic_category")
    source_dist   = _count_field(products, "data_source")

    report = {
        # Run context
        "generated_at":     datetime.now(timezone.utc).isoformat(),
        "site":             (run_meta or {}).get("site", ""),
        "export_format":    export_format,
        "output_paths":     json.dumps(output_paths or [], ensure_ascii=False),

        # Volume
        "total_rows":           total,
        "duplicate_url_count":  url_dupes,
        "duplicate_name_count": name_dupes,
        "duplicate_rate_pct":   duplicate_rate,

        # Coverage %
        "name_coverage_pct":        _filled_pct("name"),
        "price_coverage_pct":       _filled_pct("price"),
        "brand_coverage_pct":       _filled_pct("vendor"),
        "part_number_coverage_pct": _filled_pct("part_number"),
        "description_coverage_pct": _filled_pct("description"),
        "image_url_coverage_pct":   _filled_pct("image_url"),

        # Missing counts
        "missing_name":        total - _filled("name"),
        "missing_price":       total - _filled("price"),
        "missing_brand":       total - _filled("vendor"),
        "missing_part_number": total - _filled("part_number"),
        "missing_description": total - _filled("description"),

        # Price statistics
        "price_min":    price_stats["min"],
        "price_max":    price_stats["max"],
        "price_mean":   price_stats["mean"],
        "price_median": price_stats["median"],

        # Distributions (JSON strings for flat serialisation)
        "stock_distribution":  json.dumps(stock_dist,  ensure_ascii=False),
        "language_distribution": json.dumps(lang_dist, ensure_ascii=False),
        "topic_distribution":  json.dumps(topic_dist,  ensure_ascii=False),
        "source_distribution": json.dumps(source_dist, ensure_ascii=False),

        # NLP coverage
        "nlp_language_coverage_pct":  _filled_pct("language"),
        "nlp_keywords_coverage_pct":  _filled_pct("keywords"),
        "nlp_summary_coverage_pct":   _filled_pct("ai_summary"),
        "nlp_topic_coverage_pct":     _filled_pct("topic_category"),
    }

    # Merge optional run_meta (run_id, elapsed, etc.)
    if run_meta:
        for k, v in run_meta.items():
            if k not in report:
                report[k] = v

    return report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, float):
        import math
        return not math.isnan(value)
    if isinstance(value, str):
        return value.strip() not in ("", "unknown", "None", "nan")
    return True


def _is_numeric(value: Any) -> bool:
    if value is None:
        return False
    try:
        f = float(value)
        import math
        return not (math.isnan(f) or math.isinf(f))
    except (TypeError, ValueError):
        return False


def _count_field(products: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counter: Counter = Counter()
    for p in products:
        val = str(p.get(field, "unknown") or "unknown").strip() or "unknown"
        counter[val] += 1
    return dict(counter.most_common(20))


def _numeric_stats(values: List[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {"min": None, "max": None, "mean": None, "median": None}
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    median = (sorted_vals[mid - 1] + sorted_vals[mid]) / 2 if n % 2 == 0 else sorted_vals[mid]
    return {
        "min":    round(min(values), 2),
        "max":    round(max(values), 2),
        "mean":   round(sum(values) / n, 2),
        "median": round(median, 2),
    }


def _empty_report(
    run_meta: Optional[Dict[str, Any]],
    export_format: str,
    output_paths: Optional[List[str]],
) -> Dict[str, Any]:
    base = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "site": (run_meta or {}).get("site", ""),
        "export_format": export_format,
        "output_paths": json.dumps(output_paths or []),
        "total_rows": 0,
        "duplicate_rate_pct": 0.0,
    }
    if run_meta:
        base.update(run_meta)
    return base


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def save_quality_report(
    report: Dict[str, Any],
    path: Path,
) -> Path:
    """Write the report as JSON. Returns the path written."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False, default=str)
    logger.info("Quality report saved → %s", path)
    return path


def quality_report_to_dataframe(report: Dict[str, Any]) -> pd.DataFrame:
    """Convert the flat report dict into a two-column key/value DataFrame."""
    rows = [{"metric": k, "value": str(v)} for k, v in report.items()]
    return pd.DataFrame(rows, columns=["metric", "value"])
