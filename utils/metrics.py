"""
Lightweight in-process metrics tracker for a single scrape run.

Records request timings, success/failure counts, product totals, and a full
per-request audit trail (URL, status, proxy, attempt number, latency).

Saves a summary + audit JSON after the run completes.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class MetricsTracker:
    """
    Thread-safe (single-event-loop) metrics accumulator.

    Example::

        metrics = MetricsTracker(site="egycarparts")
        t0 = time.monotonic()
        # ... do request ...
        metrics.record_request(
            url="https://...", success=True, duration=time.monotonic() - t0,
            status=200, proxy=None, attempt=1,
        )
        metrics.record_products(len(products))
        summary = metrics.finish()
        metrics.save_summary("output/run_20240101_120000_meta.json")
    """

    site: str

    # Internal accumulators
    _start_time: float = field(default_factory=time.monotonic, init=False, repr=False)
    _end_time: float = field(default=0.0, init=False, repr=False)
    _response_times: List[float] = field(default_factory=list, init=False, repr=False)
    _request_audit: List[Dict[str, Any]] = field(default_factory=list, init=False, repr=False)

    # Counters
    total_requests: int = field(default=0, init=False)
    successful_requests: int = field(default=0, init=False)
    failed_requests: int = field(default=0, init=False)
    blocked_requests: int = field(default=0, init=False)
    total_products: int = field(default=0, init=False)
    categories_scraped: int = field(default=0, init=False)
    detail_pages_fetched: int = field(default=0, init=False)
    checkpoints_resumed: int = field(default=0, init=False)
    llm_extractions: int = field(default=0, init=False)
    jina_fallbacks: int = field(default=0, init=False)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_request(
        self,
        *,
        success: bool,
        duration: float,
        url: str = "",
        status: int = 0,
        proxy: Optional[str] = None,
        attempt: int = 1,
        blocked: bool = False,
    ) -> None:
        """Register one HTTP request with full audit metadata."""
        self.total_requests += 1
        if blocked:
            self.blocked_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        self._response_times.append(duration)

        self._request_audit.append({
            "url": url,
            "status": status,
            "proxy": proxy,
            "attempt": attempt,
            "duration_s": round(duration, 3),
            "success": success,
            "blocked": blocked,
            "ts": datetime.now(timezone.utc).isoformat(),
        })

        if self.total_requests >= 20:
            rate = self.failed_requests / self.total_requests
            if rate > 0.5:
                logger.warning(
                    "High failure rate: %.0f%% of %d requests failed",
                    rate * 100, self.total_requests,
                )

    def record_products(self, count: int) -> None:
        self.total_products += count

    def record_category(self) -> None:
        self.categories_scraped += 1

    def record_detail_fetch(self) -> None:
        self.detail_pages_fetched += 1

    def record_checkpoint_resume(self) -> None:
        self.checkpoints_resumed += 1

    def record_llm_extraction(self) -> None:
        self.llm_extractions += 1

    def record_jina_fallback(self) -> None:
        self.jina_fallbacks += 1

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def finish(self) -> Dict[str, Any]:
        """Finalise timing and return the summary dict."""
        self._end_time = time.monotonic()
        return self.get_summary()

    def get_summary(self) -> Dict[str, Any]:
        elapsed = (self._end_time or time.monotonic()) - self._start_time
        avg_rt = (
            sum(self._response_times) / len(self._response_times)
            if self._response_times
            else 0.0
        )
        p95_rt = (
            sorted(self._response_times)[int(len(self._response_times) * 0.95)]
            if len(self._response_times) > 1
            else avg_rt
        )
        success_rate = (
            self.successful_requests / self.total_requests * 100
            if self.total_requests
            else 100.0
        )
        products_per_min = (
            self.total_products / (elapsed / 60) if elapsed > 0 else 0.0
        )

        return {
            "site": self.site,
            "elapsed_seconds": round(elapsed, 2),
            "categories_scraped": self.categories_scraped,
            "total_products": self.total_products,
            "detail_pages_fetched": self.detail_pages_fetched,
            "checkpoints_resumed": self.checkpoints_resumed,
            "llm_extractions": self.llm_extractions,
            "jina_fallbacks": self.jina_fallbacks,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "blocked_requests": self.blocked_requests,
            "success_rate_pct": round(success_rate, 1),
            "avg_response_time_s": round(avg_rt, 3),
            "p95_response_time_s": round(p95_rt, 3),
            "products_per_minute": round(products_per_min, 1),
        }

    @property
    def failure_rate_pct(self) -> float:
        if not self.total_requests:
            return 0.0
        return self.failed_requests / self.total_requests * 100

    def save(self, filepath: str | Path) -> Path:
        """Write summary + request audit to a JSON file. Returns the path."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "summary": self.get_summary(),
            "request_audit": self._request_audit,
        }
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, ensure_ascii=False)
        logger.info("Metrics saved to %s", path)
        return path

    def save_summary(self, filepath: str | Path) -> Path:
        """Alias for save() – write run metadata JSON."""
        return self.save(filepath)


# Public alias used in main.py
MetricsCollector = MetricsTracker
