"""
Lightweight in-process metrics tracker for a single scrape run.

Records request timings, success/failure counts, and product totals.
Saves a summary JSON after the run completes.
"""
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
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
        metrics.record_request(success=True, duration=time.monotonic() - t0)
        metrics.record_products(len(products))
        summary = metrics.finish()
        metrics.save("output/metrics.json")
    """

    site: str

    # Internal – not part of the public summary
    _start_time: float = field(default_factory=time.monotonic, init=False, repr=False)
    _end_time: float = field(default=0.0, init=False, repr=False)
    _response_times: List[float] = field(default_factory=list, init=False, repr=False)

    # Counters exposed in the summary
    total_requests: int = field(default=0, init=False)
    successful_requests: int = field(default=0, init=False)
    failed_requests: int = field(default=0, init=False)
    total_products: int = field(default=0, init=False)
    categories_scraped: int = field(default=0, init=False)
    detail_pages_fetched: int = field(default=0, init=False)
    checkpoints_resumed: int = field(default=0, init=False)

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_request(self, *, success: bool, duration: float) -> None:
        """Register one HTTP request (detail or listing)."""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        self._response_times.append(duration)

        # Proactive high-failure-rate detection (logged; caller handles alerting)
        if self.total_requests >= 20:
            rate = self.failed_requests / self.total_requests
            if rate > 0.5:
                logger.warning(
                    "High failure rate detected: %.0f%% of %d requests failed",
                    rate * 100,
                    self.total_requests,
                )

    def record_products(self, count: int) -> None:
        self.total_products += count

    def record_category(self) -> None:
        self.categories_scraped += 1

    def record_detail_fetch(self) -> None:
        self.detail_pages_fetched += 1

    def record_checkpoint_resume(self) -> None:
        self.checkpoints_resumed += 1

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
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
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
        """Write the summary to a JSON file. Returns the resolved path."""
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        summary = self.get_summary()
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2, ensure_ascii=False)
        logger.info("Metrics saved to %s", path)
        return path
