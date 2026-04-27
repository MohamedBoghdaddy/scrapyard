"""
Async job state manager using aiosqlite.

Each scrape job is persisted to api/jobs.db so the service can:
  - Survive restarts (completed/failed jobs remain queryable)
  - Report progress in real-time via GET /job/{id}
  - Stream output file paths back to the caller

Schema
------
jobs table columns:
  job_id TEXT PRIMARY KEY
  site TEXT
  status TEXT                  -- pending|running|completed|failed
  created_at TEXT (ISO)
  started_at TEXT (ISO nullable)
  completed_at TEXT (ISO nullable)
  rows_collected INTEGER
  output_paths TEXT            -- JSON list of file paths
  quality_report_path TEXT
  error TEXT
  request_json TEXT            -- serialised ScrapeRequest
  progress_json TEXT           -- serialised JobProgress

Usage::

    from api.jobs import JobStore, JobRecord

    store = JobStore()
    await store.setup()

    job = await store.create(request)
    await store.update_status(job.job_id, "running")
    await store.update_progress(job.job_id, categories_found=10, products_collected=500)
    await store.complete(job.job_id, output_paths=["/storage/abc123/out.xlsx"])
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiosqlite

from api.models import JobProgress, JobResponse, JobSummary, ScrapeRequest

logger = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parent.parent / "api" / "jobs.db"

_DDL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id              TEXT PRIMARY KEY,
    site                TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    created_at          TEXT NOT NULL,
    started_at          TEXT,
    completed_at        TEXT,
    rows_collected      INTEGER DEFAULT 0,
    output_paths        TEXT DEFAULT '[]',
    quality_report_path TEXT,
    error               TEXT,
    request_json        TEXT,
    progress_json       TEXT DEFAULT '{}'
);
"""


class JobStore:
    """Async SQLite-backed job store."""

    def __init__(self, db_path: Path = _DB_PATH) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    async def setup(self) -> None:
        async with aiosqlite.connect(str(self._db_path)) as db:
            await db.execute(_DDL)
            await db.commit()
        logger.debug("JobStore initialised at %s", self._db_path)

    # ── CRUD ──────────────────────────────────────────────────────────────

    async def create(self, request: ScrapeRequest) -> JobResponse:
        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        progress = JobProgress(current_stage="pending")
        async with aiosqlite.connect(str(self._db_path)) as db:
            await db.execute(
                """INSERT INTO jobs
                   (job_id, site, status, created_at, request_json, progress_json)
                   VALUES (?, ?, 'pending', ?, ?, ?)""",
                (
                    job_id,
                    request.site,
                    now,
                    request.model_dump_json(),
                    progress.model_dump_json(),
                ),
            )
            await db.commit()
        logger.info("Job created: %s (site=%s)", job_id, request.site)
        return await self.get(job_id)

    async def get(self, job_id: str) -> Optional[JobResponse]:
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ) as cursor:
                row = await cursor.fetchone()
        if row is None:
            return None
        return _row_to_job(dict(row))

    async def list_recent(self, limit: int = 50) -> List[JobResponse]:
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
        return [_row_to_job(dict(r)) for r in rows]

    async def update_status(
        self,
        job_id: str,
        status: str,
        *,
        error: Optional[str] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        if status == "running":
            await self._exec(
                "UPDATE jobs SET status=?, started_at=? WHERE job_id=?",
                (status, now, job_id),
            )
        elif status in ("completed", "failed"):
            await self._exec(
                "UPDATE jobs SET status=?, completed_at=?, error=? WHERE job_id=?",
                (status, now, error, job_id),
            )
        else:
            await self._exec(
                "UPDATE jobs SET status=? WHERE job_id=?",
                (status, job_id),
            )

    async def update_progress(
        self,
        job_id: str,
        *,
        categories_found: Optional[int] = None,
        categories_done: Optional[int] = None,
        products_collected: Optional[int] = None,
        current_stage: Optional[str] = None,
        current_url: Optional[str] = None,
    ) -> None:
        current = await self.get(job_id)
        if current is None:
            return
        prog = current.progress
        if categories_found is not None:
            prog.categories_found = categories_found
        if categories_done is not None:
            prog.categories_done = categories_done
        if products_collected is not None:
            prog.products_collected = products_collected
            await self._exec(
                "UPDATE jobs SET rows_collected=? WHERE job_id=?",
                (products_collected, job_id),
            )
        if current_stage is not None:
            prog.current_stage = current_stage
        if current_url is not None:
            prog.current_url = current_url
        await self._exec(
            "UPDATE jobs SET progress_json=? WHERE job_id=?",
            (prog.model_dump_json(), job_id),
        )

    async def complete(
        self,
        job_id: str,
        *,
        output_paths: List[str],
        quality_report_path: Optional[str] = None,
        rows_collected: int = 0,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        await self._exec(
            """UPDATE jobs SET
               status='completed', completed_at=?, output_paths=?,
               quality_report_path=?, rows_collected=?,
               progress_json=?
               WHERE job_id=?""",
            (
                now,
                json.dumps(output_paths),
                quality_report_path,
                rows_collected,
                JobProgress(current_stage="done").model_dump_json(),
                job_id,
            ),
        )
        logger.info(
            "Job completed: %s → %d rows, %d output files",
            job_id, rows_collected, len(output_paths),
        )

    async def fail(self, job_id: str, error: str) -> None:
        await self.update_status(job_id, "failed", error=error[:2000])
        logger.error("Job failed: %s — %s", job_id, error[:200])

    # ── Internal helpers ──────────────────────────────────────────────────

    async def _exec(self, sql: str, params: tuple) -> None:
        async with aiosqlite.connect(str(self._db_path)) as db:
            await db.execute(sql, params)
            await db.commit()


# ---------------------------------------------------------------------------
# Row → model
# ---------------------------------------------------------------------------

def _row_to_job(row: Dict[str, Any]) -> JobResponse:
    def _parse_dt(val: Optional[str]) -> Optional[datetime]:
        if not val:
            return None
        try:
            return datetime.fromisoformat(val)
        except Exception:
            return None

    progress_data = {}
    try:
        progress_data = json.loads(row.get("progress_json") or "{}")
    except Exception:
        pass

    output_paths: List[str] = []
    try:
        output_paths = json.loads(row.get("output_paths") or "[]")
    except Exception:
        pass

    request_data: Optional[Dict[str, Any]] = None
    try:
        request_data = json.loads(row.get("request_json") or "null")
    except Exception:
        pass

    return JobResponse(
        job_id=row["job_id"],
        site=row["site"],
        status=row["status"],
        created_at=_parse_dt(row["created_at"]) or datetime.now(timezone.utc),
        started_at=_parse_dt(row.get("started_at")),
        completed_at=_parse_dt(row.get("completed_at")),
        progress=JobProgress(**progress_data) if progress_data else JobProgress(),
        rows_collected=int(row.get("rows_collected") or 0),
        output_paths=output_paths,
        quality_report_path=row.get("quality_report_path"),
        error=row.get("error"),
        request=request_data,
    )
