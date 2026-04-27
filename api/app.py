"""
Scrapyard FastAPI service.

Exposes the scraper as a REST API while keeping the CLI fully functional.

Endpoints
---------
POST   /scrape              Submit a new scrape job
GET    /job/{job_id}        Poll job status and progress
GET    /download/{job_id}   Stream the output file (xlsx/csv)
GET    /jobs                List recent jobs

Run locally::

    uvicorn api.app:app --host 0.0.0.0 --port 8000 --reload

Or via docker::

    docker run -p 8000:8000 scrapyard uvicorn api.app:app --host 0.0.0.0
"""
from __future__ import annotations

import asyncio
import copy
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse

from api.jobs import JobStore
from api.models import (
    JobListResponse,
    JobResponse,
    JobSummary,
    ScrapeRequest,
)

logger = logging.getLogger(__name__)

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Scrapyard API",
    description="Automotive data extraction & enrichment platform",
    version="4.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

_store: Optional[JobStore] = None
_site_configs: Dict[str, Any] = {}
_STORAGE_DIR = Path(__file__).parent.parent / "storage"
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "sites.yaml"


@app.on_event("startup")
async def _startup() -> None:
    global _store, _site_configs
    _store = JobStore()
    await _store.setup()

    if _CONFIG_PATH.exists():
        with open(_CONFIG_PATH, encoding="utf-8") as fh:
            _site_configs = yaml.safe_load(fh) or {}

    _STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Scrapyard API started — %d sites configured", len(_site_configs))


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.post("/scrape", response_model=JobResponse, status_code=202)
async def submit_scrape(
    request: ScrapeRequest,
    background_tasks: BackgroundTasks,
) -> JobResponse:
    """
    Submit a new scrape job.  Returns immediately with a job_id.
    Poll GET /job/{job_id} for progress.
    """
    if request.site not in _site_configs:
        raise HTTPException(
            status_code=404,
            detail=f"Site '{request.site}' not found in config/sites.yaml. "
                   f"Available: {sorted(_site_configs)}",
        )

    job = await _store.create(request)
    background_tasks.add_task(_run_job, job.job_id, request)
    logger.info("Job queued: %s (site=%s)", job.job_id, request.site)
    return job


@app.get("/job/{job_id}", response_model=JobResponse)
async def get_job(job_id: str) -> JobResponse:
    """Poll the status and progress of a scrape job."""
    job = await _store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    return job


@app.get("/download/{job_id}")
async def download_output(job_id: str, part: int = 0):
    """
    Download the output file for a completed job.

    Use `?part=N` (0-indexed) to select a specific part file when the dataset
    was split into multiple files.
    """
    job = await _store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
    if job.status != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"Job is '{job.status}' — download only available for completed jobs",
        )
    if not job.output_paths:
        raise HTTPException(status_code=404, detail="No output files available")

    try:
        file_path = Path(job.output_paths[part])
    except IndexError:
        raise HTTPException(
            status_code=404,
            detail=f"Part {part} not found — job has {len(job.output_paths)} file(s)",
        )

    if not file_path.exists():
        raise HTTPException(status_code=410, detail="Output file no longer exists on disk")

    media_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if file_path.suffix == ".xlsx"
        else "text/csv; charset=utf-8"
    )
    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type=media_type,
    )


@app.get("/jobs", response_model=JobListResponse)
async def list_jobs(limit: int = 50) -> JobListResponse:
    """Return a list of recent scrape jobs."""
    jobs = await _store.list_recent(limit=limit)
    summaries = [
        JobSummary(
            job_id=j.job_id,
            site=j.site,
            status=j.status,
            created_at=j.created_at,
            rows_collected=j.rows_collected,
            output_paths=j.output_paths,
        )
        for j in jobs
    ]
    return JobListResponse(total=len(summaries), jobs=summaries)


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "sites_loaded": len(_site_configs)}


# ── Background job runner ─────────────────────────────────────────────────────

async def _run_job(job_id: str, request: ScrapeRequest) -> None:
    """Execute a scrape job asynchronously as a FastAPI background task."""
    import argparse

    await _store.update_status(job_id, "running")
    await _store.update_progress(job_id, current_stage="initialising")

    job_dir = _STORAGE_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Build the args namespace that main.run() expects
        site_config = copy.deepcopy(_site_configs[request.site])
        opts = request.options

        # Apply scrape options to config
        site_config["api_first"]   = opts.api_first and not opts.html_only
        site_config["html_only"]   = opts.html_only
        site_config["max_pages"]   = request.max_pages
        site_config["llm_enabled"] = False
        site_config["site_id"]     = request.site
        site_config["ignore_ssl"]  = False

        args = argparse.Namespace(
            site=request.site,
            format=request.format,
            output=str(job_dir),
            resume=False,
            incremental=False,
            force=False,
            concurrency=opts.concurrency,
            site_concurrency=0,
            details=opts.details,
            llm=False,
            max_pages=request.max_pages,
            ignore_ssl=False,
            log_level="INFO",
            # New flags
            no_nlp=not opts.enable_nlp,
            safe_export=opts.safe_export,
            no_excel_fallback=opts.no_excel_fallback,
            force_csv=False,
            max_rows_per_file=opts.max_rows_per_file,
            quality_report=opts.quality_report,
            api_first=opts.api_first,
            html_only=opts.html_only,
            max_api_pages=0,
            enable_jina=opts.enable_jina,
            disable_jina=opts.disable_jina,
            # Internal flags expected by run()
            multi_site=False,
            show_progress=False,
            defer_save=False,
        )

        # Progress callback wired into metrics events (best-effort)
        from main import run
        await _store.update_progress(job_id, current_stage="scraping")
        result = await run(args, site_config)

        output_paths: List[str] = []
        if result.get("output_path"):
            output_paths.append(result["output_path"])

        # Also discover split files
        for f in sorted(job_dir.glob("*.xlsx")) + sorted(job_dir.glob("*.csv")):
            if str(f) not in output_paths:
                output_paths.append(str(f))

        qr_path: Optional[str] = None
        qr_files = list(job_dir.glob("*quality_report.json"))
        if qr_files:
            qr_path = str(qr_files[0])

        await _store.complete(
            job_id,
            output_paths=output_paths,
            quality_report_path=qr_path,
            rows_collected=result.get("saved_count", 0),
        )

    except Exception as exc:
        logger.exception("Job %s failed", job_id)
        await _store.fail(job_id, str(exc))
