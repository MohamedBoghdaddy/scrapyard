"""
Pydantic models for the Scrapyard REST API.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# POST /scrape  – request body
# ---------------------------------------------------------------------------

class ScrapeOptions(BaseModel):
    enable_nlp: bool = True
    api_first: bool = True
    html_only: bool = False
    safe_export: bool = True
    enable_jina: bool = False
    disable_jina: bool = False
    max_rows_per_file: int = Field(default=0, ge=0)
    quality_report: bool = True
    no_excel_fallback: bool = False
    details: bool = False
    concurrency: int = Field(default=5, ge=1, le=20)


class ScrapeRequest(BaseModel):
    site: str = Field(..., description="Site identifier from config/sites.yaml")
    max_pages: int = Field(default=5, ge=1, le=500)
    format: Literal["csv", "json", "excel", "sqlite"] = "excel"
    options: ScrapeOptions = Field(default_factory=ScrapeOptions)


# ---------------------------------------------------------------------------
# GET /job/{job_id}  – response body
# ---------------------------------------------------------------------------

class JobProgress(BaseModel):
    categories_found: int = 0
    categories_done: int = 0
    products_collected: int = 0
    current_stage: str = "pending"
    current_url: str = ""


class JobResponse(BaseModel):
    job_id: str
    site: str
    status: Literal["pending", "running", "completed", "failed"]
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: JobProgress = Field(default_factory=JobProgress)
    rows_collected: int = 0
    output_paths: List[str] = []
    quality_report_path: Optional[str] = None
    error: Optional[str] = None
    request: Optional[Dict[str, Any]] = None


# ---------------------------------------------------------------------------
# GET /jobs  – list response
# ---------------------------------------------------------------------------

class JobSummary(BaseModel):
    job_id: str
    site: str
    status: str
    created_at: datetime
    rows_collected: int = 0
    output_paths: List[str] = []


class JobListResponse(BaseModel):
    total: int
    jobs: List[JobSummary]
