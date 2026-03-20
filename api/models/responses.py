"""Pydantic response schemas for the FastAPI layer."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str
    timestamp: datetime
    version: str


class ServiceStatus(BaseModel):
    name: str
    healthy: bool
    message: str = ""


class DetailedHealthResponse(BaseModel):
    status: str
    timestamp: datetime
    services: list[ServiceStatus]


class DatasetInfo(BaseModel):
    name: str
    row_count: int
    last_loaded: datetime | None
    columns: list[str]


class DatasetListResponse(BaseModel):
    datasets: list[DatasetInfo]
    total: int


class RecordsResponse(BaseModel):
    dataset: str
    records: list[dict[str, Any]]
    total: int
    limit: int
    offset: int


class DatasetStats(BaseModel):
    dataset: str
    row_count: int
    null_rates: dict[str, float]
    min_loaded_at: datetime | None
    max_loaded_at: datetime | None


class PipelineRunRecord(BaseModel):
    run_id: str
    dag_id: str
    start_time: datetime | None
    end_time: datetime | None
    status: str
    files_processed: int
    rows_inserted: int
    rows_updated: int
    error_message: str = ""


class PipelineRunsResponse(BaseModel):
    runs: list[PipelineRunRecord]
    total: int


class TriggerResponse(BaseModel):
    success: bool
    message: str
    run_id: str | None = None


class FileRecord(BaseModel):
    key: str
    size: int
    last_modified: datetime | None
    download_url: str | None = None


class FilesResponse(BaseModel):
    files: list[FileRecord]
    total: int
