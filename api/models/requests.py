"""Pydantic request schemas for the FastAPI layer."""
from __future__ import annotations

from pydantic import BaseModel, Field


class TriggerPipelineRequest(BaseModel):
    dag_id: str = Field(default="dag_full_pipeline", description="Airflow DAG ID to trigger")
    run_date: str = Field(default="", description="Override run date (YYYY-MM-DD). Defaults to today.")
    params: dict = Field(default_factory=dict, description="Additional DAG params")
