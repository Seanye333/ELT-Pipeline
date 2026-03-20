"""Pipeline trigger and run history endpoints."""
from __future__ import annotations

from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_oracle_loader
from api.models.requests import TriggerPipelineRequest
from api.models.responses import PipelineRunRecord, PipelineRunsResponse, TriggerResponse
from src.config.settings import OracleSettings
from src.load.oracle_loader import OracleLoader

router = APIRouter(prefix="/pipeline", tags=["pipeline"])
_oracle_config = OracleSettings()

# Airflow REST API base URL (set via env or default)
AIRFLOW_API_URL = "http://airflow-webserver:8080/api/v1"


@router.get("/runs", response_model=PipelineRunsResponse, summary="List pipeline runs")
async def list_pipeline_runs(
    limit: int = 20,
    loader: OracleLoader = Depends(get_oracle_loader),
) -> PipelineRunsResponse:
    rows = loader.execute_query(
        f"""
        SELECT run_id, dag_id, start_time, end_time, status,
               files_processed, rows_inserted, rows_updated, error_message
        FROM {_oracle_config.schema}.PIPELINE_RUN
        ORDER BY start_time DESC
        FETCH FIRST :limit ROWS ONLY
        """,
        {"limit": limit},
    )
    runs = [PipelineRunRecord(**r) for r in rows]
    return PipelineRunsResponse(runs=runs, total=len(runs))


@router.get("/runs/{run_id}", response_model=PipelineRunRecord, summary="Get a single run")
async def get_pipeline_run(
    run_id: str, loader: OracleLoader = Depends(get_oracle_loader)
) -> PipelineRunRecord:
    rows = loader.execute_query(
        f"""
        SELECT run_id, dag_id, start_time, end_time, status,
               files_processed, rows_inserted, rows_updated, error_message
        FROM {_oracle_config.schema}.PIPELINE_RUN
        WHERE run_id = :run_id
        """,
        {"run_id": run_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")
    return PipelineRunRecord(**rows[0])


@router.post("/trigger", response_model=TriggerResponse, summary="Trigger a pipeline DAG run")
async def trigger_pipeline(body: TriggerPipelineRequest) -> TriggerResponse:
    """Trigger an Airflow DAG via the Airflow REST API."""
    run_id = f"api_trigger_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    payload = {
        "dag_run_id": run_id,
        "conf": {
            "run_date": body.run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            **body.params,
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                f"{AIRFLOW_API_URL}/dags/{body.dag_id}/dagRuns",
                json=payload,
                auth=("airflow", "airflow"),  # Replace with proper auth in production
            )
            resp.raise_for_status()
        return TriggerResponse(success=True, message="DAG triggered successfully", run_id=run_id)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=502, detail=f"Airflow API error: {exc.response.text}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
