"""Dataset metadata endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from api.dependencies import get_oracle_loader
from api.models.responses import DatasetInfo, DatasetListResponse, DatasetStats
from src.config.settings import OracleSettings
from src.load.oracle_loader import OracleLoader

router = APIRouter(prefix="/datasets", tags=["datasets"])
_oracle_config = OracleSettings()


@router.get("", response_model=DatasetListResponse, summary="List all datasets")
async def list_datasets(loader: OracleLoader = Depends(get_oracle_loader)) -> DatasetListResponse:
    rows = loader.execute_query(
        f"""
        SELECT table_name AS name, num_rows AS row_count, last_analyzed AS last_loaded
        FROM all_tables
        WHERE owner = :schema
        ORDER BY table_name
        """,
        {"schema": _oracle_config.schema},
    )
    datasets = [
        DatasetInfo(
            name=r["name"],
            row_count=r.get("row_count") or 0,
            last_loaded=r.get("last_loaded"),
            columns=[],
        )
        for r in rows
    ]
    return DatasetListResponse(datasets=datasets, total=len(datasets))


@router.get("/{name}", response_model=DatasetInfo, summary="Get dataset metadata")
async def get_dataset(name: str, loader: OracleLoader = Depends(get_oracle_loader)) -> DatasetInfo:
    rows = loader.execute_query(
        f"""
        SELECT column_name
        FROM all_tab_columns
        WHERE owner = :schema AND table_name = :table
        ORDER BY column_id
        """,
        {"schema": _oracle_config.schema, "table": name.upper()},
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Dataset '{name}' not found")
    columns = [r["column_name"].lower() for r in rows]
    count_result = loader.execute_query(
        f"SELECT COUNT(*) AS cnt FROM {_oracle_config.schema}.{name.upper()}"
    )
    return DatasetInfo(
        name=name,
        row_count=count_result[0]["cnt"] if count_result else 0,
        last_loaded=None,
        columns=columns,
    )


@router.get("/{name}/stats", response_model=DatasetStats, summary="Get dataset statistics")
async def get_dataset_stats(name: str, loader: OracleLoader = Depends(get_oracle_loader)) -> DatasetStats:
    count_result = loader.execute_query(
        f"SELECT COUNT(*) AS cnt FROM {_oracle_config.schema}.{name.upper()}"
    )
    row_count = count_result[0]["cnt"] if count_result else 0

    date_result = loader.execute_query(
        f"""
        SELECT MIN(loaded_at) AS min_dt, MAX(loaded_at) AS max_dt
        FROM {_oracle_config.schema}.{name.upper()}
        """
    )
    return DatasetStats(
        dataset=name,
        row_count=row_count,
        null_rates={},
        min_loaded_at=date_result[0].get("min_dt") if date_result else None,
        max_loaded_at=date_result[0].get("max_dt") if date_result else None,
    )
