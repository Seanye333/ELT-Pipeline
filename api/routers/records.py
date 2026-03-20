"""Paginated record retrieval endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_oracle_loader
from api.models.responses import RecordsResponse
from src.config.settings import OracleSettings
from src.load.oracle_loader import OracleLoader

router = APIRouter(prefix="/datasets", tags=["records"])
_oracle_config = OracleSettings()


@router.get("/{name}/records", response_model=RecordsResponse, summary="Get paginated records")
async def get_records(
    name: str,
    limit: int = Query(default=100, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    date_from: str = Query(default="", description="Filter loaded_at >= date (YYYY-MM-DD)"),
    date_to: str = Query(default="", description="Filter loaded_at <= date (YYYY-MM-DD)"),
    loader: OracleLoader = Depends(get_oracle_loader),
) -> RecordsResponse:
    table = f"{_oracle_config.schema}.{name.upper()}"

    where_clauses = []
    params: dict = {}
    if date_from:
        where_clauses.append("loaded_at >= TO_DATE(:date_from, 'YYYY-MM-DD')")
        params["date_from"] = date_from
    if date_to:
        where_clauses.append("loaded_at <= TO_DATE(:date_to, 'YYYY-MM-DD') + 1")
        params["date_to"] = date_to

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # Total count
    count_result = loader.execute_query(
        f"SELECT COUNT(*) AS cnt FROM {table} {where_sql}", params
    )
    total = count_result[0]["cnt"] if count_result else 0

    # Paginated records
    records = loader.execute_query(
        f"""
        SELECT * FROM (
            SELECT t.*, ROWNUM AS rn
            FROM (SELECT * FROM {table} {where_sql} ORDER BY ROWID) t
            WHERE ROWNUM <= :end_row
        ) WHERE rn > :start_row
        """,
        {**params, "start_row": offset, "end_row": offset + limit},
    )

    return RecordsResponse(
        dataset=name,
        records=records,
        total=total,
        limit=limit,
        offset=offset,
    )
