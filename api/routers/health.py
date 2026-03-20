"""Health check endpoints."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from api.models.responses import DetailedHealthResponse, HealthResponse, ServiceStatus
from src.config.settings import MinIOSettings, OracleSettings
from src.config.settings import PipelineSettings

router = APIRouter(prefix="/health", tags=["health"])

_settings = PipelineSettings()


@router.get("", response_model=HealthResponse, summary="Liveness probe")
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        timestamp=datetime.now(timezone.utc),
        version=_settings.api.version,
    )


@router.get("/detailed", response_model=DetailedHealthResponse, summary="Readiness probe")
async def health_detailed() -> DetailedHealthResponse:
    services: list[ServiceStatus] = []

    # Check MinIO
    try:
        from src.load.minio_client import MinIOClient
        ok = MinIOClient(MinIOSettings()).check_connectivity()
        services.append(ServiceStatus(name="minio", healthy=ok, message="OK" if ok else "unreachable"))
    except Exception as exc:
        services.append(ServiceStatus(name="minio", healthy=False, message=str(exc)))

    # Check Oracle
    try:
        from src.load.oracle_loader import OracleLoader
        ok = OracleLoader(OracleSettings()).check_connectivity()
        services.append(ServiceStatus(name="oracle", healthy=ok, message="OK" if ok else "unreachable"))
    except Exception as exc:
        services.append(ServiceStatus(name="oracle", healthy=False, message=str(exc)))

    overall = "ok" if all(s.healthy for s in services) else "degraded"
    return DetailedHealthResponse(
        status=overall,
        timestamp=datetime.now(timezone.utc),
        services=services,
    )
