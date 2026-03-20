"""FastAPI application factory."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import datasets, health, pipeline, records
from src.config.logging_config import configure_logging
from src.config.settings import PipelineSettings

_settings = PipelineSettings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging(_settings.log_level)
    yield


def create_app() -> FastAPI:
    app = FastAPI(
        title=_settings.api.title,
        version=_settings.api.version,
        description="REST API for the ELT Pipeline — query Oracle data and trigger DAGs",
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_settings.api.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(datasets.router)
    app.include_router(records.router)
    app.include_router(pipeline.router)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=_settings.api.host,
        port=_settings.api.port,
        reload=_settings.api.debug,
        workers=1 if _settings.api.debug else _settings.api.workers,
    )
