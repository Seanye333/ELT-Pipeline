"""
Central configuration module.
All other modules import settings from here — never read os.environ directly.
"""
from __future__ import annotations

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GraphAPISettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    tenant_id: str = Field(default="", alias="GRAPH_TENANT_ID")
    client_id: str = Field(default="", alias="GRAPH_CLIENT_ID")
    client_secret: str = Field(default="", alias="GRAPH_CLIENT_SECRET")
    user_id: str = Field(default="", alias="GRAPH_ONEDRIVE_USER_ID")
    scope: str = Field(default="https://graph.microsoft.com/.default", alias="GRAPH_SCOPE")
    base_url: str = Field(default="https://graph.microsoft.com/v1.0", alias="GRAPH_BASE_URL")
    onedrive_folder_path: str = Field(default="/Reports", alias="ONEDRIVE_FOLDER_PATH")


class MinIOSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    secure: bool = Field(default=False, alias="MINIO_SECURE")
    bronze_bucket: str = Field(default="bronze", alias="MINIO_BRONZE_BUCKET")
    silver_bucket: str = Field(default="silver", alias="MINIO_SILVER_BUCKET")
    region: str = Field(default="us-east-1", alias="MINIO_REGION")


class OracleSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field(default="localhost", alias="ORACLE_HOST")
    port: int = Field(default=1521, alias="ORACLE_PORT")
    service_name: str = Field(default="XEPDB1", alias="ORACLE_SERVICE_NAME")
    user: str = Field(default="", alias="ORACLE_USER")
    password: str = Field(default="", alias="ORACLE_PASSWORD")
    schema: str = Field(default="ELT_PIPELINE", alias="ORACLE_SCHEMA")
    pool_min: int = Field(default=2, alias="ORACLE_POOL_MIN")
    pool_max: int = Field(default=10, alias="ORACLE_POOL_MAX")
    pool_increment: int = Field(default=1, alias="ORACLE_POOL_INCREMENT")

    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"


class NotificationSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    slack_webhook_url: str = Field(default="", alias="SLACK_WEBHOOK_URL")
    alert_email_from: str = Field(default="", alias="ALERT_EMAIL_FROM")
    smtp_host: str = Field(default="", alias="SMTP_HOST")
    smtp_port: int = Field(default=587, alias="SMTP_PORT")
    smtp_user: str = Field(default="", alias="SMTP_USER")
    smtp_password: str = Field(default="", alias="SMTP_PASSWORD")


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    host: str = Field(default="0.0.0.0", alias="API_HOST")
    port: int = Field(default=8000, alias="API_PORT")
    debug: bool = Field(default=False, alias="API_DEBUG")
    workers: int = Field(default=4, alias="API_WORKERS")
    title: str = Field(default="ELT Pipeline Data API", alias="API_TITLE")
    version: str = Field(default="1.0.0", alias="API_VERSION")
    cors_origins: list[str] = Field(
        default=["http://localhost:8501"],
        alias="API_CORS_ORIGINS",
    )

    @model_validator(mode="before")
    @classmethod
    def parse_cors_origins(cls, values: dict) -> dict:
        cors = values.get("API_CORS_ORIGINS")
        if isinstance(cors, str):
            values["API_CORS_ORIGINS"] = [o.strip() for o in cors.split(",")]
        return values


class PipelineSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    env: str = Field(default="development", alias="PIPELINE_ENV")
    log_level: str = Field(default="INFO", alias="PIPELINE_LOG_LEVEL")
    max_file_size_mb: int = Field(default=500, alias="PIPELINE_MAX_FILE_SIZE_MB")
    batch_size: int = Field(default=10000, alias="PIPELINE_BATCH_SIZE")
    retry_max_attempts: int = Field(default=3, alias="PIPELINE_RETRY_MAX_ATTEMPTS")
    retry_backoff_seconds: int = Field(default=30, alias="PIPELINE_RETRY_BACKOFF_SECONDS")
    streamlit_api_base_url: str = Field(
        default="http://localhost:8000", alias="STREAMLIT_API_BASE_URL"
    )

    # Sub-settings (resolved lazily so .env is loaded first)
    @property
    def graph(self) -> GraphAPISettings:
        return GraphAPISettings()

    @property
    def minio(self) -> MinIOSettings:
        return MinIOSettings()

    @property
    def oracle(self) -> OracleSettings:
        return OracleSettings()

    @property
    def notifications(self) -> NotificationSettings:
        return NotificationSettings()

    @property
    def api(self) -> APISettings:
        return APISettings()


# ---------------------------------------------------------------------------
# Singleton — import this everywhere
# ---------------------------------------------------------------------------
settings = PipelineSettings()
