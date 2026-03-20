"""
Custom Airflow Hook for MinIO.
Stores credentials in an Airflow Connection (conn_id: minio_default).

Connection fields:
  - conn_type: Amazon S3 (or Generic)
  - host: minio-endpoint:9000
  - login: <access_key>
  - password: <secret_key>
  - extra (JSON): {"secure": false, "region": "us-east-1"}
"""
from __future__ import annotations

import json

from airflow.hooks.base import BaseHook

from src.config.settings import MinIOSettings
from src.load.minio_client import MinIOClient
from src.load.minio_uploader import MinIOUploader


class MinIOHook(BaseHook):
    conn_name_attr = "minio_conn_id"
    default_conn_name = "minio_default"
    conn_type = "aws"
    hook_name = "MinIO"

    def __init__(self, minio_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.minio_conn_id = minio_conn_id
        self._client: MinIOClient | None = None

    def _build_settings(self) -> MinIOSettings:
        conn = self.get_connection(self.minio_conn_id)
        extra = json.loads(conn.extra or "{}")
        return MinIOSettings(
            _env_file=None,
            MINIO_ENDPOINT=conn.host or "localhost:9000",
            MINIO_ACCESS_KEY=conn.login or "",
            MINIO_SECRET_KEY=conn.password or "",
            MINIO_SECURE=extra.get("secure", False),
            MINIO_REGION=extra.get("region", "us-east-1"),
        )

    def get_client(self) -> MinIOClient:
        if self._client is None:
            self._client = MinIOClient(self._build_settings())
        return self._client

    def get_uploader(self) -> MinIOUploader:
        settings = self._build_settings()
        return MinIOUploader(client=self.get_client(), config=settings)

    def check_connectivity(self) -> bool:
        return self.get_client().check_connectivity()
