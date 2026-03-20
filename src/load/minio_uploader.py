"""
Upload raw files to MinIO bronze bucket with checksum-based idempotency.
Also writes transformed Parquet files to the silver bucket.
"""
from __future__ import annotations

import io
from datetime import datetime, timezone

import pandas as pd

from src.config.logging_config import get_logger
from src.config.settings import MinIOSettings
from src.extract.onedrive_scanner import FileMetadata
from src.load.minio_client import MinIOClient
from src.utils.checksum import md5_bytes

logger = get_logger(__name__)


class MinIOUploader:
    def __init__(
        self,
        client: MinIOClient | None = None,
        config: MinIOSettings | None = None,
    ) -> None:
        self._config = config or MinIOSettings()
        self._minio = client or MinIOClient(self._config)

    def _bronze_key(self, file_name: str, run_date: str | None = None) -> str:
        date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return f"{date}/{file_name}"

    def _silver_key(self, file_name: str, run_date: str | None = None) -> str:
        date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        base = file_name.rsplit(".", 1)[0]
        return f"{date}/{base}.parquet"

    def upload_raw(
        self,
        file_meta: FileMetadata,
        raw_bytes: bytes,
        run_date: str | None = None,
        dag_run_id: str = "",
    ) -> str:
        """
        Upload raw bytes to the bronze bucket.
        Skips upload if an identical checksum already exists (idempotency).
        Returns the MinIO object key.
        """
        self._minio.ensure_bucket(self._config.bronze_bucket)
        key = self._bronze_key(file_meta.name, run_date)
        checksum = md5_bytes(raw_bytes)

        # Check if already uploaded with same checksum
        if self._minio.object_exists(self._config.bronze_bucket, key):
            existing_meta = self._minio.get_object_metadata(self._config.bronze_bucket, key)
            if existing_meta.get("checksum_md5") == checksum:
                logger.info(
                    "skipping_duplicate_upload",
                    key=key,
                    checksum=checksum,
                )
                return key

        metadata = {
            "source": "onedrive",
            "file_id": file_meta.file_id,
            "original_name": file_meta.name,
            "checksum_md5": checksum,
            "dag_run_id": dag_run_id,
            "upload_timestamp": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            "uploading_to_bronze",
            key=key,
            size_bytes=len(raw_bytes),
            bucket=self._config.bronze_bucket,
        )
        self._minio.client.put_object(
            Bucket=self._config.bronze_bucket,
            Key=key,
            Body=raw_bytes,
            ContentType=file_meta.mime_type,
            Metadata=metadata,
        )
        logger.info("bronze_upload_complete", key=key, checksum=checksum)
        return key

    def upload_parquet(
        self,
        df: pd.DataFrame,
        file_name: str,
        run_date: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> str:
        """
        Serialize a DataFrame to Parquet and upload to the silver bucket.
        Returns the MinIO object key.
        """
        self._minio.ensure_bucket(self._config.silver_bucket)
        key = self._silver_key(file_name, run_date)

        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        parquet_bytes = buf.read()

        extra_meta = {
            "rows": str(len(df)),
            "columns": str(len(df.columns)),
            "upload_timestamp": datetime.now(timezone.utc).isoformat(),
            **(metadata or {}),
        }

        logger.info(
            "uploading_to_silver",
            key=key,
            rows=len(df),
            bucket=self._config.silver_bucket,
        )
        self._minio.client.put_object(
            Bucket=self._config.silver_bucket,
            Key=key,
            Body=parquet_bytes,
            ContentType="application/octet-stream",
            Metadata=extra_meta,
        )
        logger.info("silver_upload_complete", key=key)
        return key

    def download_parquet(self, key: str) -> pd.DataFrame:
        """Download a Parquet file from the silver bucket and return a DataFrame."""
        logger.info("downloading_parquet", key=key)
        resp = self._minio.client.get_object(
            Bucket=self._config.silver_bucket, Key=key
        )
        buf = io.BytesIO(resp["Body"].read())
        return pd.read_parquet(buf, engine="pyarrow")

    def download_raw(self, key: str) -> bytes:
        """Download raw bytes from the bronze bucket."""
        resp = self._minio.client.get_object(
            Bucket=self._config.bronze_bucket, Key=key
        )
        return resp["Body"].read()
