"""
MinIO / S3-compatible client wrapper using boto3.
Handles bucket creation, existence checks, and presigned URLs.
"""
from __future__ import annotations

import boto3
from botocore.client import BaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from src.config.logging_config import get_logger
from src.config.settings import MinIOSettings

logger = get_logger(__name__)


class MinIOClient:
    """Thin wrapper around boto3 S3 client pointed at MinIO."""

    def __init__(self, config: MinIOSettings | None = None) -> None:
        self._config = config or MinIOSettings()
        self._client: BaseClient = self._build_client()

    def _build_client(self) -> BaseClient:
        scheme = "https" if self._config.secure else "http"
        return boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{self._config.endpoint}",
            aws_access_key_id=self._config.access_key,
            aws_secret_access_key=self._config.secret_key,
            region_name=self._config.region,
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 3, "mode": "standard"},
            ),
        )

    @property
    def client(self) -> BaseClient:
        return self._client

    def ensure_bucket(self, bucket: str) -> None:
        """Create bucket if it does not exist."""
        try:
            self._client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchBucket"):
                logger.info("creating_bucket", bucket=bucket)
                self._client.create_bucket(Bucket=bucket)
            else:
                raise

    def object_exists(self, bucket: str, key: str) -> bool:
        """Return True if the object exists in the bucket."""
        try:
            self._client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                return False
            raise

    def get_object_metadata(self, bucket: str, key: str) -> dict:
        """Return object metadata dict (includes custom UserMetadata)."""
        resp = self._client.head_object(Bucket=bucket, Key=key)
        return resp.get("Metadata", {})

    def presigned_download_url(
        self, bucket: str, key: str, expires_seconds: int = 3600
    ) -> str:
        """Generate a presigned GET URL for the object."""
        return self._client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_seconds,
        )

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict]:
        """Return list of object dicts under a prefix."""
        paginator = self._client.get_paginator("list_objects_v2")
        results = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            results.extend(page.get("Contents", []))
        return results

    def check_connectivity(self) -> bool:
        try:
            self._client.list_buckets()
            return True
        except Exception as exc:
            logger.warning("minio_connectivity_check_failed", error=str(exc))
            return False
