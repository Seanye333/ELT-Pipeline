"""
Bootstrap script: Create required MinIO buckets on first run.
Run once before starting the pipeline: python scripts/bootstrap_minio.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.logging_config import configure_logging, get_logger
from src.config.settings import MinIOSettings
from src.load.minio_client import MinIOClient

logger = get_logger(__name__)


def bootstrap_minio() -> None:
    configure_logging()
    config = MinIOSettings()
    client = MinIOClient(config)

    if not client.check_connectivity():
        logger.error("Cannot connect to MinIO. Check MINIO_ENDPOINT and credentials.")
        sys.exit(1)

    for bucket in [config.bronze_bucket, config.silver_bucket]:
        client.ensure_bucket(bucket)
        logger.info("bucket_ready", bucket=bucket)

    logger.info("minio_bootstrap_complete")


if __name__ == "__main__":
    bootstrap_minio()
