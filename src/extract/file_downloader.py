"""
Download files from OneDrive via Graph API content URLs.
Returns raw bytes ready to be uploaded to MinIO.
"""
from __future__ import annotations

import io
from pathlib import Path
from typing import Iterator

from src.config.logging_config import get_logger
from src.config.settings import GraphAPISettings, PipelineSettings
from src.extract.graph_client import GraphAPIClient
from src.extract.onedrive_scanner import FileMetadata
from src.utils.checksum import md5_bytes

logger = get_logger(__name__)


class FileDownloader:
    """Downloads files from OneDrive and yields (metadata, bytes) pairs."""

    def __init__(
        self,
        client: GraphAPIClient | None = None,
        config: GraphAPISettings | None = None,
        pipeline_config: PipelineSettings | None = None,
    ) -> None:
        self._graph_config = config or GraphAPISettings()
        self._pipeline_config = pipeline_config or PipelineSettings()
        self._client = client or GraphAPIClient(self._graph_config)

    def download(self, file_meta: FileMetadata) -> tuple[FileMetadata, bytes]:
        """
        Download a single file and return (metadata, raw_bytes).
        Raises ValueError if file exceeds the configured size limit.
        """
        max_bytes = self._pipeline_config.max_file_size_mb * 1024 * 1024
        if file_meta.size_bytes > max_bytes:
            raise ValueError(
                f"File '{file_meta.name}' ({file_meta.size_bytes} bytes) "
                f"exceeds max size limit of {self._pipeline_config.max_file_size_mb} MB"
            )

        logger.info(
            "downloading_file",
            name=file_meta.name,
            size_bytes=file_meta.size_bytes,
            file_id=file_meta.file_id,
        )
        raw_bytes = self._client.get_bytes(file_meta.download_url)
        checksum = md5_bytes(raw_bytes)
        logger.info(
            "file_downloaded",
            name=file_meta.name,
            bytes_received=len(raw_bytes),
            md5=checksum,
        )
        return file_meta, raw_bytes

    def download_all(
        self, files: list[FileMetadata]
    ) -> Iterator[tuple[FileMetadata, bytes]]:
        """Yield (metadata, bytes) for each file in the list."""
        for file_meta in files:
            try:
                yield self.download(file_meta)
            except Exception as exc:
                logger.error(
                    "file_download_failed",
                    name=file_meta.name,
                    error=str(exc),
                )
                raise

    def download_to_buffer(self, file_meta: FileMetadata) -> io.BytesIO:
        """Download a file and return it as an in-memory buffer."""
        _, raw_bytes = self.download(file_meta)
        buf = io.BytesIO(raw_bytes)
        buf.name = file_meta.name  # Preserve filename for pandas read_excel
        buf.seek(0)
        return buf

    def download_to_file(self, file_meta: FileMetadata, dest_dir: str | Path) -> Path:
        """Download a file and save it to disk. Returns the saved path."""
        dest = Path(dest_dir) / file_meta.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        _, raw_bytes = self.download(file_meta)
        dest.write_bytes(raw_bytes)
        logger.info("file_saved_to_disk", path=str(dest))
        return dest
