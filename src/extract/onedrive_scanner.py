"""
Discover Excel and CSV files on a OneDrive folder using the Graph API.
Returns a list of FileMetadata dicts that downstream tasks consume via XCom.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from src.config.logging_config import get_logger
from src.config.settings import GraphAPISettings
from src.extract.graph_client import GraphAPIClient

logger = get_logger(__name__)

SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".xlsm", ".xlsb"}


@dataclass
class FileMetadata:
    file_id: str
    name: str
    size_bytes: int
    modified_at: str        # ISO-8601 string
    mime_type: str
    download_url: str       # Graph API content URL (relative path)
    parent_path: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class OneDriveScanner:
    """Scans a OneDrive folder path and returns metadata for supported files."""

    def __init__(
        self,
        client: GraphAPIClient | None = None,
        config: GraphAPISettings | None = None,
    ) -> None:
        self._config = config or GraphAPISettings()
        self._client = client or GraphAPIClient(self._config)

    def _drive_path(self, folder_path: str) -> str:
        """Build the Graph API path for a drive folder."""
        user = self._config.user_id
        folder = folder_path.strip("/")
        if user:
            return f"users/{user}/drive/root:/{folder}:/children"
        return f"me/drive/root:/{folder}:/children"

    def scan(self, folder_path: str | None = None) -> list[FileMetadata]:
        """
        Recursively scan the OneDrive folder and return supported file metadata.
        Returns an empty list (not an exception) if the folder is empty.
        """
        folder = folder_path or self._config.onedrive_folder_path
        logger.info("scanning_onedrive_folder", folder=folder)

        items = self._client.get_paginated(
            self._drive_path(folder),
            params={"$select": "id,name,size,lastModifiedDateTime,file,parentReference"},
        )

        files: list[FileMetadata] = []
        for item in items:
            name: str = item.get("name", "")
            ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""

            # Recurse into subfolders
            if "folder" in item:
                sub_path = f"{folder}/{name}"
                files.extend(self.scan(sub_path))
                continue

            if ext not in SUPPORTED_EXTENSIONS:
                logger.debug("skipping_unsupported_file", name=name, ext=ext)
                continue

            user = self._config.user_id
            item_id = item["id"]
            download_path = (
                f"users/{user}/drive/items/{item_id}/content"
                if user
                else f"me/drive/items/{item_id}/content"
            )

            files.append(
                FileMetadata(
                    file_id=item_id,
                    name=name,
                    size_bytes=item.get("size", 0),
                    modified_at=item.get("lastModifiedDateTime", datetime.utcnow().isoformat()),
                    mime_type=item.get("file", {}).get("mimeType", "application/octet-stream"),
                    download_url=download_path,
                    parent_path=folder,
                )
            )

        logger.info("onedrive_scan_complete", folder=folder, files_found=len(files))
        return files
