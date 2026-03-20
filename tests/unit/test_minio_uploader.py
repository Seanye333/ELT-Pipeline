"""Unit tests for MinIOUploader using a mocked MinIOClient."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.extract.onedrive_scanner import FileMetadata
from src.load.minio_uploader import MinIOUploader


@pytest.fixture
def file_meta():
    return FileMetadata(
        file_id="abc123",
        name="sales.xlsx",
        size_bytes=1024,
        modified_at="2024-01-15T10:00:00Z",
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        download_url="me/drive/items/abc123/content",
        parent_path="/Reports",
    )


class TestMinIOUploader:
    def test_upload_raw_calls_put_object(self, mock_minio_client, file_meta):
        uploader = MinIOUploader(client=mock_minio_client)
        raw = b"fake excel content"
        key = uploader.upload_raw(file_meta, raw, run_date="2024-01-15")

        assert "2024-01-15" in key
        assert "sales.xlsx" in key
        mock_minio_client.client.put_object.assert_called_once()

    def test_upload_raw_skips_duplicate(self, mock_minio_client, file_meta):
        from src.utils.checksum import md5_bytes

        raw = b"identical content"
        checksum = md5_bytes(raw)

        mock_minio_client.object_exists.return_value = True
        mock_minio_client.get_object_metadata.return_value = {"checksum_md5": checksum}

        uploader = MinIOUploader(client=mock_minio_client)
        key = uploader.upload_raw(file_meta, raw, run_date="2024-01-15")

        # Should NOT call put_object for duplicate
        mock_minio_client.client.put_object.assert_not_called()
        assert key is not None

    def test_upload_parquet(self, mock_minio_client):
        uploader = MinIOUploader(client=mock_minio_client)
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        key = uploader.upload_parquet(df, "data.csv", run_date="2024-01-15")

        assert "silver" not in key or "2024-01-15" in key
        mock_minio_client.client.put_object.assert_called_once()
