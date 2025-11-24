"""Cloud storage integration for scraped basketball data (Google Cloud Storage)."""

from __future__ import annotations

import io
from typing import Optional

import pandas as pd

try:
    from google.cloud import storage
    HAS_GCS = True
except ImportError:
    HAS_GCS = False
    storage = None  # type: ignore

from src.configs.logging_config import get_logger

logger = get_logger(__name__)


class CloudStore:
    """
    Persist scraped DataFrames to Google Cloud Storage.

    Implements the StorageBackend protocol.
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str = "basketball-hoops/",
        file_format: str = "parquet",
    ) -> None:
        """
        Initialize the cloud store.

        Parameters
        ----------
        bucket_name : str
            GCS bucket name.
        prefix : str
            Path prefix for uploaded blobs.
        file_format : str
            Output format (parquet or csv).
        """
        if not HAS_GCS:
            raise ImportError(
                "google-cloud-storage is required for CloudStore. "
                "Install it with: pip install google-cloud-storage"
            )
        self.bucket_name = bucket_name
        self.prefix = prefix.rstrip("/") + "/"
        self.file_format = file_format
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def _blob_path(self, name: str) -> str:
        """Build full blob path."""
        ext = "parquet" if self.file_format == "parquet" else "csv"
        return f"{self.prefix}{name}.{ext}"

    def _save(self, df: pd.DataFrame, name: str, if_exists: str) -> int:
        """Upload DataFrame to GCS."""
        if df is None or df.empty:
            logger.info("Skipping upload for %s; no rows to persist", name)
            return 0

        blob_path = self._blob_path(name)
        blob = self.bucket.blob(blob_path)

        if if_exists == "append" and blob.exists():
            existing = self._load(name)
            if existing is not None:
                df = pd.concat([existing, df], ignore_index=True)

        buffer = io.BytesIO()
        if self.file_format == "parquet":
            df.to_parquet(buffer, index=False)
            content_type = "application/octet-stream"
        else:
            df.to_csv(buffer, index=False)
            content_type = "text/csv"

        buffer.seek(0)
        blob.upload_from_file(buffer, content_type=content_type)
        logger.info("Uploaded %d rows to gs://%s/%s", len(df), self.bucket_name, blob_path)
        return len(df)

    def _load(self, name: str) -> Optional[pd.DataFrame]:
        """Download DataFrame from GCS."""
        blob_path = self._blob_path(name)
        blob = self.bucket.blob(blob_path)

        if not blob.exists():
            return None

        buffer = io.BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)

        if self.file_format == "parquet":
            return pd.read_parquet(buffer)
        return pd.read_csv(buffer)

    # StorageBackend protocol methods
    def save_seasons(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Persist season DataFrame."""
        return self._save(df, "seasons", if_exists)

    def save_schedules(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Persist schedule DataFrame."""
        return self._save(df, "schedules", if_exists)

    def save_boxscores(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Persist boxscore DataFrame."""
        return self._save(df, "boxscores", if_exists)

    # Utility methods
    def upload_blob(self, source_file_name: str, destination_blob_name: str) -> None:
        """Upload a local file to the bucket."""
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        logger.info("Uploaded %s to %s", source_file_name, destination_blob_name)

    def download_blob(self, source_blob_name: str, destination_file_name: str) -> None:
        """Download a blob to a local file."""
        blob = self.bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        logger.info("Downloaded %s to %s", source_blob_name, destination_file_name)

    def list_blobs(self, prefix: Optional[str] = None) -> list[str]:
        """List all blobs under a prefix."""
        blobs = self.bucket.list_blobs(prefix=prefix or self.prefix)
        return [blob.name for blob in blobs]


# Legacy alias for backward compatibility
CloudStorage = CloudStore