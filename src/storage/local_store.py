"""Local file system storage for scraped basketball data."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from src.configs.logging_config import get_logger
from src.configs.settings import Config

logger = get_logger(__name__)


class LocalStore:
    """
    Persist scraped DataFrames to local CSV or Parquet files.

    Implements the StorageBackend protocol.
    """

    def __init__(
        self,
        directory: Optional[Path] = None,
        file_format: Literal["csv", "parquet"] = "csv",
    ) -> None:
        """
        Initialize the local store.

        Parameters
        ----------
        directory : Optional[Path]
            Target directory for data files. Defaults to data/raw.
        file_format : Literal["csv", "parquet"]
            Output format for saved files.
        """
        self.directory = Path(directory) if directory else Path(Config.RAW_DATA_DIR)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.file_format = file_format

    def _save(self, df: pd.DataFrame, filename: str, if_exists: str) -> int:
        """Save DataFrame to file."""
        if df is None or df.empty:
            logger.info("Skipping write for %s; no rows to persist", filename)
            return 0

        ext = "parquet" if self.file_format == "parquet" else "csv"
        file_path = self.directory / f"{filename}.{ext}"

        if if_exists == "append" and file_path.exists():
            existing = self._load(filename)
            if existing is not None:
                df = pd.concat([existing, df], ignore_index=True)

        if self.file_format == "parquet":
            df.to_parquet(file_path, index=False)
        else:
            df.to_csv(file_path, index=False)

        logger.info("Saved %d rows to %s", len(df), file_path)
        return len(df)

    def _load(self, filename: str) -> Optional[pd.DataFrame]:
        """Load DataFrame from file if it exists."""
        ext = "parquet" if self.file_format == "parquet" else "csv"
        file_path = self.directory / f"{filename}.{ext}"

        if not file_path.exists():
            return None

        if self.file_format == "parquet":
            return pd.read_parquet(file_path)
        return pd.read_csv(file_path)

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

    def load_seasons(self) -> Optional[pd.DataFrame]:
        """Load season DataFrame."""
        return self._load("seasons")

    def load_schedules(self) -> Optional[pd.DataFrame]:
        """Load schedule DataFrame."""
        return self._load("schedules")

    def load_boxscores(self) -> Optional[pd.DataFrame]:
        """Load boxscore DataFrame."""
        return self._load("boxscores")


# Backward-compatible module-level functions
def save_data_locally(
    data: pd.DataFrame,
    filename: str,
    directory: str = "data/raw",
) -> None:
    """Save data to a CSV file (backward-compatible function)."""
    store = LocalStore(directory=Path(directory), file_format="csv")
    store._save(data, filename.replace(".csv", ""), "replace")


def load_data_locally(
    filename: str,
    directory: str = "data/raw",
) -> Optional[pd.DataFrame]:
    """Load data from a CSV file (backward-compatible function)."""
    store = LocalStore(directory=Path(directory), file_format="csv")
    return store._load(filename.replace(".csv", ""))