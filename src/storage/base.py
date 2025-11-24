"""Storage interface and base types shared by DatabaseStore, LocalStore, and CloudStore."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class StorageBackend(Protocol):
    """
    Protocol defining the minimal interface for persisting scraped data.

    Implementations include DatabaseStore, LocalStore, and CloudStore.
    """

    def save_seasons(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Persist season DataFrame. Return row count written."""
        ...

    def save_schedules(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Persist schedule DataFrame. Return row count written."""
        ...

    def save_boxscores(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Persist boxscore DataFrame. Return row count written."""
        ...
