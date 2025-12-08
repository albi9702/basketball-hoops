"""Helpers for persisting scraped data to a relational database (PostgreSQL by default)."""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, SQLAlchemyError

from src.configs.logging_config import get_logger
from src.configs.settings import DatabaseConfig
from src.configs.schema import SeasonColumns, ScheduleColumns, BoxscoreColumns

logger = get_logger(__name__)


class DatabaseStore:
    """Write scraped DataFrames into relational tables with configurable names."""

    # Default rows per INSERT batch to avoid hitting SQL statement limits
    DEFAULT_CHUNK_SIZE: int = 1000

    def __init__(
        self,
        url: Optional[str] = None,
        schema: Optional[str] = None,
        season_table: Optional[str] = None,
        schedule_table: Optional[str] = None,
        boxscore_table: Optional[str] = None,
        chunk_size: Optional[int] = None,
    ) -> None:
        self.primary_url = url or DatabaseConfig.URL
        self.fallback_url = DatabaseConfig.DEFAULT_SQLITE_URL
        self.schema = DatabaseConfig.SCHEMA if schema is None else schema
        self.season_table = season_table or DatabaseConfig.SEASON_TABLE
        self.schedule_table = schedule_table or DatabaseConfig.SCHEDULE_TABLE
        self.boxscore_table = boxscore_table or DatabaseConfig.BOXSCORE_TABLE
        self.chunk_size = chunk_size or self.DEFAULT_CHUNK_SIZE
        self.url: str
        self.engine: Engine
        self._initialize_engine_with_fallback()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def save_seasons(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Save seasons DataFrame with auto-generated SeasonID."""
        if df is None or df.empty:
            return 0
        
        # Generate SeasonID if not present
        if SeasonColumns.SEASON_ID.name not in df.columns:
            df = df.copy()
            df[SeasonColumns.SEASON_ID.name] = df.apply(
                lambda row: SeasonColumns.generate_id(
                    row[SeasonColumns.SEASON.name],
                    row[SeasonColumns.LEAGUE.name]
                ),
                axis=1
            )
            # Reorder columns to put ID first
            cols = [SeasonColumns.SEASON_ID.name] + [c for c in df.columns if c != SeasonColumns.SEASON_ID.name]
            df = df[cols]
        
        return self._write_dataframe(df, self.season_table, if_exists)

    def save_schedules(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Save schedules DataFrame with auto-generated GameID and SeasonID."""
        if df is None or df.empty:
            return 0
        
        df = df.copy()
        
        # Generate SeasonID (foreign key) if not present
        if ScheduleColumns.SEASON_ID.name not in df.columns:
            df[ScheduleColumns.SEASON_ID.name] = df.apply(
                lambda row: ScheduleColumns.generate_season_id(
                    row[ScheduleColumns.SEASON.name],
                    row[ScheduleColumns.LEAGUE_NAME.name]
                ),
                axis=1
            )
        
        # Generate GameID (primary key) if not present
        if ScheduleColumns.GAME_ID.name not in df.columns:
            df[ScheduleColumns.GAME_ID.name] = df.apply(
                lambda row: ScheduleColumns.generate_id(
                    str(row[ScheduleColumns.GAME_DATE.name]),
                    row[ScheduleColumns.TEAM_NAME_HOME.name],
                    row[ScheduleColumns.TEAM_NAME_VISITORS.name],
                    row[ScheduleColumns.SEASON.name],
                    row[ScheduleColumns.LEAGUE_NAME.name]
                ),
                axis=1
            )
        
        # Reorder columns to put IDs first
        id_cols = [ScheduleColumns.GAME_ID.name, ScheduleColumns.SEASON_ID.name]
        other_cols = [c for c in df.columns if c not in id_cols]
        df = df[id_cols + other_cols]
        
        return self._write_dataframe(df, self.schedule_table, if_exists)

    def save_boxscores(self, df: pd.DataFrame, if_exists: str = "replace") -> int:
        """Save boxscores DataFrame with auto-generated BoxscoreID and GameID."""
        if df is None or df.empty:
            return 0
        
        df = df.copy()
        
        # Generate GameID (foreign key) using DateURL which uniquely identifies a game
        if BoxscoreColumns.GAME_ID.name not in df.columns:
            df[BoxscoreColumns.GAME_ID.name] = df.apply(
                lambda row: BoxscoreColumns.generate_game_id_from_url(
                    row.get(BoxscoreColumns.DATE_URL.name, ""),
                    row.get(BoxscoreColumns.SEASON.name, ""),
                    row.get(BoxscoreColumns.LEAGUE.name, "")
                ),
                axis=1
            )
        
        # Generate BoxscoreID (primary key) if not present
        if BoxscoreColumns.BOXSCORE_ID.name not in df.columns:
            df[BoxscoreColumns.BOXSCORE_ID.name] = df.apply(
                lambda row: BoxscoreColumns.generate_id(
                    row[BoxscoreColumns.GAME_ID.name],
                    row.get(BoxscoreColumns.PLAYER_NAME.name, ""),
                    row.get(BoxscoreColumns.TEAM.name, "")
                ),
                axis=1
            )
        
        # Reorder columns to put IDs first
        id_cols = [BoxscoreColumns.BOXSCORE_ID.name, BoxscoreColumns.GAME_ID.name]
        other_cols = [c for c in df.columns if c not in id_cols]
        df = df[id_cols + other_cols]
        
        return self._write_dataframe(df, self.boxscore_table, if_exists)

    def row_count(self, table_name: str, schema: Optional[str] = None) -> int:
        identifier = self._table_identifier(table_name, schema)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {identifier}"))
                return int(result.scalar_one())
        except SQLAlchemyError as exc:
            logger.warning("Unable to count rows for %s (%s)", identifier, exc)
            return 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _write_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str) -> int:
        """Write DataFrame to database in chunks to avoid statement size limits."""
        if df is None or df.empty:
            logger.info("Skipping write for %s; no rows to persist", table_name)
            return 0

        target_schema = self.schema if self.schema else None
        total_rows = len(df)
        written = 0

        for start in range(0, total_rows, self.chunk_size):
            chunk = df.iloc[start : start + self.chunk_size]
            # First chunk: use caller's if_exists; subsequent: always append
            mode = if_exists if start == 0 else "append"
            chunk.to_sql(
                table_name,
                self.engine,
                schema=target_schema,
                if_exists=mode,
                index=False,
                method="multi",
            )
            written += len(chunk)
            logger.debug(
                "Wrote chunk of %d rows to %s (total so far: %d)",
                len(chunk),
                table_name,
                written,
            )

        logger.info(
            "Persisted %d rows into %s",
            written,
            self._table_identifier(table_name, target_schema),
        )
        return written

    def _ensure_schema(self) -> None:
        if not self.schema:
            return
        if self.engine.dialect.name == "sqlite":  # SQLite does not support custom schemas
            return
        ident = self._quote_identifier(self.schema)
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {ident}"))
                conn.commit()
        except (ProgrammingError, SQLAlchemyError) as exc:
            logger.warning("Could not ensure schema %s exists (%s)", self.schema, exc)
            raise

    def _initialize_engine_with_fallback(self) -> None:
        attempts: list[tuple[str, Optional[str]]] = []
        candidates = [(self.primary_url, self.schema)]
        if not self.primary_url.startswith("sqlite"):
            candidates.append((self.fallback_url, None))

        last_exc: Optional[SQLAlchemyError] = None
        for candidate_url, candidate_schema in candidates:
            attempts.append((candidate_url, candidate_schema))
            try:
                engine = create_engine(candidate_url, future=True)
                self.engine = engine
                self.url = candidate_url
                self.schema = candidate_schema
                self._ensure_schema()
                if candidate_url != self.primary_url:
                    logger.warning(
                        "Primary database unreachable; falling back to local SQLite at %s",
                        candidate_url,
                    )
                return
            except SQLAlchemyError as exc:
                last_exc = exc
                logger.warning(
                    "Database connection failed for %s (%s)",
                    candidate_url,
                    exc,
                )
                continue

        attempted = ", ".join(url for url, _ in attempts)
        raise RuntimeError(
            "Unable to initialize database connection after attempting: "
            f"{attempted}. Set DATABASE_URL (and credentials) or allow the fallback to SQLite."
        ) from last_exc

    def _table_identifier(self, table_name: str, schema: Optional[str]) -> str:
        quoted_table = self._quote_identifier(table_name)
        schema_name = schema if schema is not None else self.schema
        if schema_name:
            return f"{self._quote_identifier(schema_name)}.{quoted_table}"
        return quoted_table

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'