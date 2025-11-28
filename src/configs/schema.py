"""Centralized schema definitions for all basketball data tables.

This module defines column names, types, and table structures in a single place.
All storage backends and scrapers should reference these definitions to ensure
consistency across the application.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd

from src.configs.settings import DatabaseConfig


# =============================================================================
# Column Definitions
# =============================================================================

@dataclass(frozen=True)
class Column:
    """Definition of a single column."""
    name: str
    dtype: str  # pandas dtype string
    nullable: bool = True
    description: str = ""


# =============================================================================
# Seasons Table Schema
# =============================================================================

class SeasonColumns:
    """Column definitions for the seasons table."""
    SEASON = Column("Season", "str", nullable=False, description="Season identifier (e.g., '2024-25')")
    SEASON_URL = Column("SeasonURL", "str", description="URL to the season page")
    LEAGUE = Column("League", "str", nullable=False, description="League name (e.g., 'EuroLeague')")
    LEAGUE_URL = Column("LeagueURL", "str", description="URL to the league page")
    SCHEDULE_URL = Column("ScheduleURL", "str", description="URL to the season schedule page")

    @classmethod
    def all_columns(cls) -> list[Column]:
        return [cls.SEASON, cls.SEASON_URL, cls.LEAGUE, cls.LEAGUE_URL, cls.SCHEDULE_URL]

    @classmethod
    def names(cls) -> list[str]:
        return [col.name for col in cls.all_columns()]

    @classmethod
    def required(cls) -> list[str]:
        return [col.name for col in cls.all_columns() if not col.nullable]


# =============================================================================
# Schedules Table Schema
# =============================================================================

class ScheduleColumns:
    """Column definitions for the schedules table."""
    GAME_DATE = Column("GameDate", "datetime64[ns]", nullable=False, description="Game date")
    TEAM_NAME_HOME = Column("TeamNameHome", "str", nullable=False, description="Home team name")
    TEAM_HOME_POINTS = Column("TeamHomePoints", "Int64", description="Home team score")
    TEAM_NAME_VISITORS = Column("TeamNameVisitors", "str", nullable=False, description="Visiting team name")
    TEAM_VISITORS_POINTS = Column("TeamVisitorsPoints", "Int64", description="Visiting team score")
    HAS_GONE_OVERTIME = Column("HasGoneOvertime", "str", description="Overtime indicator")
    NOTES = Column("Notes", "str", description="Additional game notes")
    DATE_URL = Column("DateURL", "str", description="URL to the boxscore page")
    SEASON = Column("Season", "str", nullable=False, description="Season identifier")
    LEAGUE_NAME = Column("LeagueName", "str", nullable=False, description="League name")
    SCHEDULE_URL = Column("ScheduleURL", "str", description="URL to the schedule page")

    @classmethod
    def all_columns(cls) -> list[Column]:
        return [
            cls.GAME_DATE, cls.TEAM_NAME_HOME, cls.TEAM_HOME_POINTS, cls.TEAM_NAME_VISITORS,
            cls.TEAM_VISITORS_POINTS, cls.HAS_GONE_OVERTIME, cls.NOTES, cls.DATE_URL,
            cls.SEASON, cls.LEAGUE_NAME, cls.SCHEDULE_URL,
        ]

    @classmethod
    def names(cls) -> list[str]:
        return [col.name for col in cls.all_columns()]

    @classmethod
    def required(cls) -> list[str]:
        return [col.name for col in cls.all_columns() if not col.nullable]


# =============================================================================
# Boxscores Table Schema
# =============================================================================

class BoxscoreColumns:
    """Column definitions for the boxscores table."""
    # Player info
    PLAYER_NAME = Column("PlayerName", "str", description="Player name")
    
    # Player stats
    MINUTES_PLAYED = Column("MinutesPlayed", "str", description="Minutes played")
    FIELD_GOALS_MADE = Column("FieldGoalsMade", "Int64", description="Field goals made")
    FIELD_GOALS_ATTEMPTED = Column("FieldGoalsAttempted", "Int64", description="Field goals attempted")
    FIELD_GOAL_PERCENTAGE = Column("FieldGoalPercentage", "float64", description="Field goal percentage")
    THREE_POINT_MADE = Column("ThreePointMade", "Int64", description="Three-pointers made")
    THREE_POINT_ATTEMPTED = Column("ThreePointAttempted", "Int64", description="Three-pointers attempted")
    THREE_POINT_PERCENTAGE = Column("ThreePointPercentage", "float64", description="Three-point percentage")
    FREE_THROWS_MADE = Column("FreeThrowsMade", "Int64", description="Free throws made")
    FREE_THROWS_ATTEMPTED = Column("FreeThrowsAttempted", "Int64", description="Free throws attempted")
    FREE_THROW_PERCENTAGE = Column("FreeThrowPercentage", "float64", description="Free throw percentage")
    OFFENSIVE_REBOUNDS = Column("OffensiveRebounds", "Int64", description="Offensive rebounds")
    DEFENSIVE_REBOUNDS = Column("DefensiveRebounds", "Int64", description="Defensive rebounds")
    TOTAL_REBOUNDS = Column("TotalRebounds", "Int64", description="Total rebounds")
    ASSISTS = Column("Assists", "Int64", description="Assists")
    STEALS = Column("Steals", "Int64", description="Steals")
    BLOCKS = Column("Blocks", "Int64", description="Blocks")
    TURNOVERS = Column("Turnovers", "Int64", description="Turnovers")
    PERSONAL_FOULS = Column("PersonalFouls", "Int64", description="Personal fouls")
    POINTS = Column("Points", "Int64", description="Points scored")
    
    # Team context
    TEAM_ROLE = Column("TeamRole", "str", nullable=False, description="Home or Visitors")
    TEAM = Column("Team", "str", nullable=False, description="Team name")
    
    # Game context
    DATE = Column("Date", "datetime64[ns]", nullable=False, description="Game date")
    SEASON = Column("Season", "str", nullable=False, description="Season identifier")
    LEAGUE = Column("League", "str", nullable=False, description="League name")
    SCHEDULE_URL = Column("ScheduleURL", "str", description="URL to the schedule page")
    DATE_URL = Column("DateURL", "str", description="URL to the boxscore page")

    @classmethod
    def all_columns(cls) -> list[Column]:
        """All columns in the boxscores table."""
        return [
            cls.PLAYER_NAME,
            cls.MINUTES_PLAYED,
            cls.FIELD_GOALS_MADE,
            cls.FIELD_GOALS_ATTEMPTED,
            cls.FIELD_GOAL_PERCENTAGE,
            cls.THREE_POINT_MADE,
            cls.THREE_POINT_ATTEMPTED,
            cls.THREE_POINT_PERCENTAGE,
            cls.FREE_THROWS_MADE,
            cls.FREE_THROWS_ATTEMPTED,
            cls.FREE_THROW_PERCENTAGE,
            cls.OFFENSIVE_REBOUNDS,
            cls.DEFENSIVE_REBOUNDS,
            cls.TOTAL_REBOUNDS,
            cls.ASSISTS,
            cls.STEALS,
            cls.BLOCKS,
            cls.TURNOVERS,
            cls.PERSONAL_FOULS,
            cls.POINTS,
            cls.TEAM_ROLE,
            cls.TEAM,
            cls.DATE,
            cls.SEASON,
            cls.LEAGUE,
            cls.SCHEDULE_URL,
            cls.DATE_URL,
        ]

    @classmethod
    def context_columns(cls) -> list[Column]:
        """Columns that provide game/team context (not player stats)."""
        return [
            cls.TEAM_ROLE, cls.TEAM, cls.DATE, cls.SEASON, cls.LEAGUE,
            cls.SCHEDULE_URL, cls.DATE_URL,
        ]

    @classmethod
    def names(cls) -> list[str]:
        return [col.name for col in cls.all_columns()]

    @classmethod
    def context_names(cls) -> list[str]:
        return [col.name for col in cls.context_columns()]

    @classmethod
    def required(cls) -> list[str]:
        return [col.name for col in cls.all_columns() if not col.nullable]


# =============================================================================
# Schema Registry
# =============================================================================

@dataclass
class TableSchema:
    """Complete schema definition for a table."""
    name: str
    columns: list[Column]
    description: str = ""
    
    def column_names(self) -> list[str]:
        return [col.name for col in self.columns]
    
    def required_columns(self) -> list[str]:
        return [col.name for col in self.columns if not col.nullable]
    
    def dtype_map(self) -> dict[str, str]:
        """Return a mapping of column names to pandas dtypes."""
        return {col.name: col.dtype for col in self.columns}
    
    def empty_dataframe(self) -> pd.DataFrame:
        """Create an empty DataFrame with the correct columns and dtypes."""
        return pd.DataFrame({col.name: pd.Series(dtype=col.dtype) for col in self.columns})


# Pre-defined schemas
SEASONS_SCHEMA = TableSchema(
    name=DatabaseConfig.SEASON_TABLE,
    columns=SeasonColumns.all_columns(),
    description="Basketball seasons and their associated leagues",
)

SCHEDULES_SCHEMA = TableSchema(
    name=DatabaseConfig.SCHEDULE_TABLE, 
    columns=ScheduleColumns.all_columns(),
    description="Game schedules with dates, teams, and scores",
)

BOXSCORES_SCHEMA = TableSchema(
    name=DatabaseConfig.BOXSCORE_TABLE,
    columns=BoxscoreColumns.all_columns(),
    description="Player-level boxscore statistics per game",
)


# =============================================================================
# Helper Functions
# =============================================================================

def create_empty_seasons_df() -> pd.DataFrame:
    """Create an empty seasons DataFrame with correct schema."""
    return SEASONS_SCHEMA.empty_dataframe()


def create_empty_schedules_df() -> pd.DataFrame:
    """Create an empty schedules DataFrame with correct schema."""
    return SCHEDULES_SCHEMA.empty_dataframe()


def create_empty_boxscores_df() -> pd.DataFrame:
    """Create an empty boxscores DataFrame with correct schema."""
    return BOXSCORES_SCHEMA.empty_dataframe()


def validate_seasons_df(df: pd.DataFrame) -> list[str]:
    """Validate a seasons DataFrame against the schema. Returns list of errors."""
    errors = []
    for col in SeasonColumns.required():
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    return errors


def validate_schedules_df(df: pd.DataFrame) -> list[str]:
    """Validate a schedules DataFrame against the schema. Returns list of errors."""
    errors = []
    for col in ScheduleColumns.required():
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    return errors


def validate_boxscores_df(df: pd.DataFrame) -> list[str]:
    """Validate a boxscores DataFrame against the schema. Returns list of errors."""
    errors = []
    for col in BoxscoreColumns.required():
        if col not in df.columns:
            errors.append(f"Missing required column: {col}")
    return errors


# =============================================================================
# Raw HTML Column Names (as they appear in scraped HTML tables)
# =============================================================================

class RawSeasonColumns:
    """Raw column names as they appear in the HTML season tables."""
    SEASON = "Season"
    SEASON_URL = "Season URL"
    LEAGUE = "League"
    LEAGUES = "Leagues"  # Alternative column name in some tables
    LEAGUE_URL = "League URL"


class RawScheduleColumns:
    """Raw column names as they appear in the HTML schedule tables."""
    DATE = "Date"
    TEAM = "Team"
    PTS = "PTS"
    OPP = "Opp"
    PTS_1 = "PTS.1"
    OT = "OT"
    NOTES = "Notes"
    DATE_URL = "Date URL"
    # Intermediate columns (added during processing before final rename)
    HOME = "Home"
    HOME_POINTS = "HomePoints"
    VISITORS = "Visitors"
    VISITORS_POINTS = "VisitorsPoints"
    LEAGUE = "League"
    SCHEDULE_URL = "Schedule URL"


class RawBoxscoreColumns:
    """Raw column names as they appear in the HTML boxscore tables."""
    PLAYER = "Player"
    MP = "MP"
    FG = "FG"
    FGA = "FGA"
    FG_PCT = "FG%"
    THREE_P = "3P"
    THREE_PA = "3PA"
    THREE_P_PCT = "3P%"
    FT = "FT"
    FTA = "FTA"
    FT_PCT = "FT%"
    ORB = "ORB"
    DRB = "DRB"
    TRB = "TRB"
    AST = "AST"
    STL = "STL"
    BLK = "BLK"
    TOV = "TOV"
    PF = "PF"
    PTS = "PTS"


# =============================================================================
# Column Name Mappings (for renaming raw scraped data)
# =============================================================================

# Mapping from raw scraped column names to standardized schema names (first pass - from HTML)
SCHEDULE_RAW_RENAME_MAP: dict[str, str] = {
    RawScheduleColumns.TEAM: RawScheduleColumns.HOME,
    RawScheduleColumns.PTS: RawScheduleColumns.HOME_POINTS,
    RawScheduleColumns.OPP: RawScheduleColumns.VISITORS,
    RawScheduleColumns.PTS_1: RawScheduleColumns.VISITORS_POINTS,
    RawScheduleColumns.OT: ScheduleColumns.HAS_GONE_OVERTIME.name,
    RawScheduleColumns.DATE_URL: ScheduleColumns.DATE_URL.name,
}

# Second pass - rename to final standardized names
SCHEDULE_COLUMN_RENAME_MAP: dict[str, str] = {
    RawScheduleColumns.DATE: ScheduleColumns.GAME_DATE.name,
    RawScheduleColumns.HOME: ScheduleColumns.TEAM_NAME_HOME.name,
    RawScheduleColumns.HOME_POINTS: ScheduleColumns.TEAM_HOME_POINTS.name,
    RawScheduleColumns.VISITORS: ScheduleColumns.TEAM_NAME_VISITORS.name,
    RawScheduleColumns.VISITORS_POINTS: ScheduleColumns.TEAM_VISITORS_POINTS.name,
    RawScheduleColumns.LEAGUE: ScheduleColumns.LEAGUE_NAME.name,
    RawScheduleColumns.SCHEDULE_URL: ScheduleColumns.SCHEDULE_URL.name,
}

# Core columns expected in raw schedule data before renaming
SCHEDULE_RAW_COLUMNS: list[str] = [
    RawScheduleColumns.DATE,
    RawScheduleColumns.TEAM,
    RawScheduleColumns.PTS,
    RawScheduleColumns.OPP,
    RawScheduleColumns.PTS_1,
    RawScheduleColumns.OT,
    RawScheduleColumns.NOTES,
    RawScheduleColumns.DATE_URL,
]

# Mapping from raw boxscore column names to standardized schema names
BOXSCORE_COLUMN_RENAME_MAP: dict[str, str] = {
    RawBoxscoreColumns.PLAYER: BoxscoreColumns.PLAYER_NAME.name,
    RawBoxscoreColumns.MP: BoxscoreColumns.MINUTES_PLAYED.name,
    RawBoxscoreColumns.FG: BoxscoreColumns.FIELD_GOALS_MADE.name,
    RawBoxscoreColumns.FGA: BoxscoreColumns.FIELD_GOALS_ATTEMPTED.name,
    RawBoxscoreColumns.FG_PCT: BoxscoreColumns.FIELD_GOAL_PERCENTAGE.name,
    RawBoxscoreColumns.THREE_P: BoxscoreColumns.THREE_POINT_MADE.name,
    RawBoxscoreColumns.THREE_PA: BoxscoreColumns.THREE_POINT_ATTEMPTED.name,
    RawBoxscoreColumns.THREE_P_PCT: BoxscoreColumns.THREE_POINT_PERCENTAGE.name,
    RawBoxscoreColumns.FT: BoxscoreColumns.FREE_THROWS_MADE.name,
    RawBoxscoreColumns.FTA: BoxscoreColumns.FREE_THROWS_ATTEMPTED.name,
    RawBoxscoreColumns.FT_PCT: BoxscoreColumns.FREE_THROW_PERCENTAGE.name,
    RawBoxscoreColumns.ORB: BoxscoreColumns.OFFENSIVE_REBOUNDS.name,
    RawBoxscoreColumns.DRB: BoxscoreColumns.DEFENSIVE_REBOUNDS.name,
    RawBoxscoreColumns.TRB: BoxscoreColumns.TOTAL_REBOUNDS.name,
    RawBoxscoreColumns.AST: BoxscoreColumns.ASSISTS.name,
    RawBoxscoreColumns.STL: BoxscoreColumns.STEALS.name,
    RawBoxscoreColumns.BLK: BoxscoreColumns.BLOCKS.name,
    RawBoxscoreColumns.TOV: BoxscoreColumns.TURNOVERS.name,
    RawBoxscoreColumns.PF: BoxscoreColumns.PERSONAL_FOULS.name,
    RawBoxscoreColumns.PTS: BoxscoreColumns.POINTS.name,
    RawScheduleColumns.SCHEDULE_URL: BoxscoreColumns.SCHEDULE_URL.name,
    RawScheduleColumns.LEAGUE: BoxscoreColumns.LEAGUE.name,
}
