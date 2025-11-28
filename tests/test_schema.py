"""Tests for the centralized schema definitions."""

import pandas as pd
import pytest

from src.configs.schema import (
    Column,
    SeasonColumns,
    ScheduleColumns,
    BoxscoreColumns,
    TableSchema,
    SEASONS_SCHEMA,
    SCHEDULES_SCHEMA,
    BOXSCORES_SCHEMA,
    SEASON_COLUMN_RENAME_MAP,
    SCHEDULE_RAW_RENAME_MAP,
    SCHEDULE_COLUMN_RENAME_MAP,
    SCHEDULE_RAW_COLUMNS,
    create_empty_seasons_df,
    create_empty_schedules_df,
    create_empty_boxscores_df,
    validate_seasons_df,
    validate_schedules_df,
    validate_boxscores_df,
)


class TestColumnDefinitions:
    """Tests for Column dataclass."""

    def test_column_is_frozen(self):
        col = Column("Test", "str")
        with pytest.raises(AttributeError):
            col.name = "Modified"

    def test_column_defaults(self):
        col = Column("Test", "str")
        assert col.nullable is True
        assert col.description == ""


class TestSeasonColumns:
    """Tests for SeasonColumns schema."""

    def test_all_columns_returns_list(self):
        cols = SeasonColumns.all_columns()
        assert isinstance(cols, list)
        assert len(cols) == 5

    def test_names_returns_strings(self):
        names = SeasonColumns.names()
        assert all(isinstance(n, str) for n in names)
        assert "Season" in names
        assert "League" in names

    def test_required_columns(self):
        required = SeasonColumns.required()
        assert "Season" in required
        assert "League" in required
        assert "SeasonURL" not in required  # nullable


class TestScheduleColumns:
    """Tests for ScheduleColumns schema."""

    def test_all_columns_count(self):
        assert len(ScheduleColumns.all_columns()) == 11

    def test_required_columns(self):
        required = ScheduleColumns.required()
        assert "GameDate" in required
        assert "TeamNameHome" in required
        assert "TeamNameVisitors" in required
        assert "Notes" not in required  # nullable


class TestBoxscoreColumns:
    """Tests for BoxscoreColumns schema."""

    def test_context_columns(self):
        ctx = BoxscoreColumns.context_columns()
        names = [c.name for c in ctx]
        assert "TeamRole" in names
        assert "Team" in names
        assert "Date" in names

    def test_required_columns(self):
        required = BoxscoreColumns.required()
        assert "TeamRole" in required
        assert "Team" in required


class TestTableSchema:
    """Tests for TableSchema class."""

    def test_column_names(self):
        names = SEASONS_SCHEMA.column_names()
        assert names == SeasonColumns.names()

    def test_dtype_map(self):
        dtypes = SEASONS_SCHEMA.dtype_map()
        assert dtypes["Season"] == "str"

    def test_empty_dataframe(self):
        df = SEASONS_SCHEMA.empty_dataframe()
        assert isinstance(df, pd.DataFrame)
        assert list(df.columns) == SeasonColumns.names()
        assert len(df) == 0


class TestHelperFunctions:
    """Tests for schema helper functions."""

    def test_create_empty_seasons_df(self):
        df = create_empty_seasons_df()
        assert len(df) == 0
        assert "Season" in df.columns

    def test_create_empty_schedules_df(self):
        df = create_empty_schedules_df()
        assert len(df) == 0
        assert "TeamNameHome" in df.columns

    def test_create_empty_boxscores_df(self):
        df = create_empty_boxscores_df()
        assert len(df) == 0
        assert "TeamRole" in df.columns

    def test_validate_seasons_df_valid(self):
        df = pd.DataFrame({"Season": ["2024-25"], "League": ["EuroLeague"]})
        errors = validate_seasons_df(df)
        assert errors == []

    def test_validate_seasons_df_missing_column(self):
        df = pd.DataFrame({"Season": ["2024-25"]})
        errors = validate_seasons_df(df)
        assert len(errors) == 1
        assert "League" in errors[0]

    def test_validate_schedules_df_valid(self):
        df = pd.DataFrame({
            "GameDate": [pd.Timestamp("2025-01-01")],
            "TeamNameHome": ["Team A"],
            "TeamNameVisitors": ["Team B"],
            "Season": ["2024-25"],
            "LeagueName": ["EuroLeague"],
        })
        errors = validate_schedules_df(df)
        assert errors == []

    def test_validate_boxscores_df_valid(self):
        df = pd.DataFrame({
            "TeamRole": ["Home"],
            "Team": ["Team A"],
            "Date": [pd.Timestamp("2025-01-01")],
            "Season": ["2024-25"],
            "League": ["EuroLeague"],
        })
        errors = validate_boxscores_df(df)
        assert errors == []


class TestColumnMappings:
    """Tests for column rename mappings."""

    def test_season_column_rename_map_keys(self):
        expected_keys = {"Season URL", "League URL", "Schedule URL"}
        assert set(SEASON_COLUMN_RENAME_MAP.keys()) == expected_keys

    def test_season_column_rename_map_values(self):
        assert SEASON_COLUMN_RENAME_MAP["Season URL"] == "SeasonURL"
        assert SEASON_COLUMN_RENAME_MAP["League URL"] == "LeagueURL"
        assert SEASON_COLUMN_RENAME_MAP["Schedule URL"] == "ScheduleURL"

    def test_schedule_raw_rename_map_keys(self):
        # First-pass mapping: raw scraped → intermediate
        expected_keys = {"Team", "PTS", "Opp", "PTS.1", "OT", "Date URL"}
        assert set(SCHEDULE_RAW_RENAME_MAP.keys()) == expected_keys

    def test_schedule_raw_rename_map_values(self):
        # First-pass mapping: raw scraped → intermediate
        assert SCHEDULE_RAW_RENAME_MAP["Team"] == "Home"
        assert SCHEDULE_RAW_RENAME_MAP["Opp"] == "Visitors"

    def test_schedule_column_rename_map_keys(self):
        # Second-pass mapping: intermediate → final standardized
        expected_keys = {"Date", "Home", "HomePoints", "Visitors", "VisitorsPoints", "League", "Schedule URL"}
        assert set(SCHEDULE_COLUMN_RENAME_MAP.keys()) == expected_keys

    def test_schedule_column_rename_map_values(self):
        # Second-pass mapping: intermediate → final standardized
        assert SCHEDULE_COLUMN_RENAME_MAP["Date"] == "GameDate"
        assert SCHEDULE_COLUMN_RENAME_MAP["Home"] == "TeamNameHome"
        assert SCHEDULE_COLUMN_RENAME_MAP["League"] == "LeagueName"

    def test_schedule_raw_columns(self):
        assert "Date" in SCHEDULE_RAW_COLUMNS
        assert "Team" in SCHEDULE_RAW_COLUMNS
        assert "Notes" in SCHEDULE_RAW_COLUMNS
