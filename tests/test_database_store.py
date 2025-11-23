import pandas as pd
from sqlalchemy.exc import SQLAlchemyError

import src.storage.database_store as database_store_module
from src.configs.settings import DatabaseConfig
from src.storage.database_store import DatabaseStore


def test_database_store_writes_and_counts_rows(tmp_path):
    db_path = tmp_path / "test.db"
    store = DatabaseStore(
        url=f"sqlite:///{db_path}",
        schema="",
        season_table="season_rows",
        schedule_table="schedule_games",
        boxscore_table="boxscores",
    )

    seasons_df = pd.DataFrame({"Season": ["2024-25"], "League": ["EuroLeague"]})
    schedules_df = pd.DataFrame({"GameId": [1, 2], "Home": ["A", "B"]})
    boxscores_df = pd.DataFrame({"Player": ["Alice", "Bob"], "PTS": [20, 15]})

    assert store.save_seasons(seasons_df) == 1
    assert store.save_schedules(schedules_df) == 2
    assert store.save_boxscores(boxscores_df) == 2

    assert store.row_count("season_rows") == 1
    assert store.row_count("schedule_games") == 2
    assert store.row_count("boxscores") == 2


def test_database_store_fallbacks_to_sqlite(monkeypatch, tmp_path):
    fallback_path = tmp_path / "fallback.db"
    monkeypatch.setattr(DatabaseConfig, "DEFAULT_SQLITE_PATH", fallback_path)
    monkeypatch.setattr(DatabaseConfig, "DEFAULT_SQLITE_URL", f"sqlite:///{fallback_path}")

    real_create_engine = database_store_module.create_engine

    def fake_create_engine(url, future=True):
        if url.startswith("postgresql"):
            raise SQLAlchemyError("connection refused")
        return real_create_engine(url, future=future)

    monkeypatch.setattr(database_store_module, "create_engine", fake_create_engine)

    store = DatabaseStore(
        url="postgresql+psycopg://invalid:invalid@localhost:5432/missing",
        schema="basketball_hoops",
        season_table="season_rows",
    )

    # Should fall back to SQLite default
    assert store.url.startswith("sqlite")
    assert store.schema is None

    seasons_df = pd.DataFrame({"Season": ["2024-25"], "League": ["EuroLeague"]})
    assert store.save_seasons(seasons_df) == 1
    assert store.row_count("season_rows") == 1