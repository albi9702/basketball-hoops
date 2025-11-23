# Contents of /basketball-hoops/basketball-hoops/src/configs/settings.py

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration settings
class Config:
    BASE_URL = "https://www.basketball-reference.com"
    DATA_DIR = os.path.join(os.path.dirname(__file__), '../../data')
    RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
    PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
    API_KEY = os.getenv("API_KEY")  # Example for future API usage
    TIMEOUT = 10  # Timeout for requests in seconds


class DatabaseConfig:
    """Centralized database naming so schema/table names live in one place."""

    DEFAULT_SQLITE_PATH = Path(Config.DATA_DIR).resolve() / "basketball_hoops.db"
    DEFAULT_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_SQLITE_URL = f"sqlite:///{DEFAULT_SQLITE_PATH}"

    URL = os.getenv(
        "DATABASE_URL",
        DEFAULT_SQLITE_URL,
    )
    _schema_env = os.getenv("DATABASE_SCHEMA")
    if _schema_env is not None:
        SCHEMA = _schema_env or None
    else:
        SCHEMA = None if URL.startswith("sqlite") else "basketball_hoops"
    SEASON_TABLE = os.getenv("DATABASE_SEASON_TABLE", "seasons")
    SCHEDULE_TABLE = os.getenv("DATABASE_SCHEDULE_TABLE", "schedule_games")
    BOXSCORE_TABLE = os.getenv("DATABASE_BOXSCORE_TABLE", "boxscores")


# Example of how to use the configuration
if __name__ == "__main__":
    print("Base URL:", Config.BASE_URL)
    print("Raw Data Directory:", Config.RAW_DATA_DIR)
    print("Processed Data Directory:", Config.PROCESSED_DATA_DIR)