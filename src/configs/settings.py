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


class ScraperAPIConfig:
    """Configuration for ScraperAPI proxy service.
    
    ScraperAPI is only used when:
    1. SCRAPER_API_KEY is set, AND
    2. Running in GitHub Actions (GITHUB_ACTIONS=true) OR USE_SCRAPER_API=true
    
    This prevents accidentally using API credits when running locally in VS Code.
    """
    
    API_KEY = os.getenv("SCRAPER_API_KEY")
    BASE_URL = "http://api.scraperapi.com"
    
    # Only use ScraperAPI in CI/CD or when explicitly enabled
    _is_github_actions = os.getenv("GITHUB_ACTIONS", "").lower() == "true"
    _force_enabled = os.getenv("USE_SCRAPER_API", "").lower() == "true"
    
    @classmethod
    def is_enabled(cls) -> bool:
        """Check if ScraperAPI should be used.
        
        Returns True only if:
        - API key is set AND
        - Running in GitHub Actions OR USE_SCRAPER_API=true
        """
        if not cls.API_KEY:
            return False
        # Only use in GitHub Actions or if explicitly forced
        return cls._is_github_actions or cls._force_enabled
    
    @classmethod
    def get_proxy_url(cls, target_url: str) -> str:
        """Build the ScraperAPI proxy URL for a target URL."""
        if not cls.API_KEY:
            raise ValueError("SCRAPER_API_KEY not configured")
        # Using URL parameter method for ScraperAPI
        from urllib.parse import quote
        return f"{cls.BASE_URL}?api_key={cls.API_KEY}&url={quote(target_url)}"


class DatabaseConfig:
    """Centralized database naming so schema/table names live in one place."""

    DEFAULT_SQLITE_PATH = Path(Config.DATA_DIR).resolve() / "basketball_hoops.db"
    DEFAULT_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_SQLITE_URL = f"sqlite:///{DEFAULT_SQLITE_PATH}"

    _raw_url = os.getenv("DATABASE_URL", DEFAULT_SQLITE_URL)
    
    # Ensure correct driver for psycopg3 (not psycopg2)
    # Convert postgresql:// or postgresql+psycopg2:// to postgresql+psycopg://
    if _raw_url.startswith("postgresql://"):
        URL = _raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    elif _raw_url.startswith("postgresql+psycopg2://"):
        URL = _raw_url.replace("postgresql+psycopg2://", "postgresql+psycopg://", 1)
    else:
        URL = _raw_url
    
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