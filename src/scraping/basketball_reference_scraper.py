"""Basketball Reference scraper with retry logic and centralized logging."""

from __future__ import annotations

import re
import time
from datetime import date
from io import StringIO
from typing import TYPE_CHECKING, Optional, Sequence, Type
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup, Comment, Tag

from src.configs.logging_config import get_logger

if TYPE_CHECKING:
    from src.storage.base import StorageBackend

logger = get_logger(__name__)


class ScraperError(Exception):
    """Base exception for scraper errors."""


class FetchError(ScraperError):
    """Raised when fetching a URL fails after all retries."""


class ParseError(ScraperError):
    """Raised when parsing HTML fails."""

class BasketballReferenceScraper:
    """Scrapes the international leagues table from Basketball Reference."""

    BASE_URL = "https://www.basketball-reference.com"
    INTERNATIONAL_PATH = "/international/years/"

    # Throttling and retry configuration
    REQUEST_DELAY_SECONDS: float = 2.0
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_FACTOR: float = 2.0
    RETRY_STATUS_CODES: frozenset[int] = frozenset({429, 500, 502, 503, 504})

    def __init__(
        self,
        relative_path: Optional[str] = None,
        session: Optional[requests.Session] = None,
        request_delay: Optional[float] = None,
        max_retries: Optional[int] = None,
    ) -> None:
        """
        Initialize the scraper.

        Parameters
        ----------
        relative_path : Optional[str]
            Override for the international endpoint, appended to the base URL.
        session : Optional[requests.Session]
            Reusable requests session for connection pooling.
        request_delay : Optional[float]
            Seconds to wait between requests (default: 2.0).
        max_retries : Optional[int]
            Maximum retry attempts for transient failures (default: 3).
        """
        self.base_url = self.BASE_URL.rstrip("/")
        relative_path = relative_path or self.INTERNATIONAL_PATH
        self.target_path = relative_path.lstrip("/")
        self.league_url = urljoin(f"{self.base_url}/", self.target_path)
        self.session = session or requests.Session()
        self.request_delay = (
            request_delay if request_delay is not None else self.REQUEST_DELAY_SECONDS
        )
        self.max_retries = max_retries if max_retries is not None else self.MAX_RETRIES

    def fetch_data(self, url: Optional[str] = None) -> str:
        """
        Fetch HTML content from a URL with retry logic.

        Raises
        ------
        FetchError
            If the request fails after all retries.
        """
        target_url = url or self.league_url
        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_retries + 1):
            time.sleep(self.request_delay)
            try:
                response = self.session.get(target_url, timeout=30)
                if response.status_code == 200:
                    logger.debug("Fetched %s on attempt %d", target_url, attempt)
                    return response.text

                if response.status_code in self.RETRY_STATUS_CODES:
                    wait = self.request_delay * (self.RETRY_BACKOFF_FACTOR ** (attempt - 1))
                    logger.warning(
                        "Received %d from %s; retrying in %.1fs (attempt %d/%d)",
                        response.status_code,
                        target_url,
                        wait,
                        attempt,
                        self.max_retries,
                    )
                    time.sleep(wait)
                    continue

                # Non-retryable HTTP error
                raise FetchError(f"HTTP {response.status_code} for {target_url}")

            except requests.RequestException as exc:
                last_exc = exc
                wait = self.request_delay * (self.RETRY_BACKOFF_FACTOR ** (attempt - 1))
                logger.warning(
                    "Request failed for %s: %s; retrying in %.1fs (attempt %d/%d)",
                    target_url,
                    exc,
                    wait,
                    attempt,
                    self.max_retries,
                )
                time.sleep(wait)

        raise FetchError(
            f"Failed to fetch {target_url} after {self.max_retries} attempts"
        ) from last_exc

    @staticmethod
    def _extract_href(value: object) -> Optional[str]:
        """pd.read_html with extract_links returns tuples of (text, href)."""

        if isinstance(value, tuple) and len(value) == 2:
            return value[1]
        return None

    @staticmethod
    def _second_level_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Return a copy of df whose columns are the second level if a MultiIndex is present."""

        if isinstance(df.columns, pd.MultiIndex):
            if df.columns.nlevels < 2:
                return df
            df = df.copy()
            df.columns = df.columns.get_level_values(1)
        return df

    @staticmethod
    def _unwrap_commented_tables(soup: BeautifulSoup) -> None:
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            if '<table' in comment:
                fragment = BeautifulSoup(comment, 'html.parser')
                comment.replace_with(fragment)

    @staticmethod
    def _get_column(df: pd.DataFrame, candidates: Sequence[str], field_name: str) -> pd.Series:
        """Return the first matching column Series given a list of candidate names."""
        for candidate in candidates:
            if candidate in df.columns:
                return df[candidate]
        raise ParseError(f"Expected column '{field_name}' not found. Tried: {', '.join(candidates)}")

    @staticmethod
    def _season_year_from_url(season_url: Optional[str]) -> Optional[str]:
        if not season_url:
            return None
        match = re.search(r"(\d{4})(?=\.html?$)", season_url)
        if match:
            return match.group(1)
        return None

    def _build_schedule_url(self, season_url: Optional[str], league_url: Optional[str]) -> Optional[str]:
        year = self._season_year_from_url(season_url)
        if not (year and league_url):
            return None
        normalized_league_url = league_url.rstrip('/')
        if normalized_league_url.endswith('.html'):
            normalized_league_url = normalized_league_url.rsplit('/', 1)[0]
        return f"{normalized_league_url}/{year}-schedule.html"

    @staticmethod
    def _parse_first_table(html: str) -> tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        soup = BeautifulSoup(html, 'html.parser')
        BasketballReferenceScraper._unwrap_commented_tables(soup)
        table = soup.find('table')
        if not table:
            raise ParseError("No table found on the page.")

        table_html = str(table)
        data_df = pd.read_html(StringIO(table_html))[0]
        link_df: Optional[pd.DataFrame] = None
        try:
            link_df = pd.read_html(StringIO(table_html), extract_links="body")[0]
        except (TypeError, ValueError):
            pass

        return data_df, link_df

    @staticmethod
    def _parse_game_date(value: object) -> Optional[pd.Timestamp]:
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        if not cleaned or not re.search(r"\b\d{1,2}\b", cleaned):
            return None

        parsed = pd.to_datetime(cleaned, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.normalize()

    def _parse_boxscore_tables(self, html: str) -> dict[str, pd.DataFrame]:
        soup = BeautifulSoup(html, 'html.parser')
        self._unwrap_commented_tables(soup)
        table_elements: dict[str, Tag] = {}
        role_map = [
            ('div_box-score-visitor', 'Visitors'),
            ('div_box-score-home', 'Home')
        ]

        for div_id, role in role_map:
            div = soup.find('div', id=div_id)
            if not div:
                continue
            table = div.find('table')
            if table:
                table_elements[role] = table

        combined_div = soup.find('div', id='div_box-score')
        if combined_div:
            combined_tables = combined_div.find_all('table')
            for role, table in zip(['Visitors', 'Home'], combined_tables):
                if role not in table_elements and table:
                    table_elements[role] = table

        if len(table_elements) < 2:
            fallback_tables = soup.find_all('table')
            for table in fallback_tables:
                missing_roles = [role for role in ['Visitors', 'Home'] if role not in table_elements]
                if not missing_roles:
                    break
                table_elements[missing_roles[0]] = table

        parsed_tables: dict[str, pd.DataFrame] = {}
        for role, table in table_elements.items():
            try:
                df = pd.read_html(StringIO(str(table)))[0]
                parsed_tables[role] = self._second_level_columns(df)
            except ValueError:
                continue

        return parsed_tables

    def parse_data(self, html: str) -> pd.DataFrame:
        logger.info("Parsing season listings HTML")
        full_table_df, link_table_df = self._parse_first_table(html)
        full_table_df = self._second_level_columns(full_table_df)
        if link_table_df is not None:
            link_table_df = self._second_level_columns(link_table_df)

        season_series = self._get_column(full_table_df, ['Season'], 'Season')
        league_series = self._get_column(full_table_df, ['League', 'Leagues'], 'League')

        if link_table_df is not None:
            season_url_series = self._get_column(link_table_df, ['Season'], 'Season URL')
            league_url_series = self._get_column(link_table_df, ['League', 'Leagues'], 'League URL')
            season_urls = season_url_series.map(self._extract_href)
            league_urls = league_url_series.map(self._extract_href)
        else:
            season_urls = pd.Series([None] * len(season_series), index=season_series.index)
            league_urls = pd.Series([None] * len(league_series), index=league_series.index)

        df = pd.DataFrame({
            'Season': season_series,
            'Season URL': season_urls.map(lambda href: urljoin(self.base_url, href) if href else None),
            'League': league_series,
            'League URL': league_urls.map(lambda href: urljoin(self.base_url, href) if href else None)
        })

        df = df.replace(r'^\s*$', pd.NA, regex=True)
        df = df.dropna(subset=['Season', 'League']).reset_index(drop=True)
        df['Season'] = df['Season'].astype(str)
        df['League'] = df['League'].astype(str)
        df['Schedule URL'] = df.apply(
            lambda row: self._build_schedule_url(row['Season URL'], row['League URL']), axis=1
        )

        logger.info("Parsed %d season rows with schedule URLs", len(df))
        return df

    def scrape(self):
        logger.info("Starting season scrape from %s", self.league_url)
        html = self.fetch_data()
        data = self.parse_data(html)
        logger.info("Season scrape completed with %d rows", len(data))
        return data

    def scrape_league_schedule(self, season: str, league: str, schedule_url: str) -> pd.DataFrame:
        logger.info("Scraping schedule for %s %s (%s)", league, season, schedule_url)
        html = self.fetch_data(schedule_url)
        schedule_df, schedule_link_df = self._parse_first_table(html)
        if schedule_link_df is not None:
            schedule_link_df = self._second_level_columns(schedule_link_df)

        if schedule_link_df is not None:
            try:
                date_link_series = self._get_column(schedule_link_df, ['Date'], 'Date URL')
                date_urls = date_link_series.map(self._extract_href)
            except ParseError:
                date_urls = pd.Series([None] * len(schedule_df))
        else:
            date_urls = pd.Series([None] * len(schedule_df))

        parsed_dates = schedule_df['Date'].map(self._parse_game_date)
        valid_mask = parsed_dates.notna()
        schedule_df = schedule_df.loc[valid_mask].reset_index(drop=True)
        parsed_dates = parsed_dates.loc[valid_mask].reset_index(drop=True)
        date_urls = date_urls.loc[valid_mask].reset_index(drop=True)

        schedule_df['Date'] = parsed_dates.dt.date
        schedule_df['Date URL'] = date_urls.map(lambda href: urljoin(self.base_url, href) if href else None)
        schedule_df['Season'] = season
        schedule_df['League'] = league
        schedule_df['Schedule URL'] = schedule_url

        core_columns = ['Date', 'Team', 'PTS', 'Opp', 'PTS.1', 'OT', 'Notes', 'Date URL']
        for column in core_columns:
            if column not in schedule_df.columns:
                schedule_df[column] = None

        ordered_columns = core_columns + ['Season', 'League', 'Schedule URL']
        schedule_df = schedule_df[ordered_columns]

        rename_map = {
            'Team': 'Home',
            'PTS': 'HomePoints',
            'Opp': 'Visitors',
            'PTS.1': 'VisitorsPoints',
            'OT': 'HasGoneOvertime',
            'Date URL': 'DateURL'
        }
        schedule_df = schedule_df.rename(columns=rename_map)

        logger.info("Scraped %d schedule rows for %s %s", len(schedule_df), league, season)
        return schedule_df

    @staticmethod
    def filter_schedule_by_date(
        schedule_df: pd.DataFrame,
        target_date: Optional[date],
    ) -> pd.DataFrame:
        if target_date is None or schedule_df.empty:
            return schedule_df
        filtered = schedule_df.loc[schedule_df['Date'] == target_date]
        return filtered.reset_index(drop=True)

    def scrape_league_schedules(self, df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        base_df = df if df is not None else self.scrape()
        logger.info("Scraping schedules for %d season entries", len(base_df))
        schedules: list[pd.DataFrame] = []
        for _, row in base_df.iterrows():
            schedule_url = row.get('Schedule URL')
            if not schedule_url:
                continue
            try:
                schedule_df = self.scrape_league_schedule(row['Season'], row['League'], schedule_url)
                schedules.append(schedule_df)
            except ScraperError as exc:
                logger.error(
                    "Failed to scrape schedule for %s %s: %s",
                    row['League'],
                    row['Season'],
                    exc,
                )

        if schedules:
            combined = pd.concat(schedules, ignore_index=True)
            logger.info("Aggregated %d schedule rows across %d leagues", len(combined), len(base_df))
            return combined
        logger.info("No schedules could be scraped from %d seasons", len(base_df))
        return pd.DataFrame(columns=[
            'Date', 'Home', 'HomePoints', 'Visitors', 'VisitorsPoints',
            'HasGoneOvertime', 'Notes', 'DateURL', 'Season', 'League', 'Schedule URL'
        ])

    def scrape_boxscore_tables(
        self,
        schedule_df: pd.DataFrame,
        store: Optional["StorageBackend"] = None,
    ) -> pd.DataFrame:
        if schedule_df.empty:
            logger.info("No schedule rows provided; skipping boxscore scrape")
            return pd.DataFrame()

        logger.info("Scraping boxscore tables for %d schedule rows", len(schedule_df))
        results: list[pd.DataFrame] = []
        html_cache: dict[str, dict[str, pd.DataFrame]] = {}
        first_chunk = True
        skipped_dates: list[str] = []
        logged_boxscores: set[str] = set()

        for _, row in schedule_df.iterrows():
            date_url = row.get('DateURL')
            if not date_url:
                continue

            if date_url not in html_cache:
                if date_url not in logged_boxscores:
                    logger.info(
                        "Scraping boxscore for %s vs %s on %s (%s)",
                        row.get('Home'),
                        row.get('Visitors'),
                        row.get('Date'),
                        date_url,
                    )
                    logged_boxscores.add(date_url)
                try:
                    html = self.fetch_data(date_url)
                    html_cache[date_url] = self._parse_boxscore_tables(html)
                except ScraperError as exc:
                    logger.error(
                        "Failed to fetch boxscore %s for %s vs %s (%s %s): %s",
                        date_url,
                        row.get('Home'),
                        row.get('Visitors'),
                        row.get('League'),
                        row.get('Season'),
                        exc,
                    )
                    skipped_dates.append(date_url)
                    continue

            table_map = html_cache.get(date_url, {})
            if not table_map:
                logger.warning("No boxscore tables found for %s", date_url)
                continue

            row_tables: list[pd.DataFrame] = []
            team_pairs = [('Visitors', row.get('Visitors')), ('Home', row.get('Home'))]
            for role, team_name in team_pairs:
                if team_name is None:
                    continue
                table_df = table_map.get(role)
                if table_df is None:
                    continue
                table_df = table_df.copy()
                table_df['TeamRole'] = role
                table_df['Team'] = team_name
                table_df['Date'] = row.get('Date')
                table_df['Season'] = row.get('Season')
                table_df['League'] = row.get('League')
                table_df['Schedule URL'] = row.get('Schedule URL')
                table_df['DateURL'] = date_url
                row_tables.append(table_df)

            if not row_tables:
                continue

            row_df = pd.concat(row_tables, ignore_index=True)
            results.append(row_df)

            if store is not None:
                mode = "replace" if first_chunk else "append"
                store.save_boxscores(row_df, if_exists=mode)
                first_chunk = False

        if results:
            combined = pd.concat(results, ignore_index=True)
            logger.info("Collected %d boxscore rows", len(combined))
            if skipped_dates:
                logger.info(
                    "Skipped %d boxscore pages due to fetch errors.",
                    len(skipped_dates),
                )
            return combined
        if skipped_dates:
            logger.info(
                "Skipped %d boxscore pages due to fetch errors.",
                len(skipped_dates),
            )
        logger.info("No boxscore tables were collected")
        return pd.DataFrame()


def scrape_data(scraper_cls: Optional[Type[BasketballReferenceScraper]] = None) -> pd.DataFrame:
    """Convenience helper mirroring the previous module-level API."""

    scraper_type = scraper_cls or BasketballReferenceScraper
    scraper = scraper_type()
    return scraper.scrape()

# Example usage:
# scraper = BasketballReferenceScraper()
# data = scraper.scrape()
# print(data.head())