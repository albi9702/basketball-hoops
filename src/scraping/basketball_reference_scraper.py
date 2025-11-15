import re
from io import StringIO
from typing import Optional, Sequence, Type
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

class BasketballReferenceScraper:
    """Scrapes the international leagues table from Basketball Reference."""

    BASE_URL = "https://www.basketball-reference.com"
    INTERNATIONAL_PATH = "/international/years/"

    def __init__(self, relative_path: Optional[str] = None, session: Optional[requests.Session] = None):
        """
        Parameters
        ----------
        relative_path: Optional[str]
            Override for the international endpoint, appended to the base URL.
        session: Optional[requests.Session]
            Reusable requests session for easier testing and connection pooling.
        """

        self.base_url = self.BASE_URL.rstrip("/")
        relative_path = relative_path or self.INTERNATIONAL_PATH
        self.target_path = relative_path.lstrip("/")
        self.league_url = urljoin(f"{self.base_url}/", self.target_path)
        self.session = session or requests.Session()

    def fetch_data(self, url: Optional[str] = None) -> str:
        target_url = url or self.league_url
        response = self.session.get(target_url)
        if response.status_code == 200:
            return response.text
        raise Exception(f"Failed to fetch data: {response.status_code}")

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
    def _get_column(df: pd.DataFrame, candidates: Sequence[str], field_name: str) -> pd.Series:
        """Return the first matching column Series given a list of candidate names."""

        for candidate in candidates:
            if candidate in df.columns:
                return df[candidate]
        raise Exception(f"Expected column '{field_name}' not found. Tried: {', '.join(candidates)}")

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
        normalized_league_url = league_url.rstrip('/') + '/'
        return f"{normalized_league_url}{year}-schedule.html"

    @staticmethod
    def _parse_first_table(html: str) -> pd.DataFrame:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table:
            raise Exception("No table found on the page.")
        return pd.read_html(StringIO(str(table)))[0]

    def parse_data(self, html: str) -> pd.DataFrame:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table:
            raise Exception("No table found on the page.")

        table_html = str(table)
        full_table_df = pd.read_html(StringIO(table_html))[0]
        full_table_df = self._second_level_columns(full_table_df)
        link_table_df = None
        try:
            link_table_df = pd.read_html(StringIO(table_html), extract_links='body')[0]
            link_table_df = self._second_level_columns(link_table_df)
        except (TypeError, ValueError):
            # Older pandas versions do not support extract_links; fallback handled below.
            pass

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

        return df

    def scrape(self):
        html = self.fetch_data()
        data = self.parse_data(html)
        return data

    def scrape_league_schedule(self, season: str, league: str, schedule_url: str) -> pd.DataFrame:
        html = self.fetch_data(schedule_url)
        schedule_df = self._parse_first_table(html)
        schedule_df['Season'] = season
        schedule_df['League'] = league
        schedule_df['Schedule URL'] = schedule_url
        return schedule_df

    def scrape_league_schedules(self, df: Optional[pd.DataFrame] = None) -> dict[str, pd.DataFrame]:
        base_df = df if df is not None else self.scrape()
        schedules: dict[str, pd.DataFrame] = {}
        for _, row in base_df.iterrows():
            schedule_url = row.get('Schedule URL')
            if not schedule_url:
                continue
            schedules[f"{row['Season']}|{row['League']}"] = self.scrape_league_schedule(
                row['Season'], row['League'], schedule_url
            )
        return schedules


def scrape_data(scraper_cls: Optional[Type[BasketballReferenceScraper]] = None) -> pd.DataFrame:
    """Convenience helper mirroring the previous module-level API."""

    scraper_type = scraper_cls or BasketballReferenceScraper
    scraper = scraper_type()
    return scraper.scrape()

# Example usage:
# scraper = BasketballReferenceScraper()
# data = scraper.scrape()
# print(data.head())