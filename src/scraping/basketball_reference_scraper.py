from io import StringIO
from typing import Optional, Type
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

    def parse_data(self, html: str) -> pd.DataFrame:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find('table')
        if not table:
            raise Exception("No table found on the page.")

        table_html = str(table)
        full_table_df = pd.read_html(StringIO(table_html))[0]
        link_table_df = None
        try:
            link_table_df = pd.read_html(StringIO(table_html), extract_links='body')[0]
        except (TypeError, ValueError):
            # Older pandas versions do not support extract_links; fallback handled below.
            pass

        required_columns = {'Season', 'Leagues'}
        if not required_columns.issubset(full_table_df.columns):
            raise Exception("Expected Season and Leagues columns were not found.")

        seasons = full_table_df['Season'].astype(str)
        leagues = full_table_df['Leagues'].astype(str)

        if link_table_df is not None:
            season_urls = link_table_df['Season'].map(self._extract_href)
            league_urls = link_table_df['Leagues'].map(self._extract_href)
        else:
            season_urls = pd.Series([None] * len(seasons))
            league_urls = pd.Series([None] * len(leagues))

        df = pd.DataFrame({
            'Season': seasons,
            'Season URL': season_urls.map(lambda href: urljoin(self.base_url, href) if href else None),
            'League': leagues,
            'League URL': league_urls.map(lambda href: urljoin(self.base_url, href) if href else None)
        })

        return df

    def scrape(self):
        html = self.fetch_data()
        data = self.parse_data(html)
        return data


def scrape_data(scraper_cls: Optional[Type[BasketballReferenceScraper]] = None) -> pd.DataFrame:
    """Convenience helper mirroring the previous module-level API."""

    scraper_type = scraper_cls or BasketballReferenceScraper
    scraper = scraper_type()
    return scraper.scrape()

# Example usage:
# scraper = BasketballReferenceScraper()
# data = scraper.scrape()
# print(data.head())