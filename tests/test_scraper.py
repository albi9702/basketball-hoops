import unittest
from unittest.mock import patch

import pandas as pd

from src.scraping.basketball_reference_scraper import (
        BasketballReferenceScraper,
        scrape_data,
)


SAMPLE_TABLE_HTML = """
<html>
    <body>
        <table id="international-years">
            <thead>
                <tr>
                    <th>Season</th>
                    <th>Leagues</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><a href="/international/league-a/2024.html">2024-25</a></td>
                    <td><a href="/international/league-a/">EuroLeague</a></td>
                </tr>
                <tr>
                    <td><a href="/international/league-b/2023.html">2023-24</a></td>
                    <td><a href="/international/league-b/">Liga ACB</a></td>
                </tr>
            </tbody>
        </table>
    </body>
</html>
"""


class TestBasketballReferenceScraper(unittest.TestCase):
        def setUp(self):
                self.scraper = BasketballReferenceScraper()

        def test_parse_data_returns_expected_columns(self):
                df = self.scraper.parse_data(SAMPLE_TABLE_HTML)
                self.assertIsInstance(df, pd.DataFrame)
                self.assertListEqual(
                        list(df.columns),
                        ['Season', 'Season URL', 'League', 'League URL']
                )
                self.assertEqual(df.iloc[0]['Season'], '2024-25')
                self.assertEqual(
                        df.iloc[0]['League URL'],
                        'https://www.basketball-reference.com/international/league-a/'
                )

        @patch('src.scraping.basketball_reference_scraper.BasketballReferenceScraper.fetch_data', return_value=SAMPLE_TABLE_HTML)
        def test_scrape_data_helper(self, mock_fetch):
                df = scrape_data()
                self.assertEqual(len(df), 2)
                mock_fetch.assert_called_once()


if __name__ == '__main__':
        unittest.main()