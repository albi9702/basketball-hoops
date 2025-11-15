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
                    <th></th>
                    <th>Details</th>
                </tr>
                <tr>
                    <th>Season</th>
                    <th>League</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><a href="/international/league-a/2024.html">2024-25</a></td>
                    <td><a href="/international/league-a/">EuroLeague</a></td>
                </tr>
                <tr>
                    <td><a href="/international/league-b/2023.html">2023-24</a></td>
                    <td><a href="/international/league-b/2023.html">Liga ACB</a></td>
                </tr>
                <tr>
                    <td><a href="/international/league-c/2022.html">2022-23</a></td>
                    <td></td>
                </tr>
            </tbody>
        </table>
    </body>
</html>
"""

SAMPLE_SCHEDULE_HTML = """
<html>
    <body>
        <table id="schedule">
            <thead>
                <tr><th>Date</th><th>Home</th><th>Away</th></tr>
            </thead>
            <tbody>
                <tr><td>2024-10-01</td><td>Team A</td><td>Team B</td></tr>
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
            ['Season', 'Season URL', 'League', 'League URL', 'Schedule URL']
        )
        self.assertEqual(len(df), 2, "Rows missing League should be dropped")
        self.assertEqual(df.iloc[0]['Season'], '2024-25')
        self.assertEqual(
            df.iloc[0]['Schedule URL'],
            'https://www.basketball-reference.com/international/league-a/2024-schedule.html'
        )
        self.assertEqual(
            df.iloc[1]['Schedule URL'],
            'https://www.basketball-reference.com/international/league-b/2023-schedule.html'
        )
        self.assertEqual(
            df.iloc[0]['League URL'],
            'https://www.basketball-reference.com/international/league-a/'
        )

    @patch('src.scraping.basketball_reference_scraper.BasketballReferenceScraper.fetch_data', return_value=SAMPLE_TABLE_HTML)
    def test_scrape_data_helper(self, mock_fetch):
        df = scrape_data()
        self.assertEqual(len(df), 2)
        mock_fetch.assert_called_once()

    def test_scrape_league_schedules(self):
        df = self.scraper.parse_data(SAMPLE_TABLE_HTML)

        def fake_fetch(url=None):
            return SAMPLE_SCHEDULE_HTML if url and url.endswith('-schedule.html') else SAMPLE_TABLE_HTML

        with patch.object(self.scraper, 'fetch_data', side_effect=fake_fetch) as mocked_fetch:
            schedules_df = self.scraper.scrape_league_schedules(df)

        self.assertEqual(len(schedules_df), len(df))
        self.assertIn('Season', schedules_df.columns)
        self.assertIn('League', schedules_df.columns)
        self.assertTrue((schedules_df['Schedule URL'].str.endswith('-schedule.html')).all())
        self.assertGreaterEqual(mocked_fetch.call_count, len(df))


if __name__ == '__main__':
    unittest.main()