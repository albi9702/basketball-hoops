import datetime
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
                <tr>
                    <th>Date</th>
                    <th>Team</th>
                    <th>PTS</th>
                    <th>Opp</th>
                    <th>PTS.1</th>
                    <th>OT</th>
                    <th>Notes</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><a href="/boxscores/202410030AAA.html">Fri, Oct 3, 2025</a></td>
                    <td>Team A</td>
                    <td>85</td>
                    <td>Team B</td>
                    <td>80</td>
                    <td></td>
                    <td>Opening night</td>
                </tr>
                <tr>
                    <td>October</td>
                    <td>Team C</td>
                    <td>90</td>
                    <td>Team D</td>
                    <td>88</td>
                    <td>OT</td>
                    <td></td>
                </tr>
            </tbody>
        </table>
    </body>
</html>
"""

SAMPLE_BOXSCORE_HTML = """
<html>
    <body>
        <div id="div_box-score-home">
            <!--
            <table>
                <thead>
                    <tr><th>Player</th><th>PTS</th></tr>
                </thead>
                <tbody>
                    <tr><td>Home Player</td><td>20</td></tr>
                </tbody>
            </table>
            -->
        </div>
        <div id="div_box-score-visitor">
            <!--
            <table>
                <thead>
                    <tr><th>Player</th><th>PTS</th></tr>
                </thead>
                <tbody>
                    <tr><td>Visitor Player</td><td>18</td></tr>
                </tbody>
            </table>
            -->
        </div>
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

        expected_columns = [
            'Date', 'Home', 'HomePoints', 'Visitors', 'VisitorsPoints',
            'HasGoneOvertime', 'Notes', 'DateURL', 'Season', 'League', 'Schedule URL'
        ]
        self.assertListEqual(list(schedules_df.columns), expected_columns)
        self.assertEqual(len(schedules_df), len(df))
        self.assertTrue(schedules_df['Schedule URL'].str.endswith('-schedule.html').all())
        self.assertTrue(schedules_df['DateURL'].str.contains('boxscores').all())
        self.assertTrue(all(isinstance(d, datetime.date) for d in schedules_df['Date']))
        self.assertGreaterEqual(mocked_fetch.call_count, len(df))

    def test_scrape_boxscore_tables(self):
        schedule_df = pd.DataFrame({
            'Date': [datetime.date(2025, 10, 3)],
            'Home': ['Team A'],
            'HomePoints': [85],
            'Visitors': ['Team B'],
            'VisitorsPoints': [80],
            'HasGoneOvertime': [''],
            'Notes': ['Opening night'],
            'DateURL': ['https://www.basketball-reference.com/boxscores/202410030AAA.html'],
            'Season': ['2024-25'],
            'League': ['EuroLeague'],
            'Schedule URL': ['https://www.basketball-reference.com/international/league-a/2024-schedule.html']
        })

        with patch.object(self.scraper, 'fetch_data', return_value=SAMPLE_BOXSCORE_HTML) as mocked_fetch:
            boxscore_df = self.scraper.scrape_boxscore_tables(schedule_df)

        self.assertEqual(len(boxscore_df), 2)
        self.assertSetEqual(set(boxscore_df['TeamRole']), {'Home', 'Visitors'})
        self.assertSetEqual(set(boxscore_df['Team']), {'Team A', 'Team B'})
        self.assertIn('Player', boxscore_df.columns)
        visitors_row = boxscore_df.loc[boxscore_df['TeamRole'] == 'Visitors'].iloc[0]
        home_row = boxscore_df.loc[boxscore_df['TeamRole'] == 'Home'].iloc[0]
        self.assertEqual(visitors_row['Team'], 'Team B')
        self.assertEqual(home_row['Team'], 'Team A')
        self.assertEqual(visitors_row['PTS'], 18)
        self.assertEqual(home_row['PTS'], 20)
        mocked_fetch.assert_called_once_with('https://www.basketball-reference.com/boxscores/202410030AAA.html')


if __name__ == '__main__':
    unittest.main()