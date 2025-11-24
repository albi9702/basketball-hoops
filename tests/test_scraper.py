import datetime
import unittest
from unittest.mock import MagicMock, patch, Mock

import pandas as pd
import requests

from src.scraping.basketball_reference_scraper import (
    BasketballReferenceScraper,
    scrape_data,
    FetchError,
    ParseError,
    ScraperError,
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

    def test_scrape_boxscore_tables_streams_to_store(self):
        schedule_df = pd.DataFrame({
            'Date': [datetime.date(2025, 10, 3), datetime.date(2025, 10, 4)],
            'Home': ['Team A', 'Team C'],
            'HomePoints': [85, 90],
            'Visitors': ['Team B', 'Team D'],
            'VisitorsPoints': [80, 88],
            'HasGoneOvertime': ['', 'OT'],
            'Notes': ['Opening night', ''],
            'DateURL': [
                'https://www.basketball-reference.com/boxscores/202410030AAA.html',
                'https://www.basketball-reference.com/boxscores/202410040BBB.html',
            ],
            'Season': ['2024-25', '2024-25'],
            'League': ['EuroLeague', 'EuroLeague'],
            'Schedule URL': [
                'https://www.basketball-reference.com/international/league-a/2024-schedule.html',
                'https://www.basketball-reference.com/international/league-a/2024-schedule.html',
            ],
        })

        mock_store = MagicMock()

        with patch.object(self.scraper, 'fetch_data', return_value=SAMPLE_BOXSCORE_HTML):
            self.scraper.scrape_boxscore_tables(schedule_df, store=mock_store)

        self.assertEqual(mock_store.save_boxscores.call_count, 2)
        first_args, first_kwargs = mock_store.save_boxscores.call_args_list[0]
        second_args, second_kwargs = mock_store.save_boxscores.call_args_list[1]
        self.assertEqual(first_kwargs.get('if_exists'), 'replace')
        self.assertEqual(second_kwargs.get('if_exists'), 'append')
        self.assertGreater(len(first_args[0]), 0)
        self.assertGreater(len(second_args[0]), 0)

    def test_filter_schedule_by_date(self):
        schedule_df = pd.DataFrame({
            'Date': [datetime.date(2025, 10, 3), datetime.date(2025, 10, 4)],
            'Home': ['Team A', 'Team C'],
            'League': ['EuroLeague', 'EuroLeague'],
        })

        filtered = self.scraper.filter_schedule_by_date(
            schedule_df,
            datetime.date(2025, 10, 4),
        )

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]['Home'], 'Team C')

    def test_scrape_boxscore_tables_skips_failed_fetch(self):
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

        mock_store = MagicMock()

        # Use FetchError (a ScraperError subclass) instead of generic Exception
        with patch.object(self.scraper, 'fetch_data', side_effect=FetchError("boom")) as mocked_fetch:
            result_df = self.scraper.scrape_boxscore_tables(schedule_df, store=mock_store)

        self.assertTrue(result_df.empty)
        mocked_fetch.assert_called_once()
        mock_store.save_boxscores.assert_not_called()


class TestFetchWithRetry(unittest.TestCase):
    """Tests for the retry logic in fetch_data."""

    def setUp(self):
        self.scraper = BasketballReferenceScraper(max_retries=3, request_delay=0)

    @patch('time.sleep')
    def test_fetch_data_success_on_first_try(self, mock_sleep):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html>Test</html>"
        self.scraper.session.get = Mock(return_value=mock_response)

        result = self.scraper.fetch_data("http://example.com")

        self.assertEqual(result, "<html>Test</html>")
        self.assertEqual(self.scraper.session.get.call_count, 1)

    @patch('time.sleep')
    def test_fetch_data_retries_on_server_error(self, mock_sleep):
        error_response = Mock()
        error_response.status_code = 500

        success_response = Mock()
        success_response.status_code = 200
        success_response.text = "<html>Success</html>"

        self.scraper.session.get = Mock(side_effect=[error_response, success_response])

        result = self.scraper.fetch_data("http://example.com")

        self.assertEqual(result, "<html>Success</html>")
        self.assertEqual(self.scraper.session.get.call_count, 2)

    @patch('time.sleep')
    def test_fetch_data_raises_after_max_retries(self, mock_sleep):
        error_response = Mock()
        error_response.status_code = 503
        self.scraper.session.get = Mock(return_value=error_response)

        with self.assertRaises(FetchError) as ctx:
            self.scraper.fetch_data("http://example.com")

        self.assertIn("after 3 attempts", str(ctx.exception))
        self.assertEqual(self.scraper.session.get.call_count, 3)

    @patch('time.sleep')
    def test_fetch_data_retries_on_connection_error(self, mock_sleep):
        success_response = Mock()
        success_response.status_code = 200
        success_response.text = "<html>Test</html>"

        self.scraper.session.get = Mock(side_effect=[
            requests.RequestException("Connection failed"),
            success_response,
        ])

        result = self.scraper.fetch_data("http://example.com")

        self.assertEqual(result, "<html>Test</html>")
        self.assertEqual(self.scraper.session.get.call_count, 2)

    @patch('time.sleep')
    def test_fetch_data_retries_on_rate_limit(self, mock_sleep):
        rate_limited = Mock()
        rate_limited.status_code = 429

        success_response = Mock()
        success_response.status_code = 200
        success_response.text = "<html>OK</html>"

        self.scraper.session.get = Mock(side_effect=[rate_limited, rate_limited, success_response])

        result = self.scraper.fetch_data("http://example.com")

        self.assertEqual(result, "<html>OK</html>")
        self.assertEqual(self.scraper.session.get.call_count, 3)

    @patch('time.sleep')
    def test_fetch_data_raises_on_non_retryable_error(self, mock_sleep):
        error_response = Mock()
        error_response.status_code = 404  # Not in RETRY_STATUS_CODES
        self.scraper.session.get = Mock(return_value=error_response)

        with self.assertRaises(FetchError) as ctx:
            self.scraper.fetch_data("http://example.com")

        self.assertIn("HTTP 404", str(ctx.exception))
        # Non-retryable errors should fail immediately
        self.assertEqual(self.scraper.session.get.call_count, 1)


class TestExceptions(unittest.TestCase):
    """Tests for custom exception hierarchy."""

    def test_fetch_error_is_scraper_error(self):
        self.assertTrue(issubclass(FetchError, ScraperError))

    def test_parse_error_is_scraper_error(self):
        self.assertTrue(issubclass(ParseError, ScraperError))

    def test_scraper_error_is_exception(self):
        self.assertTrue(issubclass(ScraperError, Exception))


if __name__ == '__main__':
    unittest.main()