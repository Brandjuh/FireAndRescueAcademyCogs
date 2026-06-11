import unittest
from datetime import datetime

from logscraper.logs_scraper import LogsScraper


class LogsScraperScheduleTests(unittest.TestCase):
    def test_schedules_current_hour_before_quarter_past(self):
        now = datetime(2026, 6, 12, 10, 5, 30)

        self.assertEqual(
            LogsScraper._next_scrape_time(now),
            datetime(2026, 6, 12, 10, 15),
        )

    def test_schedules_next_hour_at_quarter_past(self):
        now = datetime(2026, 6, 12, 10, 15)

        self.assertEqual(
            LogsScraper._next_scrape_time(now),
            datetime(2026, 6, 12, 11, 15),
        )

    def test_rolls_over_to_next_day_after_2315(self):
        now = datetime(2026, 6, 12, 23, 30)

        next_run = LogsScraper._next_scrape_time(now)

        self.assertEqual(next_run, datetime(2026, 6, 13, 0, 15))
        self.assertGreater((next_run - now).total_seconds(), 0)


if __name__ == "__main__":
    unittest.main()
