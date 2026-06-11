from datetime import datetime
import unittest

from buildingscraper.parsing import next_hourly_run, parse_buildings_html


class ParseBuildingsHtmlTests(unittest.TestCase):
    def test_extracts_building_owner_and_classrooms(self):
        html = """
        <table>
          <tr>
            <td><a href="/buildings/123">Fire Station</a></td>
            <td><a href="/users/456">Test Owner</a></td>
            <td>2 classrooms</td>
          </tr>
        </table>
        """

        self.assertEqual(
            parse_buildings_html(html),
            [
                {
                    "building_id": 123,
                    "owner_name": "Test Owner",
                    "building_type": "Fire Station",
                    "classrooms": 2,
                }
            ],
        )

    def test_uses_safe_defaults_for_missing_optional_fields(self):
        html = """
        <table>
          <tr><td><a href="/buildings/123">Police Station</a></td></tr>
        </table>
        """

        building = parse_buildings_html(html)[0]

        self.assertEqual(building["owner_name"], "Unknown")
        self.assertEqual(building["classrooms"], 0)

    def test_ignores_links_outside_table_rows_and_invalid_ids(self):
        html = """
        <a href="/buildings/123">Navigation link</a>
        <table>
          <tr><td><a href="/buildings/not-an-id">Invalid building</a></td></tr>
        </table>
        """

        self.assertEqual(parse_buildings_html(html), [])

    def test_deduplicates_multiple_links_to_the_same_building(self):
        html = """
        <table>
          <tr>
            <td><a href="/buildings/123">Fire Station</a></td>
            <td><a href="/buildings/123/edit">Edit</a></td>
            <td><a href="/users/456">Test Owner</a></td>
          </tr>
        </table>
        """

        buildings = parse_buildings_html(html)

        self.assertEqual(len(buildings), 1)
        self.assertEqual(buildings[0]["building_id"], 123)
        self.assertEqual(buildings[0]["building_type"], "Fire Station")


class NextHourlyRunTests(unittest.TestCase):
    def test_returns_same_hour_before_scheduled_minute(self):
        now = datetime(2026, 6, 11, 10, 30)

        self.assertEqual(next_hourly_run(now), datetime(2026, 6, 11, 10, 45))

    def test_rolls_over_to_next_day_after_2345(self):
        now = datetime(2026, 6, 11, 23, 50)

        self.assertEqual(next_hourly_run(now), datetime(2026, 6, 12, 0, 45))


if __name__ == "__main__":
    unittest.main()
