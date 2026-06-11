from datetime import datetime
import re
import unittest

from bs4 import BeautifulSoup

from buildingscraper.parsing import next_hourly_run, parse_buildings_html


def legacy_parse_buildings_html(html):
    """Copy of the parser behavior that existed before it was extracted."""
    soup = BeautifulSoup(html, "html.parser")
    buildings = []

    for link in soup.find_all("a", href=lambda value: value and "/buildings/" in str(value)):
        match = re.search(r"/buildings/(\d+)", link["href"])
        if not match:
            continue

        row = link.find_parent("tr")
        if not row:
            continue

        owner_name = "Unknown"
        classrooms = 0

        for column in row.find_all("td"):
            owner_link = column.find("a", href=lambda value: value and "/users/" in str(value))
            if owner_link:
                owner_name = owner_link.get_text(strip=True)

            classroom_match = re.search(
                r"(\d+)\s*classroom",
                column.get_text(strip=True),
                re.IGNORECASE,
            )
            if classroom_match:
                classrooms = int(classroom_match.group(1))

        buildings.append(
            {
                "building_id": int(match.group(1)),
                "owner_name": owner_name,
                "building_type": link.get_text(strip=True),
                "classrooms": classrooms,
            }
        )

    return buildings


class ParseBuildingsHtmlTests(unittest.TestCase):
    def test_matches_legacy_parser_behavior(self):
        html_samples = [
            """
            <table>
              <tr>
                <td><a href="/buildings/123">Fire Station</a></td>
                <td><a href="/users/456">Test Owner</a></td>
                <td>2 classrooms</td>
              </tr>
            </table>
            """,
            """
            <a href="/buildings/123">Navigation link</a>
            <table>
              <tr>
                <td><a href="/buildings/123">Fire Station</a></td>
                <td><a href="/buildings/123/edit">Edit</a></td>
              </tr>
              <tr><td><a href="/buildings/not-an-id">Invalid</a></td></tr>
            </table>
            """,
            "<html><body>No buildings</body></html>",
        ]

        for html in html_samples:
            with self.subTest(html=html):
                self.assertEqual(parse_buildings_html(html), legacy_parse_buildings_html(html))

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

    def test_preserves_existing_multiple_link_behavior(self):
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

        self.assertEqual(len(buildings), 2)
        self.assertEqual(buildings[0]["building_id"], 123)
        self.assertEqual(buildings[0]["building_type"], "Fire Station")
        self.assertEqual(buildings[1]["building_type"], "Edit")


class NextHourlyRunTests(unittest.TestCase):
    def test_returns_same_hour_before_scheduled_minute(self):
        now = datetime(2026, 6, 11, 10, 30)

        self.assertEqual(next_hourly_run(now), datetime(2026, 6, 11, 10, 45))

    def test_rolls_over_to_next_day_after_2345(self):
        now = datetime(2026, 6, 11, 23, 50)

        self.assertEqual(next_hourly_run(now), datetime(2026, 6, 12, 0, 45))


if __name__ == "__main__":
    unittest.main()
