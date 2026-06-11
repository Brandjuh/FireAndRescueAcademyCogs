import re
from datetime import datetime, timedelta

from bs4 import BeautifulSoup


def parse_buildings_html(html: str) -> list[dict]:
    """Extract alliance buildings from a MissionChief buildings page."""
    soup = BeautifulSoup(html, "html.parser")
    buildings = {}

    for link in soup.find_all("a", href=lambda value: value and "/buildings/" in str(value)):
        match = re.search(r"/buildings/(\d+)", link["href"])
        row = link.find_parent("tr")
        if not match or not row:
            continue

        building_id = int(match.group(1))
        if building_id in buildings:
            continue

        owner_name = "Unknown"
        classrooms = 0

        for column in row.find_all("td"):
            owner_link = column.find("a", href=lambda value: value and "/users/" in str(value))
            if owner_link:
                owner_name = owner_link.get_text(strip=True)

            classroom_match = re.search(
                r"(\d+)\s*classrooms?\b",
                column.get_text(" ", strip=True),
                re.IGNORECASE,
            )
            if classroom_match:
                classrooms = int(classroom_match.group(1))

        buildings[building_id] = {
            "building_id": building_id,
            "owner_name": owner_name,
            "building_type": link.get_text(strip=True),
            "classrooms": classrooms,
        }

    return list(buildings.values())


def next_hourly_run(now: datetime, minute: int = 45) -> datetime:
    """Return the next scheduled hourly run, including across midnight."""
    next_run = now.replace(minute=minute, second=0, microsecond=0)
    if next_run <= now:
        next_run += timedelta(hours=1)
    return next_run
