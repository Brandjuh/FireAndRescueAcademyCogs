import asyncio
import types
from unittest.mock import AsyncMock

from trainings_manager.trainings_manager import (
    AUTO_ALLIANCE_DURATION_SECONDS,
    TrainingManager,
    TrainingRequest,
    parse_academy_page,
)


ACADEMY_HTML = """
<html>
<head><meta content="csrf-token" name="csrf-param" />
<meta content="token-123" name="csrf-token" /></head>
<body>
<form action="/buildings/4951748/education" method="post">
<input name="authenticity_token" type="hidden" value="token-123" />
<select id="building_rooms_use" name="building_rooms_use">
  <option value="1">1</option>
  <option value="2">2</option>
  <option value="3">3</option>
  <option value="4">4</option>
</select>
<select id="education_select" name="education_select">
  <option value="">Select an education</option>
  <option value="hotshot:17">Hotshot Crew Training (3 days)</option>
  <option value="truck_drivers_license:7">Truck Driver's License (2 days)</option>
</select>
<select id="alliance_duration" name="alliance[duration]">
  <option value="3600">1 hour</option>
  <option value="43200">12 hours</option>
</select>
<select id="alliance_cost" name="alliance[cost]">
  <option value="0">0 Credits</option>
  <option value="100">100 Credits</option>
  <option value="200">200 Credits</option>
</select>
</form>
</body>
</html>
"""


class _Response:
    def __init__(self, html="", status=200):
        self.html = html
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def text(self):
        return self.html


class _Session:
    def __init__(self, html):
        self.html = html
        self.posts = []

    def get(self, url, **kwargs):
        self.get_url = url
        self.get_kwargs = kwargs
        return _Response(self.html)

    def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return _Response("<html>ok</html>")


def _training_request(**overrides):
    data = {
        "user_id": 123,
        "discipline": "Fire",
        "training": "Hotshot Crew Training",
        "days": 3,
        "fee_per_day": 100,
        "num_classes": 2,
        "references": [],
        "want_reminder": False,
        "request_channel_id": 999,
    }
    data.update(overrides)
    return TrainingRequest(**data)


def _manager(*, session=None, contribution_rate=None):
    role = object()
    user = types.SimpleNamespace(id=123, roles=[role])
    guild = types.SimpleNamespace(
        id=1,
        get_member=lambda user_id: user if user_id == 123 else None,
        get_role=lambda role_id: role if role_id == 555 else None,
    )
    membersync = types.SimpleNamespace(
        get_link_for_discord=AsyncMock(
            return_value={"mc_user_id": "456", "mc_username": "MCUser"}
        ),
        config=types.SimpleNamespace(verified_role_id=AsyncMock(return_value=555)),
    )
    if contribution_rate is None:
        members_scraper = types.SimpleNamespace(get_member_snapshot=AsyncMock(return_value=None))
    else:
        members_scraper = types.SimpleNamespace(
            get_member_snapshot=AsyncMock(return_value={"contribution_rate": contribution_rate})
        )
    cookie_manager = types.SimpleNamespace(get_session=AsyncMock(return_value=session or _Session(ACADEMY_HTML)))
    bot = types.SimpleNamespace(
        get_cog=lambda name: {
            "MemberSync": membersync,
            "MembersScraper": members_scraper,
            "CookieManager": cookie_manager,
        }.get(name)
    )
    manager = TrainingManager.__new__(TrainingManager)
    manager.bot = bot
    return manager, guild, user, session or cookie_manager.get_session.return_value


def test_parse_academy_page_extracts_form_rooms_costs_and_courses():
    page = parse_academy_page(ACADEMY_HTML)

    assert page.action == "/buildings/4951748/education"
    assert page.authenticity_token == "token-123"
    assert page.available_rooms == 4
    assert page.costs == [0, 100, 200]
    assert [course.label for course in page.courses] == [
        "Hotshot Crew Training (3 days)",
        "Truck Driver's License (2 days)",
    ]


def test_auto_open_training_posts_missionchief_education_form():
    session = _Session(ACADEMY_HTML)
    manager, guild, user, _ = _manager(session=session, contribution_rate=None)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.academy_id == 4951748
    assert result.course_value == "hotshot:17"
    assert len(session.posts) == 1
    post_url, kwargs = session.posts[0]
    assert post_url == "https://www.missionchief.com/buildings/4951748/education"
    assert kwargs["data"]["building_rooms_use"] == "2"
    assert kwargs["data"]["education_select"] == "hotshot:17"
    assert kwargs["data"]["alliance[duration]"] == str(AUTO_ALLIANCE_DURATION_SECONDS)
    assert kwargs["data"]["alliance[cost]"] == "100"


def test_auto_open_training_falls_back_when_known_tax_is_below_threshold():
    session = _Session(ACADEMY_HTML)
    manager, guild, user, _ = _manager(session=session, contribution_rate=4.9)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is False
    assert "below 5.0%" in result.reason
    assert session.posts == []
