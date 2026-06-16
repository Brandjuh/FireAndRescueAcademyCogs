import asyncio
import types
from unittest.mock import AsyncMock

from trainings_manager.trainings_manager import (
    AUTO_BUILDING_LIST_PATH,
    AutoTrainingResult,
    DEVELOPER_PANEL_CHANNEL_ID,
    MEMBER_PANEL_CHANNEL_ID,
    DeveloperTrainingPanelView,
    SubmitButton,
    SummaryView,
    AUTO_ALLIANCE_DURATION_SECONDS,
    TrainingManager,
    TrainingRequest,
    parse_academy_page,
    parse_available_academies,
    parse_profile_username,
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


PROFILE_HTML = """
<html>
<head><title>DutchFireFighter - MISSIONCHIEF.COM</title></head>
<body><h1>DutchFireFighter</h1></body>
</html>
"""


BUILDING_LIST_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0001">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/100">[AA] Fire Academy #0001</a></td>
  <td><a class="btn btn-success" href="/buildings/100">Start a new training course</a></td>
</tr>
<tr search_attribute="[AA] Fire Academy #0002">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/200">[AA] Fire Academy #0002</a></td>
  <td><a class="btn btn-success" href="/buildings/200">Start a new training course</a></td>
</tr>
<tr search_attribute="[AA] Police Academy #0001">
  <td><img src="/images/policechief_building_polizeischule.png" /></td>
  <td><a href="/buildings/300">[AA] Police Academy #0001</a></td>
  <td><a class="btn btn-success" href="/buildings/300">Start a new training course</a></td>
</tr>
</table>
"""


NO_ROOM_ACADEMY_HTML = ACADEMY_HTML.replace(
    """
  <option value="2">2</option>
  <option value="3">3</option>
  <option value="4">4</option>
""",
    "",
)


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
        self.get_urls = []

    def get(self, url, **kwargs):
        self.get_urls.append(url)
        self.get_url = url
        self.get_kwargs = kwargs
        html = self.html.get(url, "") if isinstance(self.html, dict) else self.html
        return _Response(html)

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


def _manager(*, session=None, contribution_rate=None, member_link=None, member_snapshot=None):
    role = object()
    user = types.SimpleNamespace(id=123, roles=[role])
    guild = types.SimpleNamespace(
        id=1,
        get_member=lambda user_id: user if user_id == 123 else None,
        get_role=lambda role_id: role if role_id == 555 else None,
    )
    membersync = types.SimpleNamespace(
        get_link_for_discord=AsyncMock(
            return_value=member_link or {"mc_user_id": "456", "mc_username": "MCUser"}
        ),
        config=types.SimpleNamespace(verified_role_id=AsyncMock(return_value=555)),
    )
    if member_snapshot is not None:
        members_scraper = types.SimpleNamespace(get_member_snapshot=AsyncMock(return_value=member_snapshot))
    elif contribution_rate is None:
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


def test_parse_available_academies_extracts_open_training_links():
    academies = parse_available_academies(BUILDING_LIST_HTML)

    assert [(academy.building_id, academy.discipline) for academy in academies] == [
        (100, "Fire"),
        (200, "Fire"),
        (300, "Police"),
    ]


def test_parse_profile_username_extracts_heading_name():
    assert parse_profile_username(PROFILE_HTML) == "DutchFireFighter"


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


def test_auto_open_training_finds_dynamic_academy_with_available_rooms():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": NO_ROOM_ACADEMY_HTML,
            "https://www.missionchief.com/buildings/200": ACADEMY_HTML.replace("4951748", "200"),
        }
    )
    manager, guild, user, _ = _manager(session=session, contribution_rate=None)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.academy_id == 200
    assert "https://www.missionchief.com/buildings/100" in session.get_urls
    assert "https://www.missionchief.com/buildings/200" in session.get_urls
    post_url, kwargs = session.posts[0]
    assert post_url == "https://www.missionchief.com/buildings/200/education"
    assert kwargs["data"]["building_rooms_use"] == "2"


def test_auto_open_training_falls_back_when_known_tax_is_below_threshold():
    session = _Session(ACADEMY_HTML)
    manager, guild, user, _ = _manager(session=session, contribution_rate=4.9)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is False
    assert "below 5.0%" in result.reason
    assert session.posts == []


def test_auto_open_training_resolves_missing_member_name_from_members_scraper():
    session = _Session(ACADEMY_HTML)
    manager, guild, user, _ = _manager(
        session=session,
        member_link={"mc_user_id": "456"},
        member_snapshot={"name": "SnapshotMCUser", "contribution_rate": 6.0},
    )
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.mc_user_id == "456"
    assert result.mc_username == "SnapshotMCUser"


def test_auto_open_training_resolves_member_name_from_profile_when_scraper_has_no_name():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": ACADEMY_HTML.replace("4951748", "100"),
            "https://www.missionchief.com/profile/456": PROFILE_HTML,
        }
    )
    manager, guild, user, _ = _manager(
        session=session,
        member_link={"mc_user_id": "456"},
        member_snapshot={"contribution_rate": 6.0},
    )
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.mc_username == "DutchFireFighter"


def test_auto_open_training_treats_unknown_member_name_as_missing():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": ACADEMY_HTML.replace("4951748", "100"),
            "https://www.missionchief.com/profile/456": PROFILE_HTML,
        }
    )
    manager, guild, user, _ = _manager(
        session=session,
        member_link={"mc_user_id": "456", "mc_username": "Unknown"},
        member_snapshot={"contribution_rate": 6.0},
    )
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.mc_username == "DutchFireFighter"


def test_normal_submit_button_falls_back_to_admin_when_auto_open_fails():
    user = types.SimpleNamespace(id=123, mention="<@123>", send=AsyncMock())
    request_channel = types.SimpleNamespace(id=10, mention="#requests", send=AsyncMock())
    admin_channel = types.SimpleNamespace(id=11, send=AsyncMock())
    log_channel = types.SimpleNamespace(id=12, send=AsyncMock())
    guild = types.SimpleNamespace(
        get_channel=lambda channel_id: {
            10: request_channel,
            11: admin_channel,
            12: log_channel,
        }.get(channel_id),
    )
    cog = TrainingManager.__new__(TrainingManager)
    cog.config = types.SimpleNamespace(
        guild=lambda guild: types.SimpleNamespace(
            all=AsyncMock(
                return_value={
                    "request_channel_id": 10,
                    "admin_channel_id": 11,
                    "log_channel_id": 12,
                }
            )
        )
    )
    cog._try_auto_open_training = AsyncMock(return_value=AutoTrainingResult(False, "No academy available"))
    parent = SummaryView(cog, user.id, "Fire", "Hotshot Crew Training", 3, 100, 2, [])
    button = SubmitButton(parent)
    response_state = {"done": False}

    async def edit_response(*args, **kwargs):
        response_state["done"] = True

    interaction = types.SimpleNamespace(
        guild=guild,
        user=user,
        response=types.SimpleNamespace(
            send_message=AsyncMock(),
            is_done=lambda: response_state["done"],
            edit_message=AsyncMock(side_effect=edit_response),
        ),
        followup=types.SimpleNamespace(send=AsyncMock()),
        message=types.SimpleNamespace(edit=AsyncMock()),
    )

    asyncio.run(button.callback(interaction))

    cog._try_auto_open_training.assert_awaited_once()
    admin_channel.send.assert_awaited_once()
    log_channel.send.assert_awaited_once()
    user.send.assert_awaited_once()
    admin_embed = admin_channel.send.await_args.kwargs["embed"]
    admin_fields = {field["name"]: field["value"] for field in admin_embed.fields}
    assert "Automatic opening" in admin_fields
    assert "No academy available" in admin_fields["Automatic opening"]
    assert "No academy available" in interaction.followup.send.await_args.args[0]
    assert all(child.disabled for child in parent.children)
    assert button.label == "Processing..."
    interaction.response.edit_message.assert_awaited_once()

    second_interaction = types.SimpleNamespace(
        guild=guild,
        user=user,
        response=types.SimpleNamespace(send_message=AsyncMock()),
    )
    asyncio.run(button.callback(second_interaction))

    cog._try_auto_open_training.assert_awaited_once()
    second_interaction.response.send_message.assert_awaited_once()
    assert "already being processed" in second_interaction.response.send_message.await_args.args[0]


def test_developer_panel_uses_configured_test_channel():
    view = DeveloperTrainingPanelView(TrainingManager.__new__(TrainingManager))

    assert DEVELOPER_PANEL_CHANNEL_ID == 1421242306136113254
    assert hasattr(view, "open_test_training")


def test_member_panel_uses_configured_member_channel():
    assert MEMBER_PANEL_CHANNEL_ID == 1421627971831070730
