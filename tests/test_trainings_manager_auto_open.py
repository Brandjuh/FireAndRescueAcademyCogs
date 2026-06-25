import asyncio
import types
from unittest.mock import AsyncMock

from trainings_manager.trainings_manager import (
    AUTO_BUILDING_LIST_PATH,
    AutoTrainingResult,
    BoardTrainingPost,
    BoardTrainingMatch,
    DEVELOPER_PANEL_CHANNEL_ID,
    MEMBER_PANEL_CHANNEL_ID,
    DeveloperTrainingPanelView,
    SubmitButton,
    SummaryView,
    AUTO_ALLIANCE_DURATION_SECONDS,
    TrainingManager,
    TrainingRequest,
    DisciplineAvailability,
    extract_board_training_matches,
    infer_academy_discipline,
    parse_academy_page,
    parse_available_academies,
    parse_available_academies_page,
    parse_missionchief_forms,
    parse_profile_username,
    parse_training_board_page,
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
<tr search_attribute="[AA] Rescue Academy #0001">
  <td><img src="/images/building_rescue_academy.png" alt="Rescue Academy" /></td>
  <td><a href="/buildings/400">[AA] Rescue Academy #0001</a></td>
  <td><a class="btn btn-success" href="/buildings/400">Start a new training course</a></td>
</tr>
</table>
"""


BUILDING_LIST_PAGE_ONE_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0001">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/100">[AA] Fire Academy #0001</a></td>
  <td><a class="btn btn-success" href="/buildings/100">Start a new training course</a></td>
</tr>
</table>
<ul class="pagination">
  <li><a href="/verband/gebauede?page=2">Next →</a></li>
</ul>
"""


BUILDING_LIST_PAGE_TWO_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0003">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/400">[AA] Fire Academy #0003</a></td>
  <td><a class="btn btn-success" href="/buildings/400">Start a new training course</a></td>
</tr>
</table>
"""


BUILDING_LIST_NEW_ACADEMY_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0010">
  <td><img src="/images/building_fireschool.png" alt="Alliance academy" /></td>
  <td><a href="/buildings/500">[AA] Fire Academy #0010</a></td>
  <td><a class="btn btn-default" href="/buildings/500">Start a new training course</a></td>
</tr>
</table>
"""


BUILDING_LIST_ALL_ACADEMIES_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0011">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/600">[AA] Fire Academy #0011</a></td>
  <td>Training courses currently running</td>
</tr>
<tr search_attribute="[AA] Fire Academy #0012">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/700">[AA] Fire Academy #0012</a></td>
  <td>Training courses currently running</td>
</tr>
</table>
"""


TRAINING_BOARD_HTML = """
<script>
  user_id = 88649;
</script>
<ul class="pagination pagination">
  <li><a href="/alliance_threads/5935?page=5">5</a></li>
  <li class="active"><span>6</span></li>
  <li class="next disabled"><span>Next</span></li>
</ul>
<div class="panel panel-default" id="post-on-page-1">
  <div class="panel-body">
    <div class="row">
      <div class="col-md-1">
        <strong><a href="/profile/123456">BoardUser</a></strong>
        <br>
        <span title="June 24, 2026 15:47">June 24, 2026 15:47</span>
      </div>
      <div class="col-md-11">
        <p>Can I get hotshot crew traning and HazMat?</p>
      </div>
    </div>
  </div>
  <div class="panel-footer">
    <a href="/alliance_posts/179134/edit">Edit</a>
  </div>
</div>
<form action="/alliance_posts?alliance_thread_id=5935" id="new_alliance_post" method="post">
  <input name="authenticity_token" type="hidden" value="token-board" />
  <textarea name="alliance_post[content]"></textarea>
</form>
"""


NEW_THREAD_FORM_HTML = """
<form action="/alliance_threads" method="post" id="new_alliance_thread">
  <input name="utf8" type="hidden" value="&#x2713;" />
  <input name="authenticity_token" type="hidden" value="token-thread" />
  <input name="alliance_thread[caption]" type="text" />
  <textarea name="alliance_post[content]">old</textarea>
  <input name="commit" type="submit" value="Save" />
</form>
"""


BUILDING_LIST_IMAGE_MARKERS_HTML = """
<table>
<tr search_attribute="[AA] Coastal Rescue #0001">
  <td><img class="building_marker_image" building_id="4825891" src="/images/building_water_rescue_school.png"></td>
  <td><a href="/buildings/4825891">[AA] Coastal Rescue #0001</a></td>
  <td><a href="/buildings/4825891">Start a new training course</a></td>
</tr>
<tr search_attribute="[AA] Fire Academy #0001">
  <td><img class="building_marker_image" building_id="4842509" src="/images/building_fireschool.png"></td>
  <td><a href="/buildings/4842509">[AA] Fire Academy #0001</a></td>
  <td><a href="/buildings/4842509">Start a new training course</a></td>
</tr>
<tr search_attribute="[AA] Police Academy #0001">
  <td><img class="building_marker_image" building_id="282585" src="/images/policechief_building_polizeischule.png"></td>
  <td><a href="/buildings/282585">[AA] Police Academy #0001</a></td>
  <td><a href="/buildings/282585">Start a new training course</a></td>
</tr>
<tr search_attribute="[AA] Rescue Academy #0001">
  <td><img class="building_marker_image" building_id="1243355" src="/images/building_rettungsschule.png"></td>
  <td><a href="/buildings/1243355">[AA] Rescue Academy #0001</a></td>
  <td><a href="/buildings/1243355">Start a new training course</a></td>
</tr>
</table>
"""


BUILDING_LIST_PAGE_ONE_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0001">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/100">[AA] Fire Academy #0001</a></td>
  <td><a class="btn btn-success" href="/buildings/100">Start a new training course</a></td>
</tr>
</table>
<ul class="pagination">
  <li><a href="/verband/gebauede?page=2">Next →</a></li>
</ul>
"""


BUILDING_LIST_PAGE_TWO_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0003">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/400">[AA] Fire Academy #0003</a></td>
  <td><a class="btn btn-success" href="/buildings/400">Start a new training course</a></td>
</tr>
</table>
"""


BUILDING_LIST_NEW_ACADEMY_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0010">
  <td><img src="/images/building_fireschool.png" alt="Alliance academy" /></td>
  <td><a href="/buildings/500">[AA] Fire Academy #0010</a></td>
  <td><a class="btn btn-default" href="/buildings/500">Start a new training course</a></td>
</tr>
</table>
"""


BUILDING_LIST_ALL_ACADEMIES_HTML = """
<table>
<tr search_attribute="[AA] Fire Academy #0011">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/600">[AA] Fire Academy #0011</a></td>
  <td>Training courses currently running</td>
</tr>
<tr search_attribute="[AA] Fire Academy #0012">
  <td><img src="/images/building_fireschool.png" /></td>
  <td><a href="/buildings/700">[AA] Fire Academy #0012</a></td>
  <td>Training courses currently running</td>
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


def test_parse_training_board_page_extracts_last_page_post_and_reply_form():
    page = parse_training_board_page(TRAINING_BOARD_HTML)

    assert page.last_page == 6
    assert page.current_user_id == "88649"
    assert page.reply_action == "/alliance_posts?alliance_thread_id=5935"
    assert page.reply_token == "token-board"
    assert len(page.posts) == 1
    assert page.posts[0].post_id == 179134
    assert page.posts[0].author_id == "123456"
    assert page.posts[0].author_name == "BoardUser"
    assert page.posts[0].content == "Can I get hotshot crew traning and HazMat?"


def test_extract_board_training_matches_handles_typos_and_multiple_requests():
    matches = extract_board_training_matches("Can I get hotshot crew traning and HazMat?")

    assert {(match.discipline, match.training) for match in matches} == {
        ("Fire", "Hotshot Crew Training"),
        ("Fire", "HazMat"),
    }


def test_parse_missionchief_forms_extracts_thread_form_fields():
    forms = parse_missionchief_forms(NEW_THREAD_FORM_HTML)

    assert len(forms) == 1
    assert forms[0].action == "/alliance_threads"
    assert forms[0].method == "post"
    assert forms[0].fields["authenticity_token"] == "token-thread"
    assert forms[0].fields["alliance_thread[caption]"] == ""
    assert forms[0].fields["alliance_post[content]"] == "old"


def test_build_board_guide_content_lists_availability_and_training_names():
    manager = TrainingManager.__new__(TrainingManager)
    availability = {
        "Fire": DisciplineAvailability(discipline="Fire", available_classrooms=3),
        "Police": DisciplineAvailability(discipline="Police", available_classrooms=2),
    }

    content = manager._build_board_guide_content(availability, None, request_thread_id=5935)

    assert "[b]Training Request Guide[/b]" in content
    assert "https://www.missionchief.com/alliance_threads/5935" in content
    assert "- Fire: 3 classes" in content
    assert "- Police: 2 classes" in content
    assert "[b]Fire training request text[/b]" in content
    assert "Hotshot Crew Training" in content
    assert "Small typos are supported" in content
    assert "Fire & Rescue Academy bot" not in content
    assert "Discord requests support automatic reminders" in content


def test_build_board_guide_contents_splits_sections_and_marks_posts():
    manager = TrainingManager.__new__(TrainingManager)
    availability = {"Fire": DisciplineAvailability(discipline="Fire", available_classrooms=3)}

    contents = manager._build_board_guide_contents(availability, None, request_thread_id=5935)

    assert set(contents) == {"overview", "Fire", "Police", "EMS", "Coastal"}
    assert contents["overview"].startswith("[TM-GUIDE:overview]")
    assert contents["Fire"].startswith("[TM-GUIDE:Fire]")
    assert "Hotshot Crew Training" in contents["Fire"]


def test_training_board_guide_posts_are_not_treated_as_requests():
    manager = TrainingManager.__new__(TrainingManager)
    post = BoardTrainingPost(
        post_id=1,
        author_id="88649",
        author_name="BotUser",
        created_at="June 24, 2026 15:47",
        content="[TM-GUIDE:Fire]\n- Hotshot Crew Training",
    )

    assert manager._is_board_guide_post(post) is True


def test_training_board_reply_reports_failed_auto_open_to_board_user():
    manager = TrainingManager.__new__(TrainingManager)
    post = BoardTrainingPost(
        post_id=179134,
        author_id="123456",
        author_name="BoardUser",
        created_at="June 24, 2026 15:47",
        content="Hotshot Crew Training",
    )
    match = BoardTrainingMatch(
        discipline="Fire",
        training="Hotshot Crew Training",
        days=3,
        matched_text="hotshot crew training",
        score=1.0,
    )
    result = AutoTrainingResult(False, "No available Fire academies found on the alliance building list")

    reply = manager._build_training_board_reply(post, [(match, result)])

    assert "Training request processed for BoardUser." in reply
    assert "Could not open automatically:" in reply
    assert "No free Fire classrooms are available right now" in reply
    assert "Please try again later" in reply


def test_training_board_error_reply_explains_unrecognized_request():
    manager = TrainingManager.__new__(TrainingManager)
    post = BoardTrainingPost(
        post_id=179134,
        author_id="123456",
        author_name="BoardUser",
        created_at="June 24, 2026 15:47",
        content="Can I have something?",
    )

    reply = manager._build_training_board_error_reply(post, "No known training name was found.")

    assert "Training request could not be processed for BoardUser." in reply
    assert "Reason: No known training name was found." in reply


def test_parse_available_academies_extracts_open_training_links():
    academies = parse_available_academies(BUILDING_LIST_HTML)

    assert [(academy.building_id, academy.discipline) for academy in academies] == [
        (100, "Fire"),
        (200, "Fire"),
        (300, "Police"),
        (400, "EMS"),
    ]
    assert [academy.has_start_button for academy in academies] == [True, True, True, True]


def test_parse_available_academies_page_extracts_next_link():
    academies, next_page = parse_available_academies_page(BUILDING_LIST_PAGE_ONE_HTML)

    assert [(academy.building_id, academy.discipline) for academy in academies] == [(100, "Fire")]
    assert next_page == "/verband/gebauede?page=2"


def test_parse_available_academies_detects_new_academy_from_row_and_link_text():
    academies = parse_available_academies(BUILDING_LIST_NEW_ACADEMY_HTML)

    assert [(academy.building_id, academy.discipline) for academy in academies] == [(500, "Fire")]


def test_parse_available_academies_includes_academies_without_start_button():
    academies = parse_available_academies(BUILDING_LIST_ALL_ACADEMIES_HTML)

    assert [(academy.building_id, academy.discipline) for academy in academies] == [
        (600, "Fire"),
        (700, "Fire"),
    ]
    assert [academy.has_start_button for academy in academies] == [False, False]


def test_parse_available_academies_uses_missionchief_school_image_markers():
    academies = parse_available_academies(BUILDING_LIST_IMAGE_MARKERS_HTML)

    assert [(academy.building_id, academy.discipline) for academy in academies] == [
        (4825891, "Coastal"),
        (4842509, "Fire"),
        (282585, "Police"),
        (1243355, "EMS"),
    ]
    assert [academy.has_start_button for academy in academies] == [True, True, True, True]


def test_infer_academy_discipline_from_image_sources():
    assert infer_academy_discipline("/images/building_fireschool.png") == "Fire"
    assert infer_academy_discipline("/images/policechief_building_polizeischule.png") == "Police"
    assert infer_academy_discipline("/images/building_rettungsschule.png") == "EMS"
    assert infer_academy_discipline("/images/building_rescue_school.png") == "EMS"
    assert infer_academy_discipline("/images/building_ems_school.png") == "EMS"
    assert infer_academy_discipline("/images/building_ambulance_school.png") == "EMS"
    assert infer_academy_discipline("/images/water_rescue_school.png") == "Coastal"


def test_parse_profile_username_extracts_heading_name():
    assert parse_profile_username(PROFILE_HTML) == "DutchFireFighter"


def test_auto_open_training_posts_missionchief_education_form():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": ACADEMY_HTML.replace("4951748", "100"),
        }
    )
    manager, guild, user, _ = _manager(session=session, contribution_rate=None)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.academy_id == 100
    assert result.course_value == "hotshot:17"
    assert len(session.posts) == 1
    post_url, kwargs = session.posts[0]
    assert post_url == "https://www.missionchief.com/buildings/100/education"
    assert kwargs["data"]["building_rooms_use"] == "2"
    assert kwargs["data"]["education_select"] == "hotshot:17"
    assert kwargs["data"]["alliance[duration]"] == str(AUTO_ALLIANCE_DURATION_SECONDS)
    assert kwargs["data"]["alliance[cost]"] == "100"


def test_board_training_request_opens_without_discord_member_verification():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": ACADEMY_HTML.replace("4951748", "100"),
        }
    )
    manager, _guild, _user, _ = _manager(session=session, contribution_rate=6.0)
    post = BoardTrainingPost(
        post_id=179134,
        author_id="456",
        author_name="BoardUser",
        created_at="June 24, 2026 15:47",
        content="Hotshot Crew Training",
    )
    req = _training_request(user_id=0, fee_per_day=0, num_classes=1)

    result = asyncio.run(manager._try_auto_open_board_training(post, req))

    assert result.success is True
    assert result.mc_user_id == "456"
    assert result.mc_username == "BoardUser"
    assert result.contribution_rate == 6.0
    post_url, kwargs = session.posts[0]
    assert post_url == "https://www.missionchief.com/buildings/100/education"
    assert kwargs["data"]["alliance[cost]"] == "0"
    assert kwargs["data"]["building_rooms_use"] == "1"


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


def test_auto_open_training_finds_new_academy_on_next_building_list_page():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_PAGE_ONE_HTML,
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}?page=2": BUILDING_LIST_PAGE_TWO_HTML,
            "https://www.missionchief.com/buildings/100": NO_ROOM_ACADEMY_HTML,
            "https://www.missionchief.com/buildings/400": ACADEMY_HTML.replace("4951748", "400"),
        }
    )
    manager, guild, user, _ = _manager(session=session, contribution_rate=None)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.academy_id == 400
    assert f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}?page=2" in session.get_urls
    post_url, kwargs = session.posts[0]
    assert post_url == "https://www.missionchief.com/buildings/400/education"
    assert kwargs["data"]["building_rooms_use"] == "2"


def test_auto_open_training_uses_newly_detected_academy_on_current_page():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_NEW_ACADEMY_HTML,
            "https://www.missionchief.com/buildings/500": ACADEMY_HTML.replace("4951748", "500"),
        }
    )
    manager, guild, user, _ = _manager(session=session, contribution_rate=None)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is True
    assert result.academy_id == 500
    post_url, kwargs = session.posts[0]
    assert post_url == "https://www.missionchief.com/buildings/500/education"
    assert kwargs["data"]["building_rooms_use"] == "2"


def test_auto_open_training_skips_academies_without_start_button_on_list():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_ALL_ACADEMIES_HTML,
            "https://www.missionchief.com/buildings/600": NO_ROOM_ACADEMY_HTML,
            "https://www.missionchief.com/buildings/700": ACADEMY_HTML.replace("4951748", "700"),
        }
    )
    manager, guild, user, _ = _manager(session=session, contribution_rate=None)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is False
    assert "No available Fire academies found on the alliance building list" in result.reason
    assert "https://www.missionchief.com/buildings/600" not in session.get_urls
    assert "https://www.missionchief.com/buildings/700" not in session.get_urls
    assert session.posts == []


def test_collect_training_availability_counts_classrooms_by_discipline():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": NO_ROOM_ACADEMY_HTML,
            "https://www.missionchief.com/buildings/200": ACADEMY_HTML.replace("4951748", "200"),
            "https://www.missionchief.com/buildings/300": ACADEMY_HTML.replace("4951748", "300"),
            "https://www.missionchief.com/buildings/400": ACADEMY_HTML.replace("4951748", "400"),
        }
    )
    manager, _guild, _user, _ = _manager(session=session, contribution_rate=None)

    availability, error = asyncio.run(manager._collect_training_availability())

    assert error is None
    assert availability["Fire"].academies_checked == 2
    assert availability["Fire"].academies_available == 2
    assert availability["Fire"].available_classrooms == 5
    assert availability["Police"].academies_checked == 1
    assert availability["Police"].available_classrooms == 4
    assert availability["EMS"].academies_checked == 1
    assert availability["EMS"].available_classrooms == 4


def test_training_availability_embed_uses_simple_class_counts():
    manager, _guild, _user, _ = _manager(session=_Session(ACADEMY_HTML), contribution_rate=None)
    availability = {
        "Fire": DisciplineAvailability(discipline="Fire", available_classrooms=5, academies_checked=2),
        "Police": DisciplineAvailability(discipline="Police", available_classrooms=4, academies_checked=1),
        "EMS": DisciplineAvailability(discipline="EMS", available_classrooms=0, academies_checked=0),
        "Coastal": DisciplineAvailability(discipline="Coastal", available_classrooms=1, academies_checked=1),
    }

    embed = manager._build_availability_embed(availability)

    assert embed.kwargs["title"] == "Academy Availability"
    assert embed.kwargs["description"] == "\n".join(
        [
            "**Fire:** 5 classes",
            "**Police:** 4 classes",
            "**EMS:** 0 classes",
            "**Coastal:** 1 classes",
        ]
    )
    assert not embed.fields


def test_auto_open_training_falls_back_when_known_tax_is_below_threshold():
    session = _Session(ACADEMY_HTML)
    manager, guild, user, _ = _manager(session=session, contribution_rate=4.9)
    req = _training_request()

    result = asyncio.run(manager._try_auto_open_training(guild, user, req))

    assert result.success is False
    assert "below 5.0%" in result.reason
    assert session.posts == []


def test_auto_open_training_resolves_missing_member_name_from_members_scraper():
    session = _Session(
        {
            f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}": BUILDING_LIST_HTML,
            "https://www.missionchief.com/buildings/100": ACADEMY_HTML.replace("4951748", "100"),
        }
    )
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


def test_member_panel_auto_repost_uses_member_channel_and_updates_config():
    sent_messages = []
    channel = types.SimpleNamespace(
        id=MEMBER_PANEL_CHANNEL_ID,
        send=AsyncMock(side_effect=lambda **kwargs: sent_messages.append(kwargs) or types.SimpleNamespace(id=987)),
    )
    guild = types.SimpleNamespace(id=1, get_channel=lambda channel_id: channel if channel_id == MEMBER_PANEL_CHANNEL_ID else None)
    panel_id_set = AsyncMock()
    request_channel_set = AsyncMock()
    last_auto_post_set = AsyncMock()
    manager = TrainingManager.__new__(TrainingManager)
    manager.config = types.SimpleNamespace(
        guild=lambda guild: types.SimpleNamespace(
            all=AsyncMock(
                return_value={
                    "request_channel_id": 123,
                    "panel_message_id": 456,
                    "panel_last_auto_post_at": None,
                    "button_message": None,
                }
            ),
            request_channel_id=types.SimpleNamespace(set=request_channel_set),
            panel_message_id=types.SimpleNamespace(set=panel_id_set),
            panel_last_auto_post_at=types.SimpleNamespace(set=last_auto_post_set),
            button_message=AsyncMock(return_value=None),
        )
    )

    asyncio.run(manager._ensure_member_panel_for_guild(guild))

    request_channel_set.assert_awaited_once_with(MEMBER_PANEL_CHANNEL_ID)
    channel.send.assert_awaited_once()
    panel_id_set.assert_awaited_once_with(987)
    last_auto_post_set.assert_awaited_once()
