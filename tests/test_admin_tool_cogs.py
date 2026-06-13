from datetime import datetime, timezone

import pytest

from admintimednotifications.admintimednotifications import (
    next_run,
    parse_title_body as parse_timer_title_body,
    split_due_reminders,
)
from announcer.announcer import parse_title_body as parse_announcement_title_body
from announcementpanel.announcementpanel import normalize_button_key, parse_label_message
from botstatus.botstatus import clean_activity_type
from rolebasedcredits.rolebasedcredits import (
    CREDIT_RANKS,
    DEFAULT_GUILD,
    DEFAULT_RANK_ROLE_IDS,
    find_rank,
    is_promotion,
    rank_for_credits,
    should_announce_rank_change,
)


def test_announcer_parses_title_and_body():
    assert parse_announcement_title_body("Update | New command added") == (
        "Update",
        "New command added",
    )
    assert parse_announcement_title_body("Plain message") == ("Announcement", "Plain message")


def test_panel_button_key_and_message_parsing():
    assert normalize_button_key("Double Credits!") == "doublecredits"
    assert parse_label_message("Double Credits | Credits are doubled for 24 hours") == (
        "Double Credits",
        "Credits are doubled for 24 hours",
    )


def test_admin_timer_due_split_and_next_run():
    now = datetime(2026, 6, 13, 12, 0, tzinfo=timezone.utc)

    assert next_run(15, now=now) == 1781352900
    assert parse_timer_title_body("Training | Start new training") == (
        "Training",
        "Start new training",
    )

    due, pending = split_due_reminders(
        [
            {"id": 1, "next_run": 100},
            {"id": 2, "next_run": 101},
            {"id": 3, "next_run": 200},
        ],
        now_ts=101,
    )

    assert [item["id"] for item in due] == [1, 2]
    assert [item["id"] for item in pending] == [3]


def test_botstatus_accepts_only_supported_activity_types():
    assert clean_activity_type("Watching") == "watching"
    with pytest.raises(ValueError):
        clean_activity_type("sleeping")


def test_credit_rank_table_matches_requested_thresholds():
    assert [(rank.name, rank.min_credits) for rank in CREDIT_RANKS] == [
        ("Probie", 0),
        ("Firefighter", 200),
        ("Senior Firefighter", 10_000),
        ("Fire Apparatus Operator", 100_000),
        ("Lieutenant", 1_000_000),
        ("Captain", 5_000_000),
        ("Staff Captain", 20_000_000),
        ("Battalion Chief", 50_000_000),
        ("Division Chief", 1_000_000_000),
        ("Deputy Chief", 2_000_000_000),
        ("Fire Chief", 5_000_000_000),
        ("Fire Commissioner", 10_000_000_000),
    ]


def test_credit_rank_lookup_and_promotion_detection():
    assert find_rank("Senior Firefighter").key == "senior_firefighter"
    assert find_rank("fire_chief").name == "Fire Chief"
    assert rank_for_credits(199).name == "Probie"
    assert rank_for_credits(200).name == "Firefighter"
    assert rank_for_credits(10_000_000_000).name == "Fire Commissioner"
    assert is_promotion("firefighter", "captain")
    assert not is_promotion("captain", "firefighter")


def test_credit_rank_defaults_include_configured_roles_and_promotion_channel():
    assert DEFAULT_RANK_ROLE_IDS == {
        "probie": 669488072911618048,
        "firefighter": 669488631811014657,
        "senior_firefighter": 669488681639346187,
        "fire_apparatus_operator": 669488729060147202,
        "lieutenant": 669488786480300062,
        "captain": 669488849780473856,
        "staff_captain": 669488888468733981,
        "battalion_chief": 669488934140641290,
        "division_chief": 669488982199107595,
        "deputy_chief": 669489030202916884,
        "fire_chief": 669489070166114314,
        "fire_commissioner": 1437513734364069940,
    }
    assert DEFAULT_GUILD["rank_role_ids"] == DEFAULT_RANK_ROLE_IDS
    assert DEFAULT_GUILD["promotion_channel_id"] == 543935264708362251
    assert DEFAULT_GUILD["announce_first_assignment"] is False
    assert DEFAULT_GUILD["baseline_initialized"] is False


def test_credit_rank_first_sync_never_announces_promotions():
    assert not should_announce_rank_change(
        "firefighter",
        "captain",
        baseline_initialized=False,
        first_assignment=False,
        announce_first_assignment=True,
    )
    assert should_announce_rank_change(
        "firefighter",
        "captain",
        baseline_initialized=True,
        first_assignment=False,
        announce_first_assignment=False,
    )
    assert not should_announce_rank_change(
        None,
        "captain",
        baseline_initialized=True,
        first_assignment=True,
        announce_first_assignment=False,
    )
