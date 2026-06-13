from datetime import datetime, timedelta, timezone

import pytest

from admintimednotifications.admintimednotifications import (
    LOCAL_TIMEZONE,
    MANAGEMENT_PANEL_TITLE,
    format_channel_reference,
    first_scheduled_run,
    is_management_panel_message,
    next_scheduled_run,
    next_run,
    parse_title_body as parse_timer_title_body,
    split_due_reminders,
)
from announcer.announcer import parse_title_body as parse_announcement_title_body
from announcementpanel.announcementpanel import (
    ButtonWizardState,
    button_config_from_wizard_state,
    button_wizard_state_from_config,
    format_announcement_content_chunks,
    normalize_id_list,
    normalize_button_key,
    panel_message_record,
    parse_channel_ids,
    parse_label_message,
    unique_button_key,
)
from botstatus.botstatus import (
    StatusActivity,
    choose_activity,
    clean_activity_type,
    compact_activity_text,
    format_activity_text,
)
from rolebasedcredits.rolebasedcredits import (
    CREDIT_RANKS,
    DEFAULT_GUILD,
    DEFAULT_RANK_ROLE_IDS,
    ensure_exit_cleanup_schema,
    find_rank,
    is_promotion,
    mark_rank_exit_rows_processed,
    pending_rank_exit_rows,
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
    assert parse_channel_ids("<#123456789012345678>, 987654321098765432") == [
        123456789012345678,
        987654321098765432,
    ]
    assert parse_channel_ids("123456789012345678 123456789012345678") == [
        123456789012345678
    ]
    assert unique_button_key("Double Credits!", {"doublecredits"}) == "doublecredits-2"
    assert panel_message_record("123456789012345678", "987654321098765432") == {
        "channel_id": 123456789012345678,
        "message_id": 987654321098765432,
    }
    content = format_announcement_content_chunks(
        "Training",
        "<@&123456789012345678> Training starts now",
    )
    assert content == ["<@&123456789012345678> Training starts now"]
    content_with_role = format_announcement_content_chunks(
        "Training",
        "Training starts now",
        ping_role_id=123456789012345678,
    )
    assert content_with_role == ["<@&123456789012345678>\nTraining starts now"]
    long_content = format_announcement_content_chunks("Training", "x" * 4500)
    assert len(long_content) == 3
    assert all(len(chunk) <= 2000 for chunk in long_content)


def test_panel_button_wizard_state_round_trip():
    assert normalize_id_list(["123456789012345678", 123456789012345678, "bad"]) == [
        123456789012345678
    ]
    state = ButtonWizardState(
        original_key="oldtraining",
        key="training",
        label="Training",
        message="Training starts now",
        channel_ids=[123456789012345678],
        ping_role_id=987654321098765432,
    )
    config = button_config_from_wizard_state(state)
    assert config == {
        "label": "Training",
        "message": "Training starts now",
        "channel_ids": [123456789012345678],
        "ping_role_id": 987654321098765432,
    }
    restored = button_wizard_state_from_config("training", config)
    assert restored.original_key == "training"
    assert restored.key == "training"
    assert restored.channel_ids == [123456789012345678]
    assert restored.ping_role_id == 987654321098765432


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


def test_admin_timer_recurring_schedule_helpers():
    now = datetime(2026, 6, 13, 10, 0, tzinfo=LOCAL_TIMEZONE)

    weekly_ts = first_scheduled_run("weekly", "maandag", "09:30", now=now)
    weekly_dt = datetime.fromtimestamp(weekly_ts, tz=LOCAL_TIMEZONE)
    assert (weekly_dt.year, weekly_dt.month, weekly_dt.day, weekly_dt.hour, weekly_dt.minute) == (
        2026,
        6,
        15,
        9,
        30,
    )

    monthly_now = datetime(2026, 2, 1, 10, 0, tzinfo=LOCAL_TIMEZONE)
    monthly_ts = first_scheduled_run("monthly", "31", "08:00", now=monthly_now)
    monthly_dt = datetime.fromtimestamp(monthly_ts, tz=LOCAL_TIMEZONE)
    assert (monthly_dt.year, monthly_dt.month, monthly_dt.day, monthly_dt.hour) == (2026, 2, 28, 8)

    reminder = {
        "recurrence": "monthly",
        "day": "31",
        "time": "08:00",
        "next_run": monthly_ts,
    }
    next_month_ts = next_scheduled_run(reminder, now_ts=monthly_ts)
    next_month_dt = datetime.fromtimestamp(next_month_ts, tz=LOCAL_TIMEZONE)
    assert (next_month_dt.year, next_month_dt.month, next_month_dt.day, next_month_dt.hour) == (
        2026,
        3,
        31,
        8,
    )


def test_admin_timer_due_split_includes_snoozed_reminders():
    due, pending = split_due_reminders(
        [
            {"id": 1, "next_run": 500, "snooze_until": 100},
            {"id": 2, "next_run": 500, "snooze_until": 600},
        ],
        now_ts=100,
    )

    assert [item["id"] for item in due] == [1]
    assert [item["id"] for item in pending] == [2]


def test_admin_timer_panel_message_detection_and_channel_display():
    class FakeEmbed:
        title = MANAGEMENT_PANEL_TITLE

    class OtherEmbed:
        title = "Other"

    class FakeAuthor:
        id = 123

    class FakeMessage:
        author = FakeAuthor()
        embeds = [FakeEmbed()]

    class OtherMessage:
        author = FakeAuthor()
        embeds = [OtherEmbed()]

    class FakeChannel:
        id = 1421625293130567690
        name = "action-required"
        mention = "<#1421625293130567690>"

    assert is_management_panel_message(FakeMessage(), bot_user_id=123)
    assert not is_management_panel_message(FakeMessage(), bot_user_id=456)
    assert not is_management_panel_message(OtherMessage(), bot_user_id=123)
    assert format_channel_reference(FakeChannel(), 1) == "#action-required (`1421625293130567690`)"
    assert format_channel_reference(None, 1421625293130567690) == "Missing channel `1421625293130567690`"


def test_botstatus_accepts_only_supported_activity_types():
    assert clean_activity_type("Watching") == "watching"
    with pytest.raises(ValueError):
        clean_activity_type("sleeping")


def test_botstatus_formats_and_prioritizes_background_activity():
    now = datetime(2026, 6, 13, 12, 0, tzinfo=timezone.utc)
    low = StatusActivity(
        token="low",
        source="MembersScraper",
        detail="scraping alliance members",
        priority=50,
        activity_type="watching",
        started_at=now,
        updated_at=now,
    )
    high = StatusActivity(
        token="high",
        source="RoleBasedCredits",
        detail="syncing credit rank roles",
        priority=80,
        activity_type="watching",
        started_at=now,
        updated_at=now,
    )
    expired = StatusActivity(
        token="expired",
        source="LogsScraper",
        detail="scraping alliance logs",
        priority=100,
        activity_type="watching",
        started_at=now,
        updated_at=now,
        expires_at=now - timedelta(seconds=1),
    )

    assert choose_activity([low, high, expired], now=now) is high
    assert (
        format_activity_text("MembersScraper", "scraping alliance members page 10")
        == "MembersScraper: scraping alliance members page 10"
    )
    assert (
        compact_activity_text("MembersScraper", "scraping alliance members page 10")
        == "Members: page 10"
    )
    assert (
        compact_activity_text("RoleBasedCredits", "syncing credit rank roles in FARA")
        == "Ranks: syncing"
    )
    assert len(format_activity_text("Source", "x" * 200)) == 128
    assert len(compact_activity_text("Source", "x" * 200, max_length=32)) == 32


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


def test_credit_rank_exit_cleanup_reads_and_marks_membersync_rows(tmp_path):
    db_path = tmp_path / "membersync.db"
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE member_left_alliance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mc_user_id TEXT NOT NULL,
                username TEXT,
                discord_id INTEGER,
                exit_detected_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO member_left_alliance
            (mc_user_id, username, discord_id, exit_detected_at)
            VALUES ('123', 'Departed User', 456, '2026-06-13T12:00:00')
            """
        )
        conn.commit()
    finally:
        conn.close()

    assert ensure_exit_cleanup_schema(db_path)
    rows = pending_rank_exit_rows(db_path)

    assert rows == [
        {
            "id": 1,
            "mc_user_id": "123",
            "username": "Departed User",
            "discord_id": 456,
        }
    ]

    mark_rank_exit_rows_processed(db_path, [1])

    assert pending_rank_exit_rows(db_path) == []
