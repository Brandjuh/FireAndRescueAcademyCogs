import asyncio
import time
import unittest
import types
from unittest.mock import AsyncMock

import messagemanager.message_manager as message_manager_module
from messagemanager.message_manager import (
    INBOX_SCAN_INTERVAL_SECONDS,
    INBOX_SCAN_JITTER_SECONDS,
    MemberResolutionError,
    MessageManager,
    TAX_WARNING_NEW_MEMBER_GRACE_HOURS,
    TAX_WARNING_MIN_DAYS_BETWEEN,
    TAX_WARNING_KICK_NOTICE_BODY,
    TAX_WARNING_KICK_NOTICE_SUBJECT,
    TAX_WARNING_PRESETS,
    build_forum_thread_title,
    build_reply_payload,
    build_message_payload,
    discord_timestamp_from_iso,
    extract_conversation_id,
    format_duration,
    get_loaded_cog_names,
    get_sanction_manager_cog,
    inbox_scan_delay_seconds,
    kick_response_indicates_failure,
    kick_response_indicates_success,
    message_was_sent,
    parse_conversation_messages,
    parse_inbox_messages,
    parse_kick_confirmation_form,
    parse_message_form,
    parse_send_spec,
    resolve_alliance_member_name,
    safe_payload_summary,
    split_discord_content,
    parse_member_first_seen_timestamp,
    tax_warning_member_is_in_grace_period,
    tax_warning_reason_matches,
    tax_warning_level_from_sanction_type,
    summarize_message_form,
    tax_warning_stats_from_sanctions,
    tax_warning_stats_from_state,
    tax_warning_kick_is_due,
    tax_warning_sanction_manager_error,
    tax_warning_is_due,
    tax_warning_level,
    tax_warning_member_identity,
)


MESSAGE_FORM_HTML = """
<html>
  <body>
    <form action="/messages" method="post">
      <input name="utf8" type="hidden" value="&#x2713;" />
      <input name="authenticity_token" type="hidden" value="secret" />
      <label for="message_recipient">Username</label>
      <input id="message_recipient" name="message[recipient]" type="text" />
      <label for="message_subject">Subject</label>
      <input id="message_subject" name="message[subject]" type="text" />
      <label for="message_body">Message</label>
      <textarea id="message_body" name="message[body]"></textarea>
      <input name="commit" type="submit" value="Send Message" />
    </form>
  </body>
</html>
"""


INBOX_HTML = """
<html>
  <body>
    <div class="panel-body system_messages_container">
      <table>
        <tr>
          <td><div>New</div></td>
          <td><div><a href="/messages/system_message/767">System update</a></div></td>
        </tr>
      </table>
    </div>
    <form action="/messages/trash" method="post">
      <input id="current_box" name="current_box" type="hidden" value="inbox" />
      <table>
        <tbody>
          <tr>
            <td><input name="conversations[]" type="checkbox" value="238264" /></td>
            <td>New</td>
            <td><a href="/messages/238264">DutchFireFighter</a></td>
            <td><a href="/messages/238264">koekkoek</a></td>
          </tr>
          <tr>
            <td><input name="conversations[]" type="checkbox" value="235837" /></td>
            <td></td>
            <td><a href="/messages/235837">Spieler1259735</a></td>
            <td><a href="/messages/235837">Game Over</a></td>
          </tr>
        </tbody>
      </table>
    </form>
  </body>
</html>
"""


REPLY_HTML = """
<html>
  <body>
    <div class="well" data-message-time="2026-06-19T20:52:25-04:00">
      <strong><a href="/profile/88649">DutchFireFighter</a></strong>
      <span class="pull-right">19 Jun 20:52</span>
      <p>test</p>
    </div>
    <form action="/messages" method="post">
      <input name="utf8" type="hidden" value="&#x2713;" />
      <input name="authenticity_token" type="hidden" value="secret" />
      <input id="message_conversation_id" name="message[conversation_id]" type="hidden" value="238264" />
      <textarea id="message_body" name="message[body]"></textarea>
      <input id="submit_button" name="submit_button" type="submit" value="Send Message" />
    </form>
  </body>
</html>
"""


KICK_CONFIRM_HTML = """
<html>
  <body>
    <form action="/verband/kick/12345" method="post">
      <input name="authenticity_token" type="hidden" value="secret" />
      <input name="commit" type="submit" value="OK" />
      <a href="/verband">Cancel</a>
    </form>
  </body>
</html>
"""


class MessageManagerTests(unittest.TestCase):
    def test_inbox_scan_delay_uses_15_minute_interval_with_jitter(self):
        class FixedRng:
            @staticmethod
            def uniform(start, end):
                del start
                return end

        self.assertEqual(
            inbox_scan_delay_seconds(FixedRng),
            INBOX_SCAN_INTERVAL_SECONDS + INBOX_SCAN_JITTER_SECONDS,
        )

    def test_parse_message_form_identifies_fields(self):
        form = parse_message_form(MESSAGE_FORM_HTML)

        self.assertEqual(form.action, "https://www.missionchief.com/messages")
        self.assertEqual(form.method, "post")
        self.assertEqual(form.recipient_field, "message[recipient]")
        self.assertEqual(form.subject_field, "message[subject]")
        self.assertEqual(form.body_field, "message[body]")
        self.assertEqual(form.submit_name, "commit")
        self.assertEqual(form.submit_value, "Send Message")

    def test_build_message_payload_preserves_username_case(self):
        form = parse_message_form(MESSAGE_FORM_HTML)

        payload = build_message_payload(form, "CrashTestDummy", "Warning", "Please increase tax.")

        self.assertIn(("message[recipient]", "CrashTestDummy"), payload)
        self.assertIn(("message[subject]", "Warning"), payload)
        self.assertIn(("message[body]", "Please increase tax."), payload)
        self.assertIn(("commit", "Send Message"), payload)

    def test_parse_send_spec_requires_all_fields(self):
        self.assertEqual(
            parse_send_spec("CrashTestDummy | Subject text | Body text"),
            ("CrashTestDummy", "Subject text", "Body text"),
        )
        with self.assertRaises(ValueError):
            parse_send_spec("CrashTestDummy | Subject only")

    def test_resolve_alliance_member_name_ignores_case(self):
        members = [
            {"name": "DutchFireFighter", "user_id": "88649"},
            {"name": "CrashTestDummy", "user_id": "12345"},
        ]

        self.assertEqual(resolve_alliance_member_name("dutchfirefighter", members), "DutchFireFighter")
        self.assertEqual(resolve_alliance_member_name("CRASHTESTDUMMY", members), "CrashTestDummy")

    def test_resolve_alliance_member_name_accepts_member_id(self):
        members = [{"name": "CrashTestDummy", "user_id": "12345"}]

        self.assertEqual(resolve_alliance_member_name("12345", members), "CrashTestDummy")

    def test_resolve_alliance_member_name_rejects_unknown_member(self):
        members = [{"name": "CrashTestDummy", "user_id": "12345"}]

        with self.assertRaises(MemberResolutionError):
            resolve_alliance_member_name("NotInAlliance", members)

    def test_resolve_alliance_member_name_rejects_ambiguous_member(self):
        members = [
            {"name": "TestUser", "user_id": "1"},
            {"name": "testuser", "user_id": "2"},
        ]

        with self.assertRaises(MemberResolutionError):
            resolve_alliance_member_name("TESTUSER", members)

    def test_parse_inbox_messages_ignores_system_messages(self):
        messages = parse_inbox_messages(INBOX_HTML)

        self.assertEqual(len(messages), 2)
        self.assertEqual(messages[0].conversation_id, "238264")
        self.assertEqual(messages[0].sender, "DutchFireFighter")
        self.assertEqual(messages[0].subject, "koekkoek")
        self.assertTrue(messages[0].is_new)
        self.assertEqual(messages[1].conversation_id, "235837")
        self.assertFalse(messages[1].is_new)
        self.assertFalse(any("system_message" in message.url for message in messages))

    def test_build_reply_payload_uses_conversation_id_and_body(self):
        action, payload = build_reply_payload(
            REPLY_HTML,
            "Reply text",
            "https://www.missionchief.com/messages/238264",
        )

        self.assertEqual(action, "https://www.missionchief.com/messages")
        self.assertIn(("message[conversation_id]", "238264"), payload)
        self.assertIn(("message[body]", "Reply text"), payload)
        self.assertIn(("submit_button", "Send Message"), payload)

    def test_extract_conversation_id_from_url_or_reply_form(self):
        self.assertEqual(
            extract_conversation_id("", "https://www.missionchief.com/messages/238264"),
            "238264",
        )
        self.assertEqual(extract_conversation_id(REPLY_HTML), "238264")

    def test_parse_conversation_messages_reads_latest_visible_message(self):
        messages = parse_conversation_messages(REPLY_HTML)

        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0].author, "DutchFireFighter")
        self.assertEqual(messages[0].body, "test")
        self.assertEqual(messages[0].timestamp, "2026-06-19T20:52:25-04:00")

    def test_build_forum_thread_title_uses_username_and_subject(self):
        self.assertEqual(
            build_forum_thread_title("DutchFireFighter", "koekkoek", "238264"),
            "DutchFireFighter - koekkoek (238264)",
        )
        self.assertLessEqual(len(build_forum_thread_title("User", "x" * 200)), 100)

    def test_conversation_embed_uses_metadata_without_private_message_link(self):
        manager = MessageManager.__new__(MessageManager)
        if not hasattr(message_manager_module.discord, "utils"):
            message_manager_module.discord.utils = types.SimpleNamespace(escape_markdown=lambda value: value)

        embed = manager._build_conversation_embed(
            title="New MissionChief Reply",
            conversation_id="238294",
            username="DutchFireFighter",
            subject="Boopie",
            timestamp="2026-06-20T06:08:36-04:00",
        )

        self.assertEqual(embed.kwargs["title"], "New MissionChief Reply")
        values = {field["name"]: field["value"] for field in embed.fields}
        self.assertEqual(values["Member"], "DutchFireFighter")
        self.assertEqual(values["Conversation ID"], "`238294`")
        self.assertEqual(values["Time"], "<t:1781950116:F>")
        self.assertEqual(values["Title"], "Boopie")
        self.assertNotIn("https://www.missionchief.com/messages/238294", str(values))

    def test_discord_timestamp_from_iso_uses_full_timestamp_style(self):
        self.assertEqual(
            discord_timestamp_from_iso("2026-06-20T06:08:36-04:00"),
            "<t:1781950116:F>",
        )
        self.assertEqual(discord_timestamp_from_iso("not-a-date"), "not-a-date")

    def test_split_discord_content_preserves_text_with_limits(self):
        chunks = split_discord_content("First paragraph.\n\n" + ("x" * 50), limit=30)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(len(chunk) <= 30 for chunk in chunks))
        self.assertEqual("".join(chunks), "First paragraph." + ("x" * 50))

    def test_format_duration_returns_short_operational_text(self):
        self.assertEqual(format_duration(45), "45s")
        self.assertEqual(format_duration(15 * 60), "15m")
        self.assertEqual(format_duration(75 * 60), "1h 15m")

    def test_format_send_result_reports_linked_forum_thread(self):
        message = MessageManager._format_send_result(
            {
                "resolved_username": "DutchFireFighter",
                "conversation_id": "238294",
                "thread": types.SimpleNamespace(mention="#thread"),
            }
        )

        self.assertIn("MissionChief message sent to `DutchFireFighter`.", message)
        self.assertIn("Conversation `238294` linked to forum: #thread", message)

    def test_sent_message_forum_thread_uses_sent_timestamp(self):
        manager = MessageManager.__new__(MessageManager)
        manager._ensure_conversation_thread = AsyncMock(return_value=types.SimpleNamespace(id=123))

        result = asyncio.run(
            manager._link_sent_message_to_forum(
                conversation_id="238294",
                username="DutchFireFighter",
                subject="Boopie",
                body="burp",
                sent_at="2026-06-20T12:00:00+00:00",
            )
        )

        self.assertEqual(result.id, 123)
        manager._ensure_conversation_thread.assert_awaited_once_with(
            conversation_id="238294",
            username="DutchFireFighter",
            subject="Boopie",
            preview="burp",
            last_message_time="2026-06-20T12:00:00+00:00",
            opening_title="MissionChief Message Sent",
            opening_timestamp="2026-06-20T12:00:00+00:00",
        )

    def test_inbound_reply_sends_embed_before_body_text(self):
        manager = MessageManager.__new__(MessageManager)
        if not hasattr(message_manager_module.discord, "utils"):
            message_manager_module.discord.utils = types.SimpleNamespace(escape_markdown=lambda value: value)
        if not hasattr(message_manager_module.discord, "AllowedMentions"):
            message_manager_module.discord.AllowedMentions = types.SimpleNamespace(none=lambda: "none")

        class FakeConfig:
            async def conversation_threads(self):
                return {"238294": {"thread_id": 123, "last_message_time": "old"}}

        class FakeThread:
            def __init__(self):
                self.calls = []
                self.id = 123

            async def send(self, **kwargs):
                self.calls.append(kwargs)

        thread = FakeThread()
        manager.config = FakeConfig()
        manager._ensure_conversation_thread = AsyncMock(return_value=thread)
        manager._save_conversation_thread = AsyncMock()

        result = asyncio.run(
            manager._post_inbound_to_forum(
                conversation_id="238294",
                username="DutchFireFighter",
                subject="Boopie",
                body="burp",
                timestamp="2026-06-20T07:00:41-04:00",
            )
        )

        self.assertIs(result, thread)
        self.assertEqual(len(thread.calls), 2)
        self.assertIn("embed", thread.calls[0])
        self.assertNotIn("content", thread.calls[0])
        self.assertEqual(thread.calls[1]["content"], "burp")
        self.assertNotIn("embed", thread.calls[1])

    def test_build_payload_rejects_empty_visible_fields(self):
        form = parse_message_form(MESSAGE_FORM_HTML)

        with self.assertRaises(ValueError):
            build_message_payload(form, "CrashTestDummy", "Warning", "")

    def test_success_detection_reads_message_sent(self):
        self.assertTrue(message_was_sent("<div class='alert'>Message Sent.</div>"))
        self.assertFalse(message_was_sent("<div class='alert'>Error</div>"))

    def test_debug_summaries_redact_tokens(self):
        form = parse_message_form(MESSAGE_FORM_HTML)
        summary = summarize_message_form(form)
        payload = safe_payload_summary(build_message_payload(form, "User", "Subject", "Body"))

        self.assertIn("authenticity_token (input:hidden) = REDACTED", summary)
        self.assertIn("authenticity_token=REDACTED", payload)
        self.assertNotIn("secret", summary)
        self.assertNotIn("secret", payload)

    def test_tax_warning_level_is_capped_at_three(self):
        self.assertEqual(tax_warning_level(0), 1)
        self.assertEqual(tax_warning_level(2), 3)
        self.assertIsNone(tax_warning_level(3))

    def test_tax_warning_due_respects_days_between_warnings(self):
        now = 1_000_000

        self.assertTrue(
            tax_warning_is_due(
                existing_warning_count=0,
                last_warning_at=None,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )
        self.assertFalse(
            tax_warning_is_due(
                existing_warning_count=1,
                last_warning_at=now - 6 * 86400,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )
        self.assertTrue(
            tax_warning_is_due(
                existing_warning_count=1,
                last_warning_at=now - 7 * 86400,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )
        self.assertFalse(
            tax_warning_is_due(
                existing_warning_count=3,
                last_warning_at=now - 10 * 86400,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )

    def test_tax_warning_kick_due_requires_three_warnings_and_final_gap(self):
        now = 1_000_000

        self.assertFalse(
            tax_warning_kick_is_due(
                existing_warning_count=2,
                last_warning_at=now - 10 * 86400,
                kicked_at=None,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )
        self.assertFalse(
            tax_warning_kick_is_due(
                existing_warning_count=3,
                last_warning_at=now - 6 * 86400,
                kicked_at=None,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )
        self.assertTrue(
            tax_warning_kick_is_due(
                existing_warning_count=3,
                last_warning_at=now - 7 * 86400,
                kicked_at=None,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )
        self.assertFalse(
            tax_warning_kick_is_due(
                existing_warning_count=3,
                last_warning_at=now - 7 * 86400,
                kicked_at=now - 1,
                now=now,
                min_days_between=TAX_WARNING_MIN_DAYS_BETWEEN,
            )
        )

    def test_tax_warning_presets_use_alliance_donation_texts(self):
        self.assertEqual(TAX_WARNING_MIN_DAYS_BETWEEN, 7)
        self.assertEqual(TAX_WARNING_PRESETS[1][0], "Reminder: Please set your alliance donation to 5%")
        self.assertEqual(TAX_WARNING_PRESETS[2][0], "Warning: Alliance donation below required minimum")
        self.assertEqual(TAX_WARNING_PRESETS[3][0], "Final warning: Alliance donation requirement not met")
        self.assertIn("Code of Conduct, rule 4.1", TAX_WARNING_PRESETS[1][1])
        self.assertIn("This is an official warning", TAX_WARNING_PRESETS[2][1])
        self.assertIn("This is your final opportunity", TAX_WARNING_PRESETS[3][1])

    def test_tax_warning_kick_notice_allows_reapply_without_staff_contact(self):
        self.assertEqual(TAX_WARNING_KICK_NOTICE_SUBJECT, "Removed from Fire & Rescue Academy")
        self.assertIn(
            "You are welcome to reapply if you are willing to follow the alliance rules",
            TAX_WARNING_KICK_NOTICE_BODY,
        )
        self.assertIn("setting your alliance donation to at least 5%", TAX_WARNING_KICK_NOTICE_BODY)
        self.assertNotIn("contact alliance staff", TAX_WARNING_KICK_NOTICE_BODY)
        self.assertNotIn("after correcting your alliance donation setting", TAX_WARNING_KICK_NOTICE_BODY)

    def test_tax_warning_member_identity_reads_scraper_member_shapes(self):
        self.assertEqual(
            tax_warning_member_identity(
                {
                    "mc_user_id": 456,
                    "name": "CrashTestDummy",
                    "contribution_rate": "4.5",
                }
            ),
            ("456", "CrashTestDummy", 4.5),
        )

    def test_tax_warning_reason_matches_current_and_legacy_reason_texts(self):
        self.assertTrue(tax_warning_reason_matches(message_manager_module.TAX_WARNING_REASON_DETAIL))
        self.assertTrue(tax_warning_reason_matches("5% donation to alliance - Minimum 5% donation required."))
        self.assertTrue(tax_warning_reason_matches("Low contribution"))
        self.assertTrue(tax_warning_reason_matches("Low TAX: below 5 percent"))
        self.assertFalse(tax_warning_reason_matches("Inactivity"))

    def test_tax_warning_stats_count_warning_levels_and_auto_kicks(self):
        stats = tax_warning_stats_from_sanctions(
            [
                {
                    "sanction_type": "Warning - Official 1st warning",
                    "reason_detail": message_manager_module.TAX_WARNING_REASON_DETAIL,
                    "mc_user_id": "1",
                    "created_at": 100,
                    "status": "active",
                },
                {
                    "sanction_type": "Warning - Official 2nd warning",
                    "reason_detail": message_manager_module.TAX_WARNING_REASON_DETAIL,
                    "mc_user_id": "1",
                    "created_at": 200,
                    "status": "active",
                },
                {
                    "sanction_type": "Warning - Official 3rd and last warning",
                    "reason_detail": message_manager_module.TAX_WARNING_REASON_DETAIL,
                    "mc_user_id": "2",
                    "created_at": 300,
                    "status": "active",
                },
                {
                    "sanction_type": "Kick",
                    "reason_detail": message_manager_module.TAX_WARNING_KICK_REASON_DETAIL,
                    "mc_user_id": "2",
                    "created_at": 400,
                    "status": "active",
                },
                {
                    "sanction_type": "Warning - Official 1st warning",
                    "reason_detail": message_manager_module.TAX_WARNING_REASON_DETAIL,
                    "mc_user_id": "3",
                    "created_at": 500,
                    "effective_status": "removed",
                },
                {
                    "sanction_type": "Warning - Official 1st warning",
                    "reason_detail": "5% donation to alliance - Minimum 5% donation required.",
                    "mc_user_id": "4",
                    "created_at": 600,
                    "status": "active",
                },
                {
                    "sanction_type": "Warning - Official 1st warning",
                    "reason_detail": "Low contribution",
                    "mc_user_id": "5",
                    "created_at": 700,
                    "status": "active",
                },
            ]
        )

        self.assertEqual(stats["warnings_total"], 5)
        self.assertEqual(stats["warning_1"], 3)
        self.assertEqual(stats["warning_2"], 1)
        self.assertEqual(stats["warning_3"], 1)
        self.assertEqual(stats["auto_kicks"], 1)
        self.assertEqual(stats["members_warned"], 4)
        self.assertEqual(stats["latest_warning_at"], 700)
        self.assertEqual(stats["latest_kick_at"], 400)

    def test_get_tax_warning_stats_uses_full_sanction_contract_when_available(self):
        class FakeSanctionManager:
            def get_sanctions(self, *, guild_id, period_start_ts=None, period_end_ts=None):
                self.request = {
                    "guild_id": guild_id,
                    "period_start_ts": period_start_ts,
                    "period_end_ts": period_end_ts,
                }
                return [
                    {
                        "sanction_type": "Warning - Official 1st warning",
                        "reason_detail": "Alliance donation below 5 percent",
                        "mc_user_id": "1",
                        "created_at": 100,
                        "status": "active",
                    },
                    {
                        "sanction_type": "Warning - Official 1st warning",
                        "reason_detail": "Inactivity",
                        "mc_user_id": "2",
                        "created_at": 200,
                        "status": "active",
                    },
                ]

            def get_sanctions_by_reason_details(self, **kwargs):
                raise AssertionError("Exact reason lookup should not be used when full contract is available")

        sanction_manager = FakeSanctionManager()
        manager = MessageManager.__new__(MessageManager)
        manager._sanction_manager = lambda: sanction_manager

        stats = asyncio.run(
            manager.get_tax_warning_stats(
                123,
                period_start_ts=10,
                period_end_ts=20,
            )
        )

        self.assertEqual(sanction_manager.request["guild_id"], 123)
        self.assertEqual(sanction_manager.request["period_start_ts"], 10)
        self.assertEqual(sanction_manager.request["period_end_ts"], 20)
        self.assertEqual(stats["warnings_total"], 1)
        self.assertEqual(stats["warning_1"], 1)

    def test_tax_warning_new_member_grace_helpers(self):
        first_seen = parse_member_first_seen_timestamp("2026-07-02T10:00:00+00:00")
        now = parse_member_first_seen_timestamp("2026-07-02T12:00:00+00:00")

        self.assertTrue(
            tax_warning_member_is_in_grace_period(
                first_seen_at=first_seen,
                now=now,
                grace_hours=TAX_WARNING_NEW_MEMBER_GRACE_HOURS,
            )
        )
        self.assertFalse(
            tax_warning_member_is_in_grace_period(
                first_seen_at=first_seen,
                now=now,
                grace_hours=0,
            )
        )

    def test_tax_warning_stats_from_state_reconstructs_reached_warning_levels(self):
        stats = tax_warning_stats_from_state(
            {
                "1": {"count": 2, "last_warning_at": 200},
                "2": {"count": 3, "last_warning_at": 300, "kicked_at": 400},
            }
        )

        self.assertEqual(stats["warnings_total"], 5)
        self.assertEqual(stats["warning_1"], 2)
        self.assertEqual(stats["warning_2"], 2)
        self.assertEqual(stats["warning_3"], 1)
        self.assertEqual(stats["auto_kicks"], 1)
        self.assertEqual(stats["members_warned"], 2)

    def test_tax_warning_level_from_sanction_type_accepts_known_labels(self):
        self.assertEqual(tax_warning_level_from_sanction_type("Warning - Official 1st warning"), 1)
        self.assertEqual(tax_warning_level_from_sanction_type("Warning - Official 2nd warning"), 2)
        self.assertEqual(tax_warning_level_from_sanction_type("Warning - Official 3rd and last warning"), 3)
        self.assertIsNone(tax_warning_level_from_sanction_type("Kick"))

    def test_parse_kick_confirmation_form_selects_ok_submit(self):
        form = parse_kick_confirmation_form(
            KICK_CONFIRM_HTML,
            "https://www.missionchief.com/verband/kick/12345",
        )

        self.assertEqual(form.action, "https://www.missionchief.com/verband/kick/12345")
        self.assertEqual(form.method, "post")
        self.assertIn(("authenticity_token", "secret"), form.payload)
        self.assertIn(("commit", "OK"), form.payload)

    def test_kick_response_detection_rejects_login_or_error_pages(self):
        self.assertTrue(kick_response_indicates_success("<p>Member kicked from the alliance.</p>"))
        self.assertTrue(kick_response_indicates_failure("<form>Sign in Password</form>"))
        self.assertFalse(kick_response_indicates_success("<form>Sign in Password</form>"))

    def test_process_tax_warning_run_autokicks_after_max_warnings(self):
        class FakeConfig:
            async def tax_warning_max_per_run(self):
                return 5

            async def tax_warning_send_delay_seconds(self):
                return 0

            async def tax_warning_autokick_enabled(self):
                return True

            async def tax_warning_autokick_max_per_run(self):
                return 1

        manager = MessageManager.__new__(MessageManager)
        manager.config = FakeConfig()
        manager._tax_warning_candidates = AsyncMock(
            return_value=[
                {
                    "mc_user_id": "456",
                    "username": "CrashTestDummy",
                    "rate": 0.0,
                    "warning_count": 3,
                    "next_level": None,
                    "due": False,
                    "kick_due": True,
                }
            ]
        )
        manager._send_tax_warning = AsyncMock()
        manager._kick_tax_warning_member = AsyncMock(return_value={"kicked": True})

        result = asyncio.run(manager._process_tax_warning_run(types.SimpleNamespace(id=123)))

        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["kick_due"], 1)
        self.assertEqual(result["kicked"], 1)
        manager._send_tax_warning.assert_not_awaited()
        manager._kick_tax_warning_member.assert_awaited_once()

    def test_kick_tax_warning_member_sends_notice_before_kick(self):
        call_order = []

        async def send_notice(username, subject, body):
            call_order.append("notice")
            self.assertEqual(username, "CrashTestDummy")
            self.assertEqual(subject, TAX_WARNING_KICK_NOTICE_SUBJECT)
            self.assertIn("You are welcome to reapply", body)
            return {
                "ok": True,
                "resolved_username": "CrashTestDummy",
                "conversation_id": "238294",
                "thread": None,
            }

        async def kick_member(mc_user_id):
            call_order.append("kick")
            self.assertEqual(mc_user_id, "456")
            return True, "MissionChief confirmed the member was kicked."

        manager = MessageManager.__new__(MessageManager)
        manager._sanction_manager = lambda: types.SimpleNamespace(create_sanction_for_member=object())
        manager._send_message_and_link = AsyncMock(side_effect=send_notice)
        manager._kick_member_from_alliance = AsyncMock(side_effect=kick_member)
        manager._record_tax_kick_sanction = AsyncMock()
        manager._save_tax_warning_kick_state = AsyncMock()

        result = asyncio.run(
            manager._kick_tax_warning_member(
                types.SimpleNamespace(id=123),
                {
                    "mc_user_id": "456",
                    "username": "CrashTestDummy",
                    "rate": 0.0,
                    "last_warning_at": 123456,
                },
            )
        )

        self.assertTrue(result["kicked"])
        self.assertEqual(result["notice_conversation_id"], "238294")
        self.assertEqual(call_order, ["notice", "kick"])
        manager._record_tax_kick_sanction.assert_awaited_once()
        manager._save_tax_warning_kick_state.assert_awaited_once()

    def test_kick_tax_warning_member_does_not_kick_when_notice_fails(self):
        manager = MessageManager.__new__(MessageManager)
        manager._sanction_manager = lambda: types.SimpleNamespace(create_sanction_for_member=object())
        manager._send_message_and_link = AsyncMock(return_value={"ok": False, "reason": "Message failed."})
        manager._kick_member_from_alliance = AsyncMock()

        result = asyncio.run(
            manager._kick_tax_warning_member(
                types.SimpleNamespace(id=123),
                {
                    "mc_user_id": "456",
                    "username": "CrashTestDummy",
                    "rate": 0.0,
                    "last_warning_at": 123456,
                },
            )
        )

        self.assertFalse(result["kicked"])
        self.assertEqual(result["reason"], "Kick notice could not be sent: Message failed.")
        manager._kick_member_from_alliance.assert_not_awaited()

    def test_tax_warning_candidates_skip_members_inside_new_member_grace(self):
        now = int(time.time())

        class FakeConfig:
            async def tax_warning_min_rate(self):
                return 5.0

            async def tax_warning_min_days_between(self):
                return TAX_WARNING_MIN_DAYS_BETWEEN

            async def tax_warning_new_member_grace_hours(self):
                return TAX_WARNING_NEW_MEMBER_GRACE_HOURS

        class FakeMembersScraper:
            async def get_members(self):
                return [
                    {
                        "mc_user_id": "456",
                        "name": "NewLowTaxMember",
                        "contribution_rate": 0.0,
                    }
                ]

            async def get_member_first_seen(self, mc_user_id):
                assert str(mc_user_id) == "456"
                return message_manager_module.datetime.fromtimestamp(
                    now - 3600,
                    tz=message_manager_module.timezone.utc,
                ).isoformat()

        scraper = FakeMembersScraper()
        manager = MessageManager.__new__(MessageManager)
        manager.bot = types.SimpleNamespace(get_cog=lambda name: scraper if name == "MembersScraper" else None)
        manager.config = FakeConfig()
        manager._tax_warning_history = AsyncMock(return_value=(0, None, None))

        candidates = asyncio.run(manager._tax_warning_candidates(types.SimpleNamespace(id=123)))

        self.assertEqual(candidates, [])
        manager._tax_warning_history.assert_not_awaited()

    def test_sanction_manager_lookup_accepts_loaded_cog_name(self):
        expected_cog = object()

        class FakeBot:
            @staticmethod
            def get_cog(name):
                return expected_cog if name == "SanctionsManager" else None

        self.assertIs(get_sanction_manager_cog(FakeBot()), expected_cog)

    def test_sanction_manager_lookup_accepts_legacy_cog_name(self):
        expected_cog = object()

        class FakeBot:
            @staticmethod
            def get_cog(name):
                return expected_cog if name == "SanctionManager" else None

        self.assertIs(get_sanction_manager_cog(FakeBot()), expected_cog)

    def test_sanction_manager_lookup_accepts_contract_match(self):
        class FakeSanctionsCog:
            def create_sanction_for_member(self):
                raise AssertionError("not called")

            def get_member_sanctions(self):
                raise AssertionError("not called")

        expected_cog = FakeSanctionsCog()

        class FakeBot:
            cogs = {"UnexpectedRuntimeName": expected_cog}

            @staticmethod
            def get_cog(name):
                del name
                return None

        self.assertIs(get_sanction_manager_cog(FakeBot()), expected_cog)

    def test_loaded_cog_names_are_reported_for_missing_sanction_manager(self):
        class FakeBot:
            cogs = {"MessageManager": object(), "MembersScraper": object()}

        self.assertEqual(get_loaded_cog_names(FakeBot()), ["MembersScraper", "MessageManager"])
        self.assertIn("MembersScraper", tax_warning_sanction_manager_error(FakeBot()))


if __name__ == "__main__":
    unittest.main()
