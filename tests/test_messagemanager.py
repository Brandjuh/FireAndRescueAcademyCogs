import asyncio
import unittest
import types
from unittest.mock import AsyncMock

import messagemanager.message_manager as message_manager_module
from messagemanager.message_manager import (
    INBOX_SCAN_INTERVAL_SECONDS,
    INBOX_SCAN_JITTER_SECONDS,
    MemberResolutionError,
    MessageManager,
    TAX_WARNING_MIN_DAYS_BETWEEN,
    TAX_WARNING_PRESETS,
    build_forum_thread_title,
    build_reply_payload,
    build_message_payload,
    discord_timestamp_from_iso,
    extract_conversation_id,
    format_duration,
    inbox_scan_delay_seconds,
    message_was_sent,
    parse_conversation_messages,
    parse_inbox_messages,
    parse_message_form,
    parse_send_spec,
    resolve_alliance_member_name,
    safe_payload_summary,
    split_discord_content,
    summarize_message_form,
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

    def test_tax_warning_presets_use_alliance_donation_texts(self):
        self.assertEqual(TAX_WARNING_MIN_DAYS_BETWEEN, 7)
        self.assertEqual(TAX_WARNING_PRESETS[1][0], "Reminder: Please set your alliance donation to 5%")
        self.assertEqual(TAX_WARNING_PRESETS[2][0], "Warning: Alliance donation below required minimum")
        self.assertEqual(TAX_WARNING_PRESETS[3][0], "Final warning: Alliance donation requirement not met")
        self.assertIn("Code of Conduct, rule 4.1", TAX_WARNING_PRESETS[1][1])
        self.assertIn("This is an official warning", TAX_WARNING_PRESETS[2][1])
        self.assertIn("This is your final opportunity", TAX_WARNING_PRESETS[3][1])

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


if __name__ == "__main__":
    unittest.main()
