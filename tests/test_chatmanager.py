import unittest

from chatmanager.chat_manager import (
    ChatManager,
    DEFAULT_POLL_INTERVAL_SECONDS,
    MIN_MC_POST_INTERVAL_SECONDS,
    build_chat_payload,
    discord_timestamp,
    format_discord_message_for_mc,
    parse_chat_form,
    parse_chat_history,
    truncate_embed_value,
)


CHAT_FORM_HTML = """
<html>
  <body>
    <form action="/alliance_chats" id="new_alliance_chat" method="post">
      <input name="utf8" type="hidden" value="&#x2713;" />
      <input name="authenticity_token" type="hidden" value="secret" />
      <input id="alliance_chat_message" name="alliance_chat[message]" type="text" />
    </form>
  </body>
</html>
"""


CHAT_HISTORY_HTML = """
<html>
  <body>
    <div class="well" id="chat_message_6941664" data-message-time="2026-06-20T16:14:12-04:00">
      <strong><a href="/profile/814047">Mtycofire</a></strong>
      <span class="pull-right">June 20, 2026 16:14</span>
      <div class="message-content" style="margin-top: 10px;">
        <p>@MOCOFIREEMS thanks for that</p>
      </div>
    </div>
    <div class="well" id="chat_message_6941627" data-message-time="2026-06-20T15:45:46-04:00">
      <strong><a href="/profile/814047">Mtycofire</a></strong>
      <span class="pull-right">June 20, 2026 15:45</span>
      <div class="message-content" style="margin-top: 10px;">
        <p>Yep 3 more missions to add to the Bermuda mess</p>
      </div>
    </div>
  </body>
</html>
"""


class ChatManagerParsingTests(unittest.TestCase):
    def test_chat_bridge_uses_30_second_game_interval(self):
        self.assertEqual(DEFAULT_POLL_INTERVAL_SECONDS, 30)
        self.assertEqual(MIN_MC_POST_INTERVAL_SECONDS, 30)

    def test_parse_chat_form_reads_message_payload_fields(self):
        form = parse_chat_form(CHAT_FORM_HTML, "https://www.missionchief.com/")

        self.assertEqual(form.action, "https://www.missionchief.com/alliance_chats")
        self.assertEqual(form.method, "post")
        self.assertEqual(form.message_field, "alliance_chat[message]")
        self.assertEqual(form.hidden_fields["authenticity_token"], "secret")

        payload = build_chat_payload(form, "[DutchFireFighter] Test message")
        self.assertEqual(payload["alliance_chat[message]"], "[DutchFireFighter] Test message")
        self.assertEqual(payload["authenticity_token"], "secret")

    def test_parse_chat_history_reads_messages_oldest_first(self):
        messages = parse_chat_history(CHAT_HISTORY_HTML)

        self.assertEqual([message.chat_id for message in messages], [6941627, 6941664])
        self.assertEqual(messages[0].username, "Mtycofire")
        self.assertEqual(messages[0].user_id, "814047")
        self.assertEqual(messages[0].message, "Yep 3 more missions to add to the Bermuda mess")
        self.assertEqual(messages[1].timestamp, "2026-06-20T16:14:12-04:00")

    def test_discord_timestamp_includes_full_and_relative_time(self):
        self.assertEqual(
            discord_timestamp("2026-06-20T16:14:12-04:00"),
            "<t:1781986452:F> (<t:1781986452:R>)",
        )
        self.assertEqual(discord_timestamp(""), "Unknown")

    def test_format_discord_message_for_missionchief(self):
        self.assertEqual(
            format_discord_message_for_mc("DutchFireFighter", "Hello\nworld"),
            "[DutchFireFighter] Hello world",
        )

    def test_truncate_embed_value_uses_discord_field_limit(self):
        value = truncate_embed_value("x" * 1100)

        self.assertEqual(len(value), 1024)
        self.assertTrue(value.endswith("..."))

    def test_build_game_chat_embed_uses_requested_fields(self):
        manager = ChatManager.__new__(ChatManager)
        chat = parse_chat_history(CHAT_HISTORY_HTML)[0]

        embed = manager._build_chat_embed(chat)

        self.assertEqual(embed.kwargs["title"], "MissionChief Alliance Chat")
        fields = {field["name"]: field["value"] for field in embed.fields}
        self.assertEqual(fields["Name"], "Mtycofire")
        self.assertIn("<t:", fields["Time"])
        self.assertEqual(fields["Message"], "Yep 3 more missions to add to the Bermuda mess")


if __name__ == "__main__":
    unittest.main()
