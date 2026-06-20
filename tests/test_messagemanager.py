import unittest

from messagemanager.message_manager import (
    MemberResolutionError,
    build_message_payload,
    message_was_sent,
    parse_message_form,
    parse_send_spec,
    resolve_alliance_member_name,
    safe_payload_summary,
    summarize_message_form,
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


class MessageManagerTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
