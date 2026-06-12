import asyncio
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.database import MemberDatabase
from MemberManager.models import MemberData
from MemberManager.views import MemberOverviewView, TogglePinNoteModal


class MemberManagerNotesTests(unittest.TestCase):
    def test_get_notes_matches_discord_or_mc_identity_when_both_are_known(self):
        async def run_test():
            with tempfile.TemporaryDirectory() as temp_dir:
                database = MemberDatabase(str(Path(temp_dir) / "membermanager.db"))
                await database.initialize()
                try:
                    await database.add_note(
                        guild_id=1,
                        discord_id=123,
                        mc_user_id=None,
                        note_text="Discord-only note",
                        author_id=999,
                        author_name="Admin",
                    )
                    await database.add_note(
                        guild_id=1,
                        discord_id=None,
                        mc_user_id="456",
                        note_text="MC-only note",
                        author_id=999,
                        author_name="Admin",
                    )

                    notes = await database.get_notes(discord_id=123, mc_user_id="456")
                finally:
                    await database.close()

            return notes

        notes = asyncio.run(run_test())

        self.assertEqual(len(notes), 2)
        self.assertEqual(
            {note["note_text"] for note in notes},
            {"Discord-only note", "MC-only note"},
        )

    def test_member_note_lookup_rejects_ref_code_for_other_member(self):
        class FakeDB:
            async def get_notes(self, **kwargs):
                self.kwargs = kwargs
                return [
                    {
                        "ref_code": "N2026-000001",
                        "discord_id": 999,
                        "mc_user_id": "999",
                        "note_text": "Other member",
                    }
                ]

        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456")
        view.db = FakeDB()

        note = asyncio.run(view._get_member_note("N2026-000001"))

        self.assertIsNone(note)

    def test_toggle_pin_note_updates_note_and_records_audit_event(self):
        class FakeDB:
            def __init__(self):
                self.pinned = None
                self.event = None

            async def get_notes(self, **kwargs):
                self.kwargs = kwargs
                return [
                    {
                        "ref_code": "N2026-000001",
                        "discord_id": 123,
                        "mc_user_id": None,
                        "note_text": "Important note",
                    }
                ]

            async def pin_note(self, ref_code, pinned=True):
                self.pinned = (ref_code, pinned)
                return True

            async def add_event(self, **kwargs):
                self.event = kwargs

        fake_db = FakeDB()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456")
        view.db = fake_db
        view.current_tab = "notes"
        view._update_view = AsyncMock()

        modal = TogglePinNoteModal(view)
        modal.ref_code.value = "N2026-000001"
        modal.action.value = "PIN"

        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(fake_db.pinned, ("N2026-000001", True))
        self.assertEqual(fake_db.event["event_type"], "note_pinned")
        self.assertEqual(fake_db.event["event_data"], {"ref_code": "N2026-000001"})
        view._update_view.assert_awaited_once_with(interaction)


if __name__ == "__main__":
    unittest.main()
