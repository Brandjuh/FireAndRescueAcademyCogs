import asyncio
import sqlite3
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.database import MemberDatabase
from MemberManager.models import MemberData
from MemberManager.views import (
    AddNoteModal,
    DeleteNoteModal,
    EditNoteModal,
    MemberOverviewView,
    TogglePinNoteModal,
)


class MemberManagerNotesTests(unittest.TestCase):
    def test_initialize_migrates_old_notes_schema_for_current_ui_queries(self):
        async def run_test():
            with tempfile.TemporaryDirectory() as temp_dir:
                db_path = Path(temp_dir) / "membermanager.db"
                connection = sqlite3.connect(db_path)
                connection.execute(
                    """
                    CREATE TABLE notes (
                        note_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ref_code TEXT UNIQUE NOT NULL,
                        guild_id INTEGER NOT NULL,
                        discord_id INTEGER,
                        mc_user_id TEXT,
                        note_text TEXT NOT NULL,
                        author_id INTEGER NOT NULL,
                        author_name TEXT NOT NULL,
                        infraction_ref TEXT,
                        created_at INTEGER NOT NULL
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO notes (
                        ref_code, guild_id, discord_id, mc_user_id, note_text,
                        author_id, author_name, infraction_ref, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "N2026-000001",
                        1,
                        123,
                        "456",
                        "Legacy note",
                        999,
                        "Admin",
                        None,
                        1_800_000_000,
                    ),
                )
                connection.commit()
                connection.close()

                database = MemberDatabase(str(db_path))
                await database.initialize()
                try:
                    notes = await database.get_notes(discord_id=123, mc_user_id="456")
                finally:
                    await database.close()

            return notes

        notes = asyncio.run(run_test())

        self.assertEqual(len(notes), 1)
        self.assertEqual(notes[0]["note_text"], "Legacy note")
        self.assertEqual(notes[0]["status"], "active")
        self.assertEqual(notes[0]["is_pinned"], 0)

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
        self.assertEqual(
            fake_db.event["event_data"],
            {"ref_code": "N2026-000001", "status": "pinned"},
        )
        view._update_view.assert_awaited_once_with(interaction)

    def test_add_note_records_note_preview_in_audit_event(self):
        class FakeDB:
            def __init__(self):
                self.event = None

            async def add_note(self, **kwargs):
                self.note = kwargs
                return "N2026-000002"

            async def add_event(self, **kwargs):
                self.event = kwargs

        fake_db = FakeDB()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456")
        view.member_data.notes_count = 0
        view.db = fake_db
        view._update_view = AsyncMock()

        modal = AddNoteModal(view)
        modal.note_text.value = "This is a staff note"
        modal.infraction_ref.value = ""
        modal.expires_days.value = ""

        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(fake_db.event["event_type"], "note_created")
        self.assertEqual(
            fake_db.event["event_data"],
            {"ref_code": "N2026-000002", "note": "This is a staff note"},
        )
        self.assertEqual(view.member_data.notes_count, 1)
        view._update_view.assert_awaited_once_with(interaction)

    def test_edit_note_records_old_and_new_note_preview_in_audit_event(self):
        class FakeDB:
            def __init__(self):
                self.event = None
                self.updated = None

            async def get_notes(self, **kwargs):
                return [
                    {
                        "ref_code": "N2026-000003",
                        "discord_id": 123,
                        "mc_user_id": "456",
                        "note_text": "Old note text",
                    }
                ]

            async def update_note(self, **kwargs):
                self.updated = kwargs
                return True

            async def add_event(self, **kwargs):
                self.event = kwargs

        fake_db = FakeDB()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456")
        view.db = fake_db
        view._update_view = AsyncMock()

        modal = EditNoteModal(view)
        modal.ref_code.value = "N2026-000003"
        modal.new_text.value = "Updated note text"

        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(fake_db.event["event_type"], "note_edited")
        self.assertEqual(
            fake_db.event["event_data"],
            {
                "ref_code": "N2026-000003",
                "old_value": "Old note text",
                "new_value": "Updated note text",
            },
        )
        view._update_view.assert_awaited_once_with(interaction)

    def test_delete_note_records_note_preview_in_audit_event(self):
        class FakeDB:
            def __init__(self):
                self.event = None
                self.deleted = None

            async def get_notes(self, **kwargs):
                return [
                    {
                        "ref_code": "N2026-000004",
                        "discord_id": 123,
                        "mc_user_id": "456",
                        "note_text": "Note to remove",
                    }
                ]

            async def delete_note(self, ref_code):
                self.deleted = ref_code
                return True

            async def add_event(self, **kwargs):
                self.event = kwargs

        fake_db = FakeDB()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456")
        view.member_data.notes_count = 1
        view.db = fake_db
        view._update_view = AsyncMock()

        modal = DeleteNoteModal(view)
        modal.ref_code.value = "N2026-000004"
        modal.confirm.value = "DELETE"

        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(fake_db.deleted, "N2026-000004")
        self.assertEqual(fake_db.event["event_type"], "note_deleted")
        self.assertEqual(
            fake_db.event["event_data"],
            {"ref_code": "N2026-000004", "note": "Note to remove"},
        )
        self.assertEqual(view.member_data.notes_count, 0)
        view._update_view.assert_awaited_once_with(interaction)


if __name__ == "__main__":
    unittest.main()
