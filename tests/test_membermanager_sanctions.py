import asyncio
import importlib
import importlib.util
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

from MemberManager.models import MemberData
from MemberManager.views import (
    CreateSanctionModal,
    EditSanctionModal,
    MemberOverviewView,
    RemoveSanctionModal,
)


def load_sanctions_database_class():
    module_path = Path(__file__).resolve().parents[1] / "sanctionmanager" / "sanction_manager.py"
    spec = importlib.util.spec_from_file_location("sanction_manager_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SanctionsDatabase


def load_sanctions_manager_class():
    module_path = Path(__file__).resolve().parents[1] / "sanctionmanager" / "sanction_manager.py"
    spec = importlib.util.spec_from_file_location("sanction_manager_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SanctionsManager


class MemberManagerSanctionsTests(unittest.TestCase):
    def test_sanctionmanager_package_exports_loadable_cog(self):
        module = importlib.import_module("sanctionmanager")

        self.assertTrue(hasattr(module, "SanctionsManager"))

    def test_sanction_database_matches_discord_and_mc_ids_together(self):
        SanctionsDatabase = load_sanctions_database_class()

        with tempfile.TemporaryDirectory() as temp_dir:
            database = SanctionsDatabase(str(Path(temp_dir) / "sanctions.db"))
            database.add_sanction(
                guild_id=1,
                discord_user_id=123,
                mc_user_id=None,
                mc_username="Discord Only",
                admin_user_id=999,
                admin_username="Admin",
                sanction_type="Warning - Official 1st",
                reason_category="Conduct",
                reason_detail="Discord-linked sanction",
                additional_notes=None,
            )
            database.add_sanction(
                guild_id=1,
                discord_user_id=None,
                mc_user_id="456",
                mc_username="MCUser",
                admin_user_id=999,
                admin_username="Admin",
                sanction_type="Warning - Official 2nd",
                reason_category="Conduct",
                reason_detail="MC-linked sanction",
                additional_notes=None,
            )

            sanctions = database.get_user_sanctions(
                guild_id=1,
                discord_user_id=123,
                mc_user_id="456",
            )
            warnings = database.get_active_warnings(
                guild_id=1,
                discord_user_id=123,
                mc_user_id="456",
            )

        self.assertEqual(len(sanctions), 2)
        self.assertEqual(len(warnings), 2)
        self.assertEqual(
            {sanction["reason_detail"] for sanction in sanctions},
            {"Discord-linked sanction", "MC-linked sanction"},
        )

    def test_sanctions_embed_uses_view_guild_without_message_object(self):
        class FakeSanctionDB:
            def __init__(self):
                self.call = None

            def get_user_sanctions(self, **kwargs):
                self.call = kwargs
                return [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning - Official 1st",
                        "reason_category": "Conduct",
                        "reason_detail": "Needs attention",
                        "admin_username": "Admin",
                        "created_at": 1_800_000_000,
                        "status": "active",
                    }
                ]

        sanction_db = FakeSanctionDB()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
        )
        view.integrations = {"sanction_manager": types.SimpleNamespace(db=sanction_db)}
        view.guild = types.SimpleNamespace(id=1)
        view.message = None
        view.infraction_page = 0
        view.infractions_per_page = 5

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Active Sanction", embed.kwargs["title"])
        self.assertEqual(
            sanction_db.call,
            {
                "guild_id": 1,
                "discord_user_id": 123,
                "mc_user_id": "456",
            },
        )

    def test_sanctions_embed_prefers_public_sanction_contract(self):
        class FakeSanctionManager:
            def __init__(self):
                self.call = None
                self.db = types.SimpleNamespace(
                    get_user_sanctions=lambda **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used"))
                )

            def get_member_sanctions(self, **kwargs):
                self.call = kwargs
                return [
                    {
                        "sanction_id": 1,
                        "sanction_type": "Warning - Official 1st",
                        "reason_category": "Conduct",
                        "reason_detail": "Needs attention",
                        "admin_username": "Admin",
                        "created_at": 1_800_000_000,
                        "status": "active",
                    }
                ]

        sanction_manager = FakeSanctionManager()
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(
            discord_id=123,
            mc_user_id="456",
            discord_username="DiscordUser",
            mc_username="MCUser",
        )
        view.integrations = {"sanction_manager": sanction_manager}
        view.guild = types.SimpleNamespace(id=1)
        view.infraction_page = 0
        view.infractions_per_page = 5

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Active Sanction", embed.kwargs["title"])
        self.assertEqual(
            sanction_manager.call,
            {
                "guild_id": 1,
                "discord_user_id": 123,
                "mc_user_id": "456",
            },
        )

    def test_sanction_manager_public_contract_delegates_to_database(self):
        SanctionsManager = load_sanctions_manager_class()
        calls = {}

        class FakeDB:
            def get_sanction(self, sanction_id):
                calls["get_one"] = sanction_id
                return {"sanction_id": sanction_id}

            def get_user_sanctions(self, **kwargs):
                calls["get"] = kwargs
                return [{"sanction_id": 1}]

            def add_sanction(self, **kwargs):
                calls["add"] = kwargs
                return 42

            def edit_sanction(self, sanction_id, admin_user_id, **updates):
                calls["edit"] = (sanction_id, admin_user_id, updates)

            def update_sanction_status(self, sanction_id, status, admin_user_id, notes):
                calls["remove"] = (sanction_id, status, admin_user_id, notes)

        manager = SanctionsManager.__new__(SanctionsManager)
        manager.db = FakeDB()

        sanction = manager.get_sanction_by_id(42)
        sanctions = manager.get_member_sanctions(
            guild_id=1,
            discord_user_id=123,
            mc_user_id="456",
        )
        sanction_id = manager.create_sanction_for_member(
            guild_id=1,
            discord_user_id=123,
            mc_user_id="456",
            mc_username="MCUser",
            admin_user_id=999,
            admin_username="Admin",
            sanction_type="Warning",
            reason_category="Conduct",
            reason_detail="Reason",
            additional_notes="Notes",
        )
        manager.edit_member_sanction(42, admin_user_id=999, reason_detail="Updated")
        manager.remove_member_sanction(42, admin_user_id=999, notes="Resolved")

        self.assertEqual(sanction, {"sanction_id": 42})
        self.assertEqual(calls["get_one"], 42)
        self.assertEqual(sanctions, [{"sanction_id": 1}])
        self.assertEqual(sanction_id, 42)
        self.assertEqual(calls["get"]["mc_user_id"], "456")
        self.assertEqual(calls["add"]["sanction_type"], "Warning")
        self.assertEqual(calls["edit"], (42, 999, {"reason_detail": "Updated"}))
        self.assertEqual(calls["remove"], (42, "removed", 999, "Resolved"))

    def test_sanction_manager_records_membermanager_audit_when_available(self):
        SanctionsManager = load_sanctions_manager_class()
        add_event = AsyncMock()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(
            get_cog=lambda name: types.SimpleNamespace(db=types.SimpleNamespace(add_event=add_event))
            if name == "MemberManager"
            else None
        )

        asyncio.run(
            manager._record_membermanager_sanction_event(
                guild_id=1,
                sanction={
                    "sanction_id": 42,
                    "discord_user_id": 123,
                    "mc_user_id": "456",
                    "sanction_type": "Warning",
                },
                event_type="sanction_added",
                actor_id=999,
                event_data={"source": "SanctionManager"},
            )
        )

        add_event.assert_awaited_once_with(
            guild_id=1,
            discord_id=123,
            mc_user_id="456",
            event_type="sanction_added",
            event_data={
                "sanction_id": 42,
                "sanction_type": "Warning",
                "source": "SanctionManager",
            },
            triggered_by="sanctionmanager",
            actor_id=999,
        )

    def test_sanction_manager_audit_hook_is_noop_without_membermanager(self):
        SanctionsManager = load_sanctions_manager_class()
        manager = SanctionsManager.__new__(SanctionsManager)
        manager.bot = types.SimpleNamespace(get_cog=lambda name: None)

        asyncio.run(
            manager._record_membermanager_sanction_event(
                guild_id=1,
                sanction={"sanction_id": 42},
                event_type="sanction_removed",
                actor_id=999,
            )
        )

    def test_create_sanction_modal_uses_public_contract(self):
        calls = {}

        class FakeSanctionManager:
            db = types.SimpleNamespace(
                add_sanction=lambda **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used"))
            )

            def create_sanction_for_member(self, **kwargs):
                calls["create"] = kwargs
                return 42

        parent = types.SimpleNamespace(
            integrations={"sanction_manager": FakeSanctionManager()},
            member_data=MemberData(
                discord_id=123,
                mc_user_id="456",
                discord_username="DiscordUser",
                mc_username="MCUser",
            ),
            db=types.SimpleNamespace(add_event=AsyncMock()),
            _update_view=AsyncMock(),
        )
        modal = CreateSanctionModal(parent)
        modal.sanction_type.value = "Warning"
        modal.reason_category.value = "Conduct"
        modal.reason_detail.value = "Reason"
        modal.admin_notes.value = "Internal"
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        asyncio.run(modal.on_submit(interaction))

        self.assertEqual(calls["create"]["discord_user_id"], 123)
        self.assertEqual(calls["create"]["mc_user_id"], "456")
        self.assertEqual(calls["create"]["sanction_type"], "Warning")
        parent.db.add_event.assert_awaited_once()
        parent._update_view.assert_awaited_once_with(interaction)

    def test_edit_and_remove_sanction_modals_use_public_contracts(self):
        calls = {}

        class FakeSanctionManager:
            db = types.SimpleNamespace(
                get_sanction=lambda sanction_id: (_ for _ in ()).throw(AssertionError("db fallback used")),
                edit_sanction=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("db fallback used")),
                update_sanction_status=lambda *args, **kwargs: (_ for _ in ()).throw(
                    AssertionError("db fallback used")
                ),
            )

            def get_sanction_by_id(self, sanction_id):
                calls.setdefault("get", []).append(sanction_id)
                return {"sanction_id": sanction_id, "guild_id": 1}

            def edit_member_sanction(self, sanction_id, *, admin_user_id, **updates):
                calls["edit"] = (sanction_id, admin_user_id, updates)

            def remove_member_sanction(self, sanction_id, *, admin_user_id, notes=None):
                calls["remove"] = (sanction_id, admin_user_id, notes)

        parent = types.SimpleNamespace(
            integrations={"sanction_manager": FakeSanctionManager()},
            member_data=MemberData(discord_id=123, mc_user_id="456"),
            db=types.SimpleNamespace(add_event=AsyncMock()),
            _update_view=AsyncMock(),
        )
        interaction = types.SimpleNamespace(
            guild=types.SimpleNamespace(id=1),
            user=types.SimpleNamespace(id=999, __str__=lambda self: "Admin"),
            response=types.SimpleNamespace(send_message=AsyncMock()),
        )

        edit_modal = EditSanctionModal(parent)
        edit_modal.sanction_id.value = "42"
        edit_modal.new_reason.value = "Updated reason"
        edit_modal.new_notes.value = ""
        asyncio.run(edit_modal.on_submit(interaction))

        remove_modal = RemoveSanctionModal(parent)
        remove_modal.sanction_id.value = "42"
        remove_modal.reason.value = "Resolved"
        remove_modal.confirm.value = "REMOVE"
        asyncio.run(remove_modal.on_submit(interaction))

        self.assertEqual(calls["get"], [42, 42])
        self.assertEqual(calls["edit"], (42, 999, {"reason_detail": "Updated reason"}))
        self.assertEqual(calls["remove"][0], 42)
        self.assertEqual(calls["remove"][1], 999)
        self.assertIn("Resolved", calls["remove"][2])

    def test_sanctions_embed_has_quiet_fallback_without_backend(self):
        view = MemberOverviewView.__new__(MemberOverviewView)
        view.member_data = MemberData(discord_id=123, mc_user_id="456", discord_username="DiscordUser")
        view.integrations = {"sanction_manager": None}
        view.guild = types.SimpleNamespace(id=1)

        embed = asyncio.run(view.get_infractions_embed())

        self.assertIn("Sanction data is currently unavailable", embed.description)
        self.assertNotIn("SanctionManager not available", embed.description)


if __name__ == "__main__":
    unittest.main()
