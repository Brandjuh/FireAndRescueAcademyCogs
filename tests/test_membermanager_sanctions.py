import asyncio
import importlib.util
import tempfile
import types
import unittest
from pathlib import Path

from MemberManager.models import MemberData
from MemberManager.views import MemberOverviewView


def load_sanctions_database_class():
    module_path = Path(__file__).resolve().parents[1] / "sanctionmanager" / "sanction_manager.py"
    spec = importlib.util.spec_from_file_location("sanction_manager_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.SanctionsDatabase


class MemberManagerSanctionsTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
