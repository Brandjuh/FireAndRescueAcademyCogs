import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from MemberManager.membermanager import DEFAULTS, MemberManager


class MemberManagerEntrypointTests(unittest.TestCase):
    def test_default_panel_channel_is_configured(self):
        self.assertEqual(DEFAULTS["panel_channel_id"], 1426226521231589507)
        self.assertIsNone(DEFAULTS["panel_message_id"])

    def test_missionchief_search_resolves_exact_id(self):
        cog = MemberManager.__new__(MemberManager)
        cog.alliance_scraper = None
        cog._get_mc_data = AsyncMock(
            return_value={
                "user_id": "12345",
                "name": "DutchFireFighter",
            }
        )

        results = asyncio.run(cog._search_missionchief_members("12345"))

        self.assertEqual(results[0]["mc_user_id"], "12345")
        self.assertEqual(results[0]["name"], "DutchFireFighter")

    def test_missionchief_search_resolves_name_from_alliance_scraper(self):
        cog = MemberManager.__new__(MemberManager)
        cog.alliance_scraper = types.SimpleNamespace(
            get_members=AsyncMock(
                return_value=[
                    {"user_id": "111", "name": "Alpha"},
                    {"user_id": "222", "name": "DutchFireFighter"},
                ]
            )
        )
        cog._get_mc_data = AsyncMock(return_value=None)

        results = asyncio.run(cog._search_missionchief_members("Dutch"))

        self.assertEqual(results[0]["mc_user_id"], "222")
        self.assertEqual(results[0]["name"], "DutchFireFighter")

    def test_send_panel_message_stores_message_id(self):
        stored = {}
        cog = MemberManager.__new__(MemberManager)
        cog.config = types.SimpleNamespace(
            panel_message_id=types.SimpleNamespace(
                set=AsyncMock(side_effect=lambda value: stored.update({"id": value}))
            )
        )
        channel = types.SimpleNamespace(
            send=AsyncMock(return_value=types.SimpleNamespace(id=98765))
        )

        message = asyncio.run(cog._send_panel_message(channel))

        self.assertEqual(message.id, 98765)
        self.assertEqual(stored["id"], 98765)
        channel.send.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
