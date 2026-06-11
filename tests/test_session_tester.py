import asyncio
import types
import unittest
from unittest.mock import AsyncMock

from session_tester.session_tester import SessionTester


class SessionTesterTests(unittest.TestCase):
    def setUp(self):
        self.session = types.SimpleNamespace(
            get=AsyncMock(),
            close=AsyncMock(),
        )
        self.cookie_manager = types.SimpleNamespace(
            get_session=AsyncMock(return_value=self.session),
        )
        self.bot = types.SimpleNamespace(
            get_cog=lambda name: self.cookie_manager if name == "CookieManager" else None,
        )
        self.ctx = types.SimpleNamespace(send=AsyncMock())
        self.tester = SessionTester(self.bot)

    def test_success_does_not_close_shared_session(self):
        response = types.SimpleNamespace(
            status=200,
            url="https://www.missionchief.com/buildings",
            text=AsyncMock(return_value="page"),
        )
        self.session.get.return_value = response

        asyncio.run(self.tester.cookietest(self.ctx, url="https://www.missionchief.com/buildings"))

        self.session.close.assert_not_awaited()
        self.assertIn("GET https://www.missionchief.com/buildings -> 200", self.ctx.send.await_args.args[0])

    def test_failure_does_not_close_shared_session(self):
        self.session.get.side_effect = RuntimeError("request failed")

        asyncio.run(self.tester.cookietest(self.ctx, url="https://www.missionchief.com/buildings"))

        self.session.close.assert_not_awaited()
        self.assertEqual("Request failed: request failed", self.ctx.send.await_args.args[0])


if __name__ == "__main__":
    unittest.main()
