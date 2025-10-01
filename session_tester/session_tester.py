# session_tester.py (unchanged)
from __future__ import annotations
from redbot.core import commands, checks

class SessionTester(commands.Cog):
    """Session tester for CookieManager."""

    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="cookietest")
    @checks.is_owner()
    async def cookietest(self, ctx: commands.Context, url: str = None):
        """Fetch a URL using CookieManager session. Default is CookieManager's check_url."""
        cookie_cog = self.bot.get_cog("CookieManager")
        if not cookie_cog:
            await ctx.send("CookieManager cog not loaded.")
            return
        if url is None:
            url = await cookie_cog.config.check_url()
        session = await cookie_cog.get_session()
        try:
            r = await session.get(url, allow_redirects=True)
            text = await r.text()
            final_url = str(r.url)
            await session.close()
            await ctx.send(f"GET {url} -> {r.status}; final={final_url}; length={len(text)}")
        except Exception as e:
            await ctx.send(f"Request failed: {e}")

async def setup(bot):
    await bot.add_cog(SessionTester(bot))
