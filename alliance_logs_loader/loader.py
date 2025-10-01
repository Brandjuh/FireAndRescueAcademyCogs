# loader.py v0.0.1
from __future__ import annotations
from redbot.core import commands

__version__ = "0.0.1"

class AllianceLogsLoader(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.group(name="alogloader")
    async def alogloader(self, ctx: commands.Context):
        """Sanity loader group."""

    @alogloader.command(name="ping")
    async def ping(self, ctx: commands.Context):
        await ctx.send(f"Pong. Loader {__version__} is loaded.")

async def setup(bot):
    await bot.add_cog(AllianceLogsLoader(bot))
