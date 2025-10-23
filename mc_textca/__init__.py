# mc_textcad: MissionChief-style Text CAD for Red-Discord-Bot
from redbot.core import commands, Config
from redbot.core.bot import Red
from .economy import EconomyBridge
from .cad import TextCAD
from .rp import RPRolls

__red_end_user_data_statement__ = (
    "This cog stores per-user units and statuses, incident participation, and leaderboards. "
    "It stores per-guild incident threads, configuration, and audit logs. No message content beyond state."
)

class McTextCAD(commands.Cog):
    """MissionChief Text CAD — dynamic incidents, per-unit status, payouts, leaderboards, and RP checks."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xCADA5515, force_registration=True)
        self.econ = EconomyBridge()
        self.cad = TextCAD(bot, self.config, self.econ)
        self.rp = RPRolls(bot, self.config, self.econ, self.cad)

    async def cog_unload(self):
        await self.cad.stop_all_loops()

async def setup(bot: Red):
    cog = McTextCAD(bot)
    await bot.add_cog(cog)
    await cog.cad.post_init()
