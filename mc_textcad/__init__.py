# mc_textcad/__init__.py
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
    """Container cog that wires up TextCAD and RPRolls."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xCADA5515, force_registration=True)
        self.econ = EconomyBridge()
        # subcogs (declare but don't register here)
        self.cad = TextCAD(bot, self.config, self.econ)
        self.rp = RPRolls(bot, self.config, self.econ, self.cad)

    async def cog_unload(self):
        # make sure background loops are stopped
        await self.cad.stop_all_loops()

async def setup(bot: Red):
    # create the container
    container = McTextCAD(bot)
    # add the *real* cogs so their commands exist
    await bot.add_cog(container)
    await bot.add_cog(container.cad)
    await bot.add_cog(container.rp)
    # start background tasks
    await container.cad.post_init()
