"""
MissionsDatabase cog for Red-DiscordBot
"""

from .missionsdatabase import MissionsDatabase


async def setup(bot):
    """Load the MissionsDatabase cog."""
    cog = MissionsDatabase(bot)
    await bot.add_cog(cog)
