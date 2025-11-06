from .missions_database import MissionsDatabase


async def setup(bot):
    """Load MissionsDatabase cog."""
    cog = MissionsDatabase(bot)
    await bot.add_cog(cog)
