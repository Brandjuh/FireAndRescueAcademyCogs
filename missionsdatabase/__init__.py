from .missions_database import MissionsDatabase


async def setup(bot):
    """Load MissionsDatabase cog."""
    await bot.add_cog(MissionsDatabase(bot))
