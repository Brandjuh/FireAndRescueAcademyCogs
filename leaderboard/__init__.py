from .leaderboard import Leaderboard


async def setup(bot):
    """Load Leaderboard cog."""
    await bot.add_cog(Leaderboard(bot))
