from .sanction_manager import SanctionsManager


async def setup(bot):
    """Load SanctionsManager cog."""
    await bot.add_cog(SanctionsManager(bot))
