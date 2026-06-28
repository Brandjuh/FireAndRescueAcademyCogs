"""FireStationCommander Red cog entrypoint."""

from .firestationcommander import FireStationCommander


async def setup(bot) -> None:
    """Load the FireStationCommander cog."""
    await bot.add_cog(FireStationCommander(bot))
