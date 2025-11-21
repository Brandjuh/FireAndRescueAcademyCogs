"""
RapidResponse - A MissionChief guessing game for Discord
Author: BrandjuhNL
"""

from .rapidresponse import RapidResponse


async def setup(bot):
    """Load the RapidResponse cog."""
    await bot.add_cog(RapidResponse(bot))
