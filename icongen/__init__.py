"""
IconGen cog initialization
"""

from .icongen import IconGen

async def setup(bot):
    """Load IconGen cog"""
    cog = IconGen(bot)
    await bot.add_cog(cog)
