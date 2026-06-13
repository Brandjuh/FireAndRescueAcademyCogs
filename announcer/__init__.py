from .announcer import Announcer


async def setup(bot):
    await bot.add_cog(Announcer(bot))
