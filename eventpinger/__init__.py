from .eventpinger import EventPinger


async def setup(bot):
    await bot.add_cog(EventPinger(bot))
