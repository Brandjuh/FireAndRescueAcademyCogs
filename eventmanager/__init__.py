from .event_manager import EventManager


async def setup(bot):
    await bot.add_cog(EventManager(bot))
