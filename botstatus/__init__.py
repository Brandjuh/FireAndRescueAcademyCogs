from .botstatus import BotStatus


async def setup(bot):
    await bot.add_cog(BotStatus(bot))
