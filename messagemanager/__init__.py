from .message_manager import MessageManager


async def setup(bot):
    await bot.add_cog(MessageManager(bot))
