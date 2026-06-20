from .chat_manager import ChatManager


async def setup(bot):
    await bot.add_cog(ChatManager(bot))
