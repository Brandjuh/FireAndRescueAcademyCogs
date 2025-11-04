from .emojilist import EmojiList


async def setup(bot):
    """Load the EmojiList cog."""
    await bot.add_cog(EmojiList(bot))
