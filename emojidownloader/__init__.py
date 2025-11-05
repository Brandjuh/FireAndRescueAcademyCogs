from .emojidownloader import EmojiDownloader


async def setup(bot):
    """Load the EmojiDownloader cog."""
    cog = EmojiDownloader(bot)
    await bot.add_cog(cog)
    await cog.cog_load()
