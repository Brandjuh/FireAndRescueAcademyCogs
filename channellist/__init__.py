from .channellist import ChannelList


async def setup(bot):
    """Load the ChannelList cog."""
    await bot.add_cog(ChannelList(bot))
