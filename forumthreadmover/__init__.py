from .forumthreadmover import ForumThreadMover

async def setup(bot):
    """Load ForumThreadMover cog."""
    await bot.add_cog(ForumThreadMover(bot))
