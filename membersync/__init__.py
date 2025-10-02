from .membersync import MemberSync

async def setup(bot):
    await bot.add_cog(MemberSync(bot))