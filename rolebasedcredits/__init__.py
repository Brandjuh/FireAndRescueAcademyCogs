from .rolebasedcredits import RoleBasedCredits


async def setup(bot):
    await bot.add_cog(RoleBasedCredits(bot))
