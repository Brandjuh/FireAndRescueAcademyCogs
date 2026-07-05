from .rolerenamer import RoleRenamer


async def setup(bot):
    await bot.add_cog(RoleRenamer(bot))
