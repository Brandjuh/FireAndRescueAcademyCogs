from .roleexporter import RoleExporter


async def setup(bot):
    await bot.add_cog(RoleExporter(bot))
