from .data_overview import DataOverview

async def setup(bot):
    await bot.add_cog(DataOverview(bot))
