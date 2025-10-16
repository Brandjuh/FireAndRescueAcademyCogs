from .buildings_scraper import BuildingsScraper

async def setup(bot):
    await bot.add_cog(BuildingsScraper(bot))
