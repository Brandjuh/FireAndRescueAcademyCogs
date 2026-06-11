async def setup(bot):
    from .buildings_scraper import BuildingsScraper

    await bot.add_cog(BuildingsScraper(bot))
