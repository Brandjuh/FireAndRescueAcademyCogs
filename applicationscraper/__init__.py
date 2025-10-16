from .applications_scraper import ApplicationsScraper

async def setup(bot):
    await bot.add_cog(ApplicationsScraper(bot))
