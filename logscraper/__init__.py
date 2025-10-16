from .logs_scraper import LogsScraper

async def setup(bot):
    await bot.add_cog(LogsScraper(bot))
