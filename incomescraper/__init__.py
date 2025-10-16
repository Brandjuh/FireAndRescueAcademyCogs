from .income_scraper import IncomeScraper

async def setup(bot):
    await bot.add_cog(IncomeScraper(bot))
