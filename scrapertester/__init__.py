from .scraper_tester import ScraperTester


async def setup(bot):
    await bot.add_cog(ScraperTester(bot))
