from .applications_scraper import ApplicationsScraper
from .fixture_capture_cog import ApplicationsFixtureCapture

async def setup(bot):
    await bot.add_cog(ApplicationsScraper(bot))
    await bot.add_cog(ApplicationsFixtureCapture(bot))
