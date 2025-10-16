from .members_scraper import MembersScraper

async def setup(bot):
    await bot.add_cog(MembersScraper(bot))
