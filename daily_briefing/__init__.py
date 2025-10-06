from .daily_briefing import DailyBriefing

async def setup(bot):
    await bot.add_cog(DailyBriefing(bot))
