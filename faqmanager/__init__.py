from .faqmanager import FAQManager

async def setup(bot):
    await bot.add_cog(FAQManager(bot))
