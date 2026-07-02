async def setup(bot):
    from .faqmanager import FAQManager

    await bot.add_cog(FAQManager(bot))
