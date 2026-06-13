from .announcementpanel import AnnouncementPanel


async def setup(bot):
    await bot.add_cog(AnnouncementPanel(bot))
