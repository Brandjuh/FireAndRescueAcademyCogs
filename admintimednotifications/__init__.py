from .admintimednotifications import AdminTimedNotifications


async def setup(bot):
    await bot.add_cog(AdminTimedNotifications(bot))
