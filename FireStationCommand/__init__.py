
from .fire_station_command import FireStationCommand

async def setup(bot):
    await bot.add_cog(FireStationCommand(bot))
