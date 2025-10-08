from .buildingmanager import BuildingManager

__red_end_user_data_statement__ = (
    "This cog stores user IDs, usernames, and building request data "
    "in a SQLite database for statistics and logging purposes. "
    "No other personal data is stored."
)


async def setup(bot):
    await bot.add_cog(BuildingManager(bot))
