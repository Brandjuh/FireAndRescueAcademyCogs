"""
Incident Roulette Cog for Red-Discord Bot

A skill-based emergency response allocation game with economy integration.
Full implementation with all features from specification v2.0
"""

from .incidentroulette import IncidentRoulette

__red_end_user_data_statement__ = (
    "This cog stores user gameplay statistics, scores, and economy transactions. "
    "Data includes: active runs, score history (last 50), daily/weekly play limits, "
    "and payout tracking. All data is stored per-user per-guild."
)

__version__ = "2.0.0"
__author__ = "Brandjuh"


async def setup(bot):
    """Load the IncidentRoulette cog"""
    await bot.add_cog(IncidentRoulette(bot))
