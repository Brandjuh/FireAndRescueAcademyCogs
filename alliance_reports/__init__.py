"""
AllianceReports - Comprehensive reporting system for Fire & Rescue Academy
Generates daily and monthly reports from alliance data.

Author: FireAndRescueAcademy
Version: 1.0.0
"""

from .alliance_reports import AllianceReports

async def setup(bot):
    """Load the AllianceReports cog."""
    cog = AllianceReports(bot)
    await bot.add_cog(cog)
