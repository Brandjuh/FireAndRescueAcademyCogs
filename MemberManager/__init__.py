"""
MemberManager - Comprehensive Member Management System
Fire & Rescue Academy Alliance

Author: FireAndRescueAcademy
Version: 1.0.0
"""

from .membermanager import MemberManager, setup

__red_end_user_data_statement__ = (
    "This cog stores member information including Discord IDs, MissionChief IDs, "
    "notes, infractions, and audit trails. Data can be deleted upon user request."
)

__version__ = "1.0.0"
__author__ = "FireAndRescueAcademy"

async def setup(bot):
    """Load MemberManager cog."""
    from .membermanager import MemberManager
    await bot.add_cog(MemberManager(bot))
