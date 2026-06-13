"""
Daily Member Report Template
Generates the complete daily member report.
"""

import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import discord

from ..data_aggregator import DataAggregator
from ..embed_formatter import EmbedFormatter

log = logging.getLogger("red.FARA.AllianceReports.DailyMember")


class DailyMemberReport:
    """Generates daily member reports."""
    
    def __init__(self, bot, config_manager):
        """Initialize report generator."""
        self.bot = bot
        self.config_manager = config_manager
        self.aggregator = DataAggregator(config_manager, bot)
    
    async def generate(self) -> Optional[discord.Embed]:
        """
        Generate daily member report embed.
        
        Returns:
            Discord embed or None if error
        """
        try:
            log.info("Generating daily member report...")
            tz_str = await self.config_manager.config.timezone()
            now = datetime.now(ZoneInfo(tz_str))
            
            # Get daily data
            data = await self.aggregator.get_daily_data()
            
            # Create embed
            embed = EmbedFormatter.create_daily_member_embed(
                data,
                now=now,
                timezone_label=tz_str,
            )
            
            log.info("Daily member report generated")
            return embed
            
        except Exception as e:
            log.exception(f"Error generating daily member report: {e}")
            return None
    
    async def post(self, channel: discord.TextChannel) -> bool:
        """
        Generate and post daily member report.
        
        Args:
            channel: Channel to post to
        
        Returns:
            True if successful, False otherwise
        """
        try:
            embed = await self.generate()
            
            if not embed:
                log.error("Failed to generate embed")
                return False
            
            # Check if test mode
            test_mode = await self.config_manager.config.test_mode()
            if test_mode:
                log.info("Test mode enabled - report not posted")
                log.info(f"Would post to channel: {channel.name} ({channel.id})")
                return True
            
            # Post to channel
            await channel.send(embed=embed)
            log.info(f"Daily member report posted to {channel.name}")
            return True
            
        except discord.Forbidden:
            log.error(f"No permission to post in {channel.name}")
            return False
        except Exception as e:
            log.exception(f"Error posting daily member report: {e}")
            return False
