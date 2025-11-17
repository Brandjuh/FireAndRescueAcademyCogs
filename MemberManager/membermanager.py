"""
MemberManager - Comprehensive member tracking and management
Fire & Rescue Academy Alliance

VERSION: 2.2.3 - ULTRA VERBOSE DEBUG
Every step is sent to Discord for debugging
"""

from __future__ import annotations
import asyncio
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

import discord
from discord import app_commands
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path
from redbot.core.utils.chat_formatting import box, pagify

from .database import MemberDatabase
from .models import MemberData, NoteData, InfractionData
from .views import MemberOverviewView
from .utils import fuzzy_search_member, format_contribution_trend
from .automation import ContributionMonitor
from .config_commands import ConfigCommands

log = logging.getLogger("red.FARA.MemberManager")

__version__ = "2.2.3-DEBUG"

DEFAULTS = {
    "contribution_threshold": 5.0,
    "contribution_trend_weeks": 3,
    "auto_contribution_alert": True,
    "auto_role_drift_check": True,
    "admin_alert_channel": None,
    "modlog_channel": None,
    "admin_role_ids": [],
    "moderator_role_ids": [],
    "note_expiry_days": 90,
    "dormancy_threshold_days": 30,
}


class MemberManager(ConfigCommands, commands.Cog):
    """Member Management System for Fire & Rescue Academy."""
    
    __version__ = __version__
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(
            self,
            identifier=0xFA11A9E5,
            force_registration=True
        )
        self.config.register_global(**DEFAULTS)
        
        self.data_path = cog_data_path(self)
        self.db_path = self.data_path / "membermanager.db"
        self.db: Optional[MemberDatabase] = None
        
        self.membersync: Optional[commands.Cog] = None
        self.alliance_scraper: Optional[commands.Cog] = None
        self.members_scraper: Optional[commands.Cog] = None
        self.logs_scraper: Optional[commands.Cog] = None
        self.sanction_manager: Optional[commands.Cog] = None
        
        self.contribution_monitor: Optional[ContributionMonitor] = None
        self._automation_task: Optional[asyncio.Task] = None
        
        self._register_views()
    
    async def cog_load(self) -> None:
        """Initialize cog on load."""
        log.info(f"MemberManager v{__version__} loading...")
        
        self.db = MemberDatabase(str(self.db_path))
        await self.db.initialize()
        log.info("Database initialized")
        
        await self._connect_integrations()
        asyncio.create_task(self._delayed_start())
        
        log.info("MemberManager loaded successfully")
    
    async def cog_unload(self) -> None:
        """Cleanup on cog unload."""
        log.info("MemberManager unloading...")
        
        if self._automation_task:
            self._automation_task.cancel()
            try:
                await self._automation_task
            except asyncio.CancelledError:
                pass
        
        if self.db:
            await self.db.close()
        
        log.info("MemberManager unloaded")
    
    def _register_views(self):
        pass
    
    async def _delayed_start(self):
        """Start automation after bot is ready."""
        await self.bot.wait_until_ready()
        await asyncio.sleep(5)
        
        if await self.config.auto_contribution_alert():
            self.contribution_monitor = ContributionMonitor(
                self.bot,
                self.db,
                self.config,
                self.alliance_scraper
            )
            self._automation_task = asyncio.create_task(
                self.contribution_monitor.run()
            )
            log.info("Contribution monitoring started")
    
    async def _connect_integrations(self):
        """Detect and connect to other cogs."""
        self.membersync = self.bot.get_cog("MemberSync")
        self.alliance_scraper = self.bot.get_cog("AllianceScraper")
        self.members_scraper = self.bot.get_cog("MembersScraper")
        self.logs_scraper = self.bot.get_cog("LogsScraper")
        self.sanction_manager = self.bot.get_cog("SanctionManager")
        
        integrations = []
        if self.membersync:
            integrations.append("MemberSync")
        if self.alliance_scraper:
            integrations.append("AllianceScraper")
        if self.members_scraper:
            integrations.append("MembersScraper")
        if self.logs_scraper:
            integrations.append("LogsScraper")
        if self.sanction_manager:
            integrations.append("SanctionManager")
        
        if integrations:
            log.info(f"Connected to: {', '.join(integrations)}")
        else:
            log.warning("No integrations found")
    
    async def _is_admin(self, member: discord.Member) -> bool:
        """Check if member has admin permissions."""
        if member.guild_permissions.administrator:
            return True
        
        admin_roles = await self.config.admin_role_ids()
        return any(role.id in admin_roles for role in member.roles)
    
    async def _is_moderator(self, member: discord.Member) -> bool:
        """Check if member has moderator permissions."""
        if await self._is_admin(member):
            return True
        
        mod_roles = await self.config.moderator_role_ids()
        return any(role.id in mod_roles for role in member.roles)
    
    @commands.hybrid_group(name="member", fallback="help")
    @commands.guild_only()
    async def member(self, ctx: commands.Context):
        """Member management commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @member.command(name="checkcontributions", aliases=["checkcontrib"])
    @commands.guild_only()
    async def check_contributions(
        self,
        ctx: commands.Context,
        target: Optional[str] = None,
        *,
        flags: str = ""
    ):
        """
        🔍 Debug tool: Check who qualifies for low contribution alerts.
        
        **Usage:**
        - `[p]member checkcontrib @user` - Check specific member with VERBOSE debug
        """
        if not await self._is_admin(ctx.author):
            await ctx.send("❌ You need admin permissions to use this command.")
            return
        
        await ctx.typing()
        
        if not self.members_scraper:
            await ctx.send("❌ **MembersScraper not available**")
            return
        
        threshold = await self.config.contribution_threshold()
        
        if target and not target.startswith("--"):
            await self._check_single_member_verbose(ctx, target, threshold)
            return
        
        await ctx.send("❌ For debugging, please specify a target: `[p]member checkcontrib <mc_id>`")
    
    async def _check_single_member_verbose(
        self,
        ctx: commands.Context,
        target: str,
        threshold: float
    ):
        """Check contribution with VERBOSE Discord output."""
        
        # Step 1
        await ctx.send("🔍 **Step 1:** Resolving target...")
        
        try:
            member_data = await self._resolve_target(ctx.guild, target)
            
            if not member_data or not member_data.mc_user_id:
                await ctx.send(f"❌ Could not find member with MC account: `{target}`")
                return
            
            mc_id = member_data.mc_user_id
            mc_name = member_data.mc_username or "Unknown"
            
            await ctx.send(f"✅ **Step 1 Complete:** Found {mc_name} (MC ID: {mc_id})")
        
        except Exception as e:
            await ctx.send(f"❌ **Step 1 FAILED:** {type(e).__name__}: {str(e)}")
            return
        
        # Step 2
        await ctx.send("🔍 **Step 2:** Getting current MC data...")
        
        try:
            mc_data = await self._get_mc_data_verbose(ctx, mc_id)
            
            if not mc_data:
                await ctx.send(f"❌ Member not found in alliance")
                return
            
            current_rate = mc_data.get("contribution_rate", 0.0)
            await ctx.send(f"✅ **Step 2 Complete:** Current rate: {current_rate}%")
        
        except Exception as e:
            await ctx.send(f"❌ **Step 2 FAILED:** {type(e).__name__}: {str(e)}")
            return
        
        # Step 3
        await ctx.send("🔍 **Step 3:** Getting join date...")
        
        try:
            join_date, grace_source = await self._get_join_date_verbose(ctx, mc_id, mc_name)
            
            if join_date:
                days = (datetime.now(timezone.utc) - join_date).days
                await ctx.send(f"✅ **Step 3 Complete:** Join date: {join_date.date()} ({days} days ago) | Source: {grace_source}")
            else:
                await ctx.send(f"❌ **Step 3:** No join date found")
        
        except Exception as e:
            await ctx.send(f"❌ **Step 3 FAILED:** {type(e).__name__}: {str(e)}")
            join_date = None
            grace_source = "error"
        
        # Step 4 - THE CRITICAL ONE
        await ctx.send("🔍 **Step 4:** Getting historical contribution rates...")
        
        try:
            # First check if members_scraper exists
            if not self.members_scraper:
                await ctx.send("❌ **Step 4 FAILED:** members_scraper is None!")
                return
            
            await ctx.send(f"✅ members_scraper exists: {type(self.members_scraper).__name__}")
            
            # Check if db_path exists
            if not hasattr(self.members_scraper, 'db_path'):
                await ctx.send("❌ **Step 4 FAILED:** members_scraper has no db_path attribute!")
                return
            
            db_path = self.members_scraper.db_path
            await ctx.send(f"✅ db_path: `{db_path}`")
            
            # Check if path exists
            if not Path(db_path).exists():
                await ctx.send(f"❌ **Step 4 FAILED:** Database file does not exist at path!")
                return
            
            await ctx.send(f"✅ Database file exists")
            
            # Try to call the function
            await ctx.send("🔍 Calling _get_historical_rates_for_member()...")
            
            historical_rates = await self._get_historical_rates_verbose(ctx, mc_id)
            
            await ctx.send(f"✅ **Step 4 Complete:** Found {len(historical_rates)} historical rates")
            
            if historical_rates:
                rates_preview = ", ".join(f"{r:.1f}%" for r in historical_rates[:5])
                await ctx.send(f"📊 Preview: {rates_preview}")
        
        except Exception as e:
            await ctx.send(f"❌ **Step 4 FAILED:** {type(e).__name__}: {str(e)}")
            import traceback
            tb = traceback.format_exc()
            # Send traceback in chunks if too long
            for chunk in [tb[i:i+1900] for i in range(0, len(tb), 1900)]:
                await ctx.send(f"```python\n{chunk}\n```")
            historical_rates = []
        
        # Final summary
        embed = discord.Embed(
            title=f"🔍 Debug Summary: {mc_name}",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="✅ Working",
            value=f"• MC ID: {mc_id}\n• Current rate: {current_rate}%",
            inline=False
        )
        
        if join_date:
            days = (datetime.now(timezone.utc) - join_date).days
            embed.add_field(
                name="✅ Join Date",
                value=f"• Days in alliance: {days}\n• Source: {grace_source}",
                inline=False
            )
        else:
            embed.add_field(
                name="❌ Join Date",
                value="No join date found",
                inline=False
            )
        
        embed.add_field(
            name="📊 Historical Rates",
            value=f"Found: {len(historical_rates)} checks",
            inline=False
        )
        
        await ctx.send(embed=embed)
    
    async def _get_mc_data_verbose(self, ctx: commands.Context, mc_id: str) -> Optional[Dict[str, Any]]:
        """Get MC data with verbose output."""
        if not self.members_scraper:
            await ctx.send("⚠️ MembersScraper not available")
            return None
        
        try:
            db_path = Path(self.members_scraper.db_path)
            
            result = await asyncio.to_thread(
                self._query_mc_data_sync,
                db_path,
                mc_id
            )
            
            return result
        
        except Exception as e:
            await ctx.send(f"⚠️ _get_mc_data failed: {e}")
            return None
    
    async def _get_join_date_verbose(
        self,
        ctx: commands.Context,
        mc_id: str,
        mc_name: Optional[str]
    ) -> tuple[Optional[datetime], str]:
        """Get join date with verbose output."""
        
        # Try LogsScraper
        if self.logs_scraper:
            try:
                await ctx.send("  🔍 Trying LogsScraper...")
                db_path = self.logs_scraper.db_path
                
                join_date_str = await asyncio.to_thread(
                    self._query_join_date_sync,
                    db_path,
                    mc_id,
                    mc_name
                )
                
                if join_date_str:
                    if join_date_str.endswith('Z'):
                        join_date_str = join_date_str.replace('Z', '+00:00')
                    
                    join_date = datetime.fromisoformat(join_date_str)
                    await ctx.send(f"  ✅ Found in LogsScraper: {join_date.date()}")
                    return join_date, "LogsScraper"
                else:
                    await ctx.send("  ⚠️ Not found in LogsScraper")
            
            except Exception as e:
                await ctx.send(f"  ⚠️ LogsScraper error: {e}")
        
        # Fallback to first scrape
        if self.members_scraper:
            try:
                await ctx.send("  🔍 Trying first scrape fallback...")
                db_path = self.members_scraper.db_path
                
                first_seen_str = await asyncio.to_thread(
                    self._query_first_scrape_sync,
                    Path(db_path),
                    mc_id
                )
                
                if first_seen_str:
                    if first_seen_str.endswith('Z'):
                        first_seen_str = first_seen_str.replace('Z', '+00:00')
                    
                    first_seen = datetime.fromisoformat(first_seen_str)
                    await ctx.send(f"  ✅ Found first scrape: {first_seen.date()}")
                    return first_seen, "first scrape (fallback)"
                else:
                    await ctx.send("  ⚠️ No first scrape found")
            
            except Exception as e:
                await ctx.send(f"  ⚠️ First scrape error: {e}")
        
        return None, "unknown"
    
    async def _get_historical_rates_verbose(
        self,
        ctx: commands.Context,
        mc_id: str
    ) -> List[float]:
        """Get historical rates with ULTRA VERBOSE output."""
        
        if not self.members_scraper:
            await ctx.send("  ⚠️ members_scraper is None")
            return []
        
        try:
            await ctx.send(f"  🔍 Getting db_path from members_scraper...")
            db_path = self.members_scraper.db_path
            await ctx.send(f"  ✅ db_path type: {type(db_path)}")
            await ctx.send(f"  ✅ db_path value: `{db_path}`")
            
            # Convert to Path
            await ctx.send(f"  🔍 Converting to Path object...")
            db_path_obj = Path(db_path)
            await ctx.send(f"  ✅ Path object created")
            
            # Check existence
            await ctx.send(f"  🔍 Checking if file exists...")
            exists = db_path_obj.exists()
            await ctx.send(f"  ✅ File exists: {exists}")
            
            if not exists:
                await ctx.send(f"  ❌ File does not exist!")
                return []
            
            # Call sync function via to_thread
            await ctx.send(f"  🔍 Calling _query_historical_rates_sync via to_thread...")
            
            rates = await asyncio.to_thread(
                self._query_historical_rates_sync,
                db_path_obj,
                mc_id
            )
            
            await ctx.send(f"  ✅ to_thread completed, got {len(rates)} rates")
            
            return rates
        
        except Exception as e:
            await ctx.send(f"  ❌ Exception: {type(e).__name__}: {str(e)}")
            import traceback
            tb = traceback.format_exc()
            for chunk in [tb[i:i+1900] for i in range(0, len(tb), 1900)]:
                await ctx.send(f"```python\n{chunk}\n```")
            return []
    
    # ==================== SYNC FUNCTIONS ====================
    
    def _query_join_date_sync(self, db_path: Path, mc_id: str, mc_name: Optional[str]) -> Optional[str]:
        """Sync query for join date."""
        import sqlite3
        
        try:
            if not db_path.exists():
                return None
            
            conn = sqlite3.connect(str(db_path))
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT MIN(ts) as join_date 
                FROM logs 
                WHERE (affected_mc_id = ? OR affected_name = ?)
                AND action_key = 'added_to_alliance'
                """,
                (mc_id, mc_name)
            )
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                return result[0]
        
        except Exception as e:
            log.error(f"Query join date error: {e}")
        
        return None
    
    def _query_first_scrape_sync(self, db_path: Path, mc_id: str) -> Optional[str]:
        """Sync query for first scrape."""
        import sqlite3
        
        try:
            if not db_path.exists():
                return None
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT MIN(timestamp) as first_seen
                FROM members
                WHERE member_id = ?
            """, (mc_id,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result and result["first_seen"]:
                return result["first_seen"]
        
        except Exception as e:
            log.error(f"Query first scrape error: {e}")
        
        return None
    
    def _query_historical_rates_sync(self, db_path: Path, mc_id: str) -> List[float]:
        """Sync query for historical rates."""
        import sqlite3
        
        try:
            if not db_path.exists():
                log.error(f"Database not found: {db_path}")
                return []
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT contribution_rate, timestamp
                FROM members
                WHERE member_id = ?
                ORDER BY timestamp DESC
                LIMIT 12
            """, (mc_id,))
            
            rows = cursor.fetchall()
            conn.close()
            
            rates = [row["contribution_rate"] for row in rows if row["contribution_rate"] is not None]
            
            log.debug(f"Found {len(rates)} historical rates for {mc_id}")
            return rates
        
        except Exception as e:
            log.error(f"Query historical rates error: {e}", exc_info=True)
            return []
    
    def _query_mc_data_sync(self, db_path: Path, mc_user_id: str) -> Optional[Dict[str, Any]]:
        """Sync query for MC data."""
        import sqlite3
        
        try:
            if not db_path.exists():
                return None
            
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT member_id, username, rank, earned_credits, contribution_rate
                FROM members
                WHERE member_id = ?
                ORDER BY timestamp DESC
                LIMIT 1
            """, (mc_user_id,))
            
            row = cursor.fetchone()
            conn.close()
            
            if row:
                return {
                    "user_id": row["member_id"],
                    "name": row["username"],
                    "role": row["rank"],
                    "earned_credits": row["earned_credits"],
                    "contribution_rate": row["contribution_rate"]
                }
        
        except Exception as e:
            log.error(f"Query MC data error: {e}")
        
        return None
    
    async def _resolve_target(
        self,
        guild: discord.Guild,
        target: str
    ) -> Optional[MemberData]:
        """Resolve target to MemberData."""
        # Try Discord mention/ID
        discord_member = None
        try:
            if target.startswith("<@") and target.endswith(">"):
                user_id = int(target.strip("<@!>"))
                discord_member = guild.get_member(user_id)
            else:
                user_id = int(target)
                discord_member = guild.get_member(user_id)
        except ValueError:
            pass
        
        if discord_member:
            return await self._build_member_data(
                guild=guild,
                discord_id=discord_member.id
            )
        
        # Try MC ID
        if target.isdigit():
            mc_data = await self._get_mc_data_verbose(None, target)
            
            if mc_data:
                discord_id = None
                if self.membersync:
                    link = await self.membersync.get_link_for_mc(target)
                    if link:
                        discord_id = link.get("discord_id")
                
                return await self._build_member_data(
                    guild=guild,
                    discord_id=discord_id,
                    mc_user_id=target
                )
        
        return None
    
    async def _build_member_data(
        self,
        guild: discord.Guild,
        discord_id: Optional[int] = None,
        mc_user_id: Optional[str] = None
    ) -> MemberData:
        """Build MemberData object."""
        data = MemberData(
            discord_id=discord_id,
            mc_user_id=mc_user_id
        )
        
        # Get Discord data
        if discord_id:
            member = guild.get_member(discord_id)
            if member:
                data.discord_username = str(member)
        
        # Get MC data
        if mc_user_id and self.members_scraper:
            try:
                mc_data = await self._get_mc_data_verbose(None, mc_user_id)
                if mc_data:
                    data.mc_username = mc_data.get("name")
                    data.mc_role = mc_data.get("role")
                    data.contribution_rate = mc_data.get("contribution_rate")
            except Exception as e:
                log.error(f"Failed to get MC data: {e}")
        
        return data


async def setup(bot: Red):
    """Load MemberManager cog."""
    await bot.add_cog(MemberManager(bot))
