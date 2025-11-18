"""
MissionsDatabase Cog - Fetch and post MissionChief missions to Discord forum
FIXED VERSION with debug commands and better error tracking
"""

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box, pagify
import asyncio
from datetime import datetime, time, timedelta
from pathlib import Path
import logging

from .database import MissionsDatabase as DB
from .mission_fetcher import MissionFetcher
from .mission_formatter import MissionFormatter
from .mappings import get_tags_for_mission

log = logging.getLogger("red.missionsdatabase")


class MissionsDatabase(commands.Cog):
    """
    MissionChief missions database system.
    Fetches missions from MissionChief and posts them to a Discord forum.
    """
    
    # Rate limiting settings (configurable)
    POSTS_PER_BATCH = 5  # Number of posts before taking a longer break
    BATCH_DELAY = 11  # Seconds to wait between batches
    POST_DELAY = 1  # Seconds to wait between individual posts
    MAX_RETRIES = 3  # Maximum retries for failed operations
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        # Database path
        data_path = Path(__file__).parent
        self.db = DB(data_path / "missions.db")
        
        # Fetcher and formatter
        self.fetcher = MissionFetcher()
        self.formatter = MissionFormatter()
        
        # Background task
        self.sync_task = None
        
        # Track failed missions during sync
        self.last_sync_errors = []
    
    async def cog_load(self):
        """Initialize the cog."""
        await self.db.initialize()
        self.sync_task = self.bot.loop.create_task(self.auto_sync_loop())
        log.info("MissionsDatabase cog loaded")
    
    async def cog_unload(self):
        """Clean up when cog is unloaded."""
        if self.sync_task:
            self.sync_task.cancel()
        await self.fetcher.close()
        log.info("MissionsDatabase cog unloaded")
    
    async def auto_sync_loop(self):
        """Background task that runs daily sync at 3 AM."""
        await self.bot.wait_until_ready()
        
        while True:
            try:
                # Calculate time until next 3 AM
                now = datetime.now()
                target_time = datetime.combine(now.date(), time(hour=3, minute=0))
                
                # If it's past 3 AM today, target tomorrow
                if now >= target_time:
                    target_time = datetime.combine(
                        now.date(), 
                        time(hour=3, minute=0)
                    ) + timedelta(days=1)
                
                # Wait until target time
                wait_seconds = (target_time - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
                # Run sync for all configured guilds
                await self.run_auto_sync()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error in auto sync loop: {e}", exc_info=True)
                # Wait 1 hour before retrying on error
                await asyncio.sleep(3600)
    
    async def run_auto_sync(self):
        """Run automatic sync for all configured guilds."""
        log.info("Running automatic mission sync...")
        
        for guild in self.bot.guilds:
            try:
                config = await self.db.get_config(guild.id)
                if not config or not config.get('auto_sync_enabled'):
                    continue
                
                log.info(f"Auto-syncing missions for guild {guild.name}")
                await self._sync_missions(guild)
                
            except Exception as e:
                log.error(f"Error syncing missions for guild {guild.name}: {e}", exc_info=True)
    
    @commands.group(name="missions")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def missions(self, ctx):
        """Manage MissionChief missions database."""
        pass
    
    @missions.command(name="setup")
    async def missions_setup(self, ctx, forum_channel: discord.ForumChannel, 
                            admin_channel: discord.TextChannel = None):
        """
        Set up the missions database system.
        
        Args:
            forum_channel: The forum channel where missions will be posted
            admin_channel: Optional channel for admin alerts
        """
        await ctx.send("Setting up missions database...")
        
        # Save configuration
        admin_channel_id = admin_channel.id if admin_channel else None
        await self.db.set_config(ctx.guild.id, forum_channel.id, admin_channel_id)
        
        msg = f"✅ Configuration saved!\n"
        msg += f"Forum Channel: {forum_channel.mention}\n"
        if admin_channel:
            msg += f"Admin Alerts: {admin_channel.mention}\n"
        msg += f"\nUse `{ctx.prefix}missions sync` to start syncing missions."
        
        await ctx.send(msg)
    
    @missions.command(name="sync")
    async def missions_sync(self, ctx):
        """Manually sync missions from MissionChief."""
        config = await self.db.get_config(ctx.guild.id)
        if not config:
            await ctx.send("❌ Please run setup first using `[p]missions setup`")
            return
        
        msg = await ctx.send("🔄 Starting mission sync...\nThis may take several minutes for the first sync.")
        
        try:
            stats = await self._sync_missions(ctx.guild, progress_message=msg)
            
            result_msg = "✅ Mission sync complete!\n"
            result_msg += f"New missions posted: {stats['new_missions']}\n"
            result_msg += f"Updated missions: {stats['updated_missions']}\n"
            result_msg += f"Unchanged missions: {stats['skipped_missions']}\n"
            result_msg += f"Failed missions: {stats['failed_missions']}\n"
            result_msg += f"Total missions: {stats['total_missions']}\n\n"
            
            if stats['failed_missions'] > 0:
                result_msg += f"⚠️ {stats['failed_missions']} missions failed. Use `{ctx.prefix}missions errors` to see details."
            
            await msg.edit(content=result_msg)
            
        except Exception as e:
            log.error(f"Error syncing missions: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error syncing missions: {str(e)}")
    
    @missions.command(name="fullreset")
    async def missions_fullreset(self, ctx):
        """
        Completely reset the missions database.
        WARNING: This will delete all tracked missions and optionally delete forum posts.
        """
        # Confirmation prompt
        confirm_msg = (
            "⚠️ **WARNING** ⚠️\n"
            "This will:\n"
            "1. Clear the entire missions database\n"
            "2. Optionally delete all mission forum posts\n\n"
            "Type `CONFIRM RESET` to proceed."
        )
        await ctx.send(confirm_msg)
        
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel
        
        try:
            response = await self.bot.wait_for('message', check=check, timeout=30.0)
            if response.content != "CONFIRM RESET":
                await ctx.send("Reset cancelled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("Reset cancelled (timeout).")
            return
        
        # Ask about deleting forum posts
        await ctx.send("Do you want to delete all mission forum posts? (yes/no)")
        
        try:
            response = await self.bot.wait_for('message', check=check, timeout=30.0)
            delete_posts = response.content.lower() in ['yes', 'y']
        except asyncio.TimeoutError:
            await ctx.send("Assuming no. Posts will not be deleted.")
            delete_posts = False
        
        msg = await ctx.send("🔄 Resetting database...")
        
        try:
            # Get all posts if we need to delete them
            if delete_posts:
                config = await self.db.get_config(ctx.guild.id)
                if config:
                    forum_channel = ctx.guild.get_channel(int(config['forum_channel_id']))
                    if forum_channel:
                        all_posts = await self.db.get_all_mission_posts()
                        deleted_count = 0
                        
                        for i, post_data in enumerate(all_posts):
                            try:
                                thread = forum_channel.get_thread(int(post_data['thread_id']))
                                if thread:
                                    await thread.delete()
                                    deleted_count += 1
                                    
                                    # Rate limiting
                                    await asyncio.sleep(1)
                                    
                                    if (deleted_count % 5 == 0):
                                        await asyncio.sleep(10)
                                        await msg.edit(content=f"Deleting posts... {deleted_count}/{len(all_posts)}")
                                        
                            except Exception as e:
                                log.error(f"Error deleting thread {post_data['thread_id']}: {e}")
                        
                        await msg.edit(content=f"Deleted {deleted_count} forum posts...")
            
            # Clear database
            await self.db.clear_all_missions()
            
            await msg.edit(content="✅ Database reset complete!")
            
        except Exception as e:
            log.error(f"Error resetting database: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error resetting database: {str(e)}")
    
    @missions.command(name="deleteall")
    async def missions_deleteall(self, ctx):
        """
        Delete all mission forum posts (with rate limiting).
        Database tracking remains intact.
        """
        config = await self.db.get_config(ctx.guild.id)
        if not config:
            await ctx.send("❌ Not configured. Use `[p]missions setup` first.")
            return
        
        # Confirmation
        confirm_msg = (
            "⚠️ **WARNING** ⚠️\n"
            "This will delete ALL mission forum posts!\n"
            "Database tracking will remain (you can recreate posts with sync).\n\n"
            "Type `CONFIRM DELETE` to proceed."
        )
        await ctx.send(confirm_msg)
        
        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel
        
        try:
            response = await self.bot.wait_for('message', check=check, timeout=30.0)
            if response.content != "CONFIRM DELETE":
                await ctx.send("Deletion cancelled.")
                return
        except asyncio.TimeoutError:
            await ctx.send("Deletion cancelled (timeout).")
            return
        
        msg = await ctx.send("🔄 Deleting forum posts...")
        
        try:
            forum_channel = ctx.guild.get_channel(int(config['forum_channel_id']))
            if not forum_channel:
                await msg.edit(content="❌ Forum channel not found.")
                return
            
            all_posts = await self.db.get_all_mission_posts()
            deleted_count = 0
            failed_count = 0
            
            for i, post_data in enumerate(all_posts):
                try:
                    thread = forum_channel.get_thread(int(post_data['thread_id']))
                    if thread:
                        await thread.delete()
                        deleted_count += 1
                    else:
                        failed_count += 1
                    
                    # Rate limiting
                    await asyncio.sleep(self.POST_DELAY)
                    
                    # Batch rate limiting
                    if (deleted_count % self.POSTS_PER_BATCH == 0) and deleted_count > 0:
                        await asyncio.sleep(self.BATCH_DELAY)
                    
                    # Progress updates every 10 posts
                    if (i + 1) % 10 == 0:
                        await msg.edit(
                            content=f"🔄 Deleting posts... {i + 1}/{len(all_posts)}\n"
                                   f"Deleted: {deleted_count} | Failed: {failed_count}"
                        )
                        
                except discord.errors.NotFound:
                    failed_count += 1
                except Exception as e:
                    log.error(f"Error deleting thread {post_data['thread_id']}: {e}")
                    failed_count += 1
            
            result_msg = f"✅ Deletion complete!\n"
            result_msg += f"Deleted: {deleted_count}\n"
            result_msg += f"Failed/Not Found: {failed_count}\n"
            result_msg += f"Total processed: {len(all_posts)}\n\n"
            result_msg += f"Use `{ctx.prefix}missions sync` to recreate posts."
            
            await msg.edit(content=result_msg)
            
        except Exception as e:
            log.error(f"Error deleting posts: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error deleting posts: {str(e)}")
    
    @missions.command(name="check")
    async def missions_check(self, ctx):
        """Check missions database statistics."""
        config = await self.db.get_config(ctx.guild.id)
        if not config:
            await ctx.send("❌ Not configured. Use `[p]missions setup` first.")
            return
        
        stats = await self.db.get_statistics()
        
        msg = "📊 **Missions Database Statistics**\n"
        msg += f"Total missions tracked: {stats['total_missions']}\n"
        msg += f"Updated missions: {stats['updated_missions']}\n"
        msg += f"Auto-sync enabled: {'Yes' if config.get('auto_sync_enabled') else 'No'}\n"
        
        if config.get('last_full_sync'):
            msg += f"Last sync: {config['last_full_sync']}"
        
        await ctx.send(msg)
    
    @missions.command(name="toggle")
    async def missions_toggle(self, ctx):
        """Toggle automatic syncing on/off."""
        config = await self.db.get_config(ctx.guild.id)
        if not config:
            await ctx.send("❌ Not configured. Use `[p]missions setup` first.")
            return
        
        current_state = config.get('auto_sync_enabled', True)
        new_state = not current_state
        
        await self.db.set_auto_sync(ctx.guild.id, new_state)
        
        status = "enabled" if new_state else "disabled"
        await ctx.send(f"✅ Automatic syncing {status}.")
    
    @missions.command(name="ratelimit")
    async def missions_ratelimit(self, ctx, posts_per_batch: int = None, 
                                 batch_delay: int = None, post_delay: float = None):
        """
        View or configure rate limiting settings.
        
        Without arguments, shows current settings.
        With arguments, updates the settings.
        
        Args:
            posts_per_batch: Number of posts before taking a longer break (default: 5)
            batch_delay: Seconds to wait between batches (default: 11)
            post_delay: Seconds to wait between individual posts (default: 1)
        """
        if posts_per_batch is None and batch_delay is None and post_delay is None:
            # Show current settings
            msg = "⚙️ **Rate Limiting Settings**\n"
            msg += f"Posts per batch: {self.POSTS_PER_BATCH}\n"
            msg += f"Batch delay: {self.BATCH_DELAY} seconds\n"
            msg += f"Post delay: {self.POST_DELAY} seconds\n\n"
            msg += f"Use `{ctx.prefix}missions ratelimit <posts_per_batch> <batch_delay> <post_delay>` to change."
            await ctx.send(msg)
            return
        
        # Update settings
        if posts_per_batch is not None:
            if posts_per_batch < 1 or posts_per_batch > 10:
                await ctx.send("❌ Posts per batch must be between 1 and 10.")
                return
            self.POSTS_PER_BATCH = posts_per_batch
        
        if batch_delay is not None:
            if batch_delay < 5 or batch_delay > 60:
                await ctx.send("❌ Batch delay must be between 5 and 60 seconds.")
                return
            self.BATCH_DELAY = batch_delay
        
        if post_delay is not None:
            if post_delay < 0.5 or post_delay > 5:
                await ctx.send("❌ Post delay must be between 0.5 and 5 seconds.")
                return
            self.POST_DELAY = post_delay
        
        msg = "✅ Rate limiting settings updated!\n"
        msg += f"Posts per batch: {self.POSTS_PER_BATCH}\n"
        msg += f"Batch delay: {self.BATCH_DELAY} seconds\n"
        msg += f"Post delay: {self.POST_DELAY} seconds"
        
        await ctx.send(msg)
    
    @missions.command(name="update")
    async def missions_update(self, ctx, mission_id: str):
        """
        Force update a specific mission.
        
        Args:
            mission_id: The mission ID (e.g., "88" or "88/a")
        """
        config = await self.db.get_config(ctx.guild.id)
        if not config:
            await ctx.send("❌ Not configured. Use `[p]missions setup` first.")
            return
        
        msg = await ctx.send(f"🔄 Updating mission {mission_id}...")
        
        try:
            # Fetch all missions
            missions = await self.fetcher.fetch_missions()
            
            # Find the specific mission
            target_mission = None
            for mission in missions:
                parsed_id = self.fetcher.parse_mission_id(mission)
                if parsed_id == mission_id or str(mission.get('id')) == mission_id:
                    target_mission = mission
                    break
            
            if not target_mission:
                await msg.edit(content=f"❌ Mission {mission_id} not found in MissionChief JSON.")
                return
            
            # Update the mission
            await self._update_single_mission(ctx.guild, target_mission)
            await msg.edit(content=f"✅ Mission {mission_id} updated successfully!")
            
        except Exception as e:
            log.error(f"Error updating mission {mission_id}: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error updating mission: {str(e)}")
    
    @missions.command(name="view")
    async def missions_view(self, ctx, mission_id: str):
        """
        Preview how a mission will be formatted.
        
        Args:
            mission_id: The mission ID (e.g., "88" or "88/a")
        """
        msg = await ctx.send(f"🔍 Fetching mission {mission_id}...")
        
        try:
            # Fetch all missions
            missions = await self.fetcher.fetch_missions()
            
            # Find the specific mission
            target_mission = None
            for mission in missions:
                parsed_id = self.fetcher.parse_mission_id(mission)
                if parsed_id == mission_id or str(mission.get('id')) == mission_id:
                    target_mission = mission
                    break
            
            if not target_mission:
                await msg.edit(content=f"❌ Mission {mission_id} not found.")
                return
            
            # Format the mission
            formatted = self.formatter.format_mission_post(target_mission)
            
            # Get tags
            categories = target_mission.get('mission_categories', [])
            tags = get_tags_for_mission(categories)
            
            # Send in pages if too long
            await msg.delete()
            
            # Show tags first
            await ctx.send(f"**Tags:** {', '.join(tags)}\n")
            
            for page in pagify(formatted):
                await ctx.send(box(page))
            
        except Exception as e:
            log.error(f"Error viewing mission {mission_id}: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error viewing mission: {str(e)}")
    
    @missions.command(name="debug")
    async def missions_debug(self, ctx):
        """
        Show debug information about missing missions.
        """
        msg = await ctx.send("🔍 Analyzing missions database...")
        
        try:
            # Fetch all missions from MissionChief
            missions = await self.fetcher.fetch_missions()
            
            # Get all mission IDs from JSON
            json_mission_ids = set()
            for mission in missions:
                mission_id = self.fetcher.parse_mission_id(mission)
                json_mission_ids.add(mission_id)
            
            # Get all mission IDs from database
            db_posts = await self.db.get_all_mission_posts()
            db_mission_ids = {post['mission_id'] for post in db_posts}
            
            # Find missing missions
            missing_ids = json_mission_ids - db_mission_ids
            extra_ids = db_mission_ids - json_mission_ids
            
            # Build report
            report = "📊 **Mission Database Debug Report**\n\n"
            report += f"**JSON missions:** {len(json_mission_ids)}\n"
            report += f"**Database missions:** {len(db_mission_ids)}\n"
            report += f"**Missing from DB:** {len(missing_ids)}\n"
            report += f"**Extra in DB:** {len(extra_ids)}\n\n"
            
            if missing_ids:
                # Sort numerically if possible
                try:
                    sorted_missing = sorted(missing_ids, key=lambda x: (int(x.split('/')[0]), x))
                except:
                    sorted_missing = sorted(missing_ids)
                
                report += "**Missing Mission IDs (first 20):**\n"
                for mid in list(sorted_missing)[:20]:
                    report += f"- {mid}\n"
                
                if len(missing_ids) > 20:
                    report += f"... and {len(missing_ids) - 20} more\n"
            
            if extra_ids:
                report += f"\n**Extra IDs in DB (removed from JSON):** {len(extra_ids)}\n"
            
            await msg.edit(content=report)
            
        except Exception as e:
            log.error(f"Error in debug command: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error: {str(e)}")
    
    @missions.command(name="missing")
    async def missions_missing(self, ctx, limit: int = 50):
        """
        Show missing mission IDs and allow posting them.
        
        Args:
            limit: Maximum number of missing missions to show (default: 50)
        """
        msg = await ctx.send("🔍 Finding missing missions...")
        
        try:
            # Fetch all missions from MissionChief
            missions = await self.fetcher.fetch_missions()
            
            # Get all mission IDs from JSON
            json_missions = {}
            for mission in missions:
                mission_id = self.fetcher.parse_mission_id(mission)
                json_missions[mission_id] = mission
            
            # Get all mission IDs from database
            db_posts = await self.db.get_all_mission_posts()
            db_mission_ids = {post['mission_id'] for post in db_posts}
            
            # Find missing missions
            missing_ids = set(json_missions.keys()) - db_mission_ids
            
            if not missing_ids:
                await msg.edit(content="✅ No missing missions! Database is up to date.")
                return
            
            # Sort numerically
            try:
                sorted_missing = sorted(missing_ids, key=lambda x: (int(x.split('/')[0]), x))
            except:
                sorted_missing = sorted(missing_ids)
            
            # Show limited list
            display_list = sorted_missing[:limit]
            
            report = f"📋 **Missing Missions: {len(missing_ids)} total**\n\n"
            report += "**First " + str(min(limit, len(missing_ids))) + " missing IDs:**\n"
            
            for mid in display_list:
                mission_name = json_missions[mid].get('name', 'Unknown')
                report += f"- {mid}: {mission_name}\n"
            
            if len(missing_ids) > limit:
                report += f"\n... and {len(missing_ids) - limit} more\n"
            
            report += f"\nUse `{ctx.prefix}missions syncmissing` to post all missing missions."
            
            await msg.edit(content=report)
            
        except Exception as e:
            log.error(f"Error in missing command: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error: {str(e)}")
    
    @missions.command(name="syncmissing")
    async def missions_syncmissing(self, ctx):
        """
        Sync only the missions that are missing from the database.
        """
        config = await self.db.get_config(ctx.guild.id)
        if not config:
            await ctx.send("❌ Not configured. Use `[p]missions setup` first.")
            return
        
        msg = await ctx.send("🔄 Finding and syncing missing missions...")
        
        try:
            forum_channel = ctx.guild.get_channel(int(config['forum_channel_id']))
            if not forum_channel:
                await msg.edit(content="❌ Forum channel not found.")
                return
            
            # Fetch all missions from MissionChief
            missions = await self.fetcher.fetch_missions()
            
            # Get all mission IDs from JSON
            json_missions = {}
            for mission in missions:
                mission_id = self.fetcher.parse_mission_id(mission)
                json_missions[mission_id] = mission
            
            # Get all mission IDs from database
            db_posts = await self.db.get_all_mission_posts()
            db_mission_ids = {post['mission_id'] for post in db_posts}
            
            # Find missing missions
            missing_ids = set(json_missions.keys()) - db_mission_ids
            
            if not missing_ids:
                await msg.edit(content="✅ No missing missions! Database is up to date.")
                return
            
            # Sort numerically
            try:
                sorted_missing = sorted(missing_ids, key=lambda x: (int(x.split('/')[0]), x))
            except:
                sorted_missing = sorted(missing_ids)
            
            await msg.edit(content=f"🔄 Found {len(missing_ids)} missing missions. Starting sync...")
            
            posted = 0
            failed = 0
            errors = []
            
            for i, mission_id in enumerate(sorted_missing):
                try:
                    mission_data = json_missions[mission_id]
                    mission_hash = self.fetcher.calculate_hash(mission_data)
                    
                    # Create the post
                    await self._create_mission_post(forum_channel, mission_data, mission_id, mission_hash)
                    posted += 1
                    
                    # Rate limiting
                    await asyncio.sleep(self.POST_DELAY)
                    
                    # Batch rate limiting
                    if (posted % self.POSTS_PER_BATCH == 0) and posted > 0:
                        await asyncio.sleep(self.BATCH_DELAY)
                    
                    # Progress updates
                    if (i + 1) % 10 == 0:
                        await msg.edit(
                            content=f"🔄 Syncing... {i + 1}/{len(missing_ids)}\n"
                                   f"Posted: {posted} | Failed: {failed}"
                        )
                        
                except discord.errors.RateLimited as e:
                    log.warning(f"Rate limited! Waiting {e.retry_after} seconds...")
                    await asyncio.sleep(e.retry_after)
                    # Retry this mission
                    try:
                        await self._create_mission_post(forum_channel, mission_data, mission_id, mission_hash)
                        posted += 1
                    except Exception as retry_error:
                        failed += 1
                        errors.append(f"{mission_id}: {str(retry_error)[:50]}")
                        log.error(f"Failed to post mission {mission_id} after retry: {retry_error}")
                        
                except Exception as e:
                    failed += 1
                    errors.append(f"{mission_id}: {str(e)[:50]}")
                    log.error(f"Error posting mission {mission_id}: {e}", exc_info=True)
            
            # Final report
            result = f"✅ **Sync Complete!**\n"
            result += f"Posted: {posted}\n"
            result += f"Failed: {failed}\n"
            result += f"Total: {len(missing_ids)}\n"
            
            if errors:
                result += f"\n⚠️ Use `{ctx.prefix}missions errors` to see failed missions."
                self.last_sync_errors = errors
            
            await msg.edit(content=result)
            
        except Exception as e:
            log.error(f"Error in syncmissing: {e}", exc_info=True)
            await msg.edit(content=f"❌ Error: {str(e)}")
    
    @missions.command(name="errors")
    async def missions_errors(self, ctx):
        """Show errors from the last sync."""
        if not self.last_sync_errors:
            await ctx.send("✅ No errors from last sync!")
            return
        
        report = f"⚠️ **Errors from last sync ({len(self.last_sync_errors)} total):**\n\n"
        
        for error in self.last_sync_errors[:20]:
            report += f"- {error}\n"
        
        if len(self.last_sync_errors) > 20:
            report += f"\n... and {len(self.last_sync_errors) - 20} more errors"
        
        for page in pagify(report):
            await ctx.send(page)
    
    async def _sync_missions(self, guild: discord.Guild, progress_message=None) -> dict:
        """
        Sync missions for a guild with rate limiting.
        
        Args:
            guild: Discord guild
            progress_message: Optional message to update with progress
            
        Returns:
            Dictionary with sync statistics
        """
        config = await self.db.get_config(guild.id)
        if not config:
            raise ValueError("Guild not configured")
        
        forum_channel = guild.get_channel(int(config['forum_channel_id']))
        if not forum_channel:
            raise ValueError("Forum channel not found")
        
        # Fetch missions from MissionChief
        missions = await self.fetcher.fetch_missions()
        
        stats = {
            'new_missions': 0,
            'updated_missions': 0,
            'skipped_missions': 0,
            'failed_missions': 0,
            'total_missions': len(missions)
        }
        
        processed = 0
        errors = []
        
        for mission_data in missions:
            mission_id = self.fetcher.parse_mission_id(mission_data)
            mission_hash = self.fetcher.calculate_hash(mission_data)
            
            # Check if mission exists in database
            existing = await self.db.get_mission_post(mission_id)
            
            try:
                if not existing:
                    # New mission - create forum post
                    await self._create_mission_post_with_retry(
                        forum_channel, mission_data, mission_id, mission_hash
                    )
                    stats['new_missions'] += 1
                    
                    # Rate limiting: wait after creating post
                    await asyncio.sleep(self.POST_DELAY)
                    
                elif existing['mission_data_hash'] != mission_hash:
                    # Mission changed - update post
                    await self._update_mission_post_with_retry(
                        forum_channel, mission_data, mission_id, 
                        mission_hash, existing, config
                    )
                    stats['updated_missions'] += 1
                    
                    # Rate limiting: wait after updating post
                    await asyncio.sleep(self.POST_DELAY)
                    
                else:
                    # No changes - just update last check timestamp
                    await self.db.update_last_check(mission_id)
                    stats['skipped_missions'] += 1
                
                processed += 1
                
                # Update progress message every 10 missions
                if progress_message and processed % 10 == 0:
                    try:
                        await progress_message.edit(
                            content=f"🔄 Syncing missions... {processed}/{len(missions)}\n"
                                   f"New: {stats['new_missions']} | Updated: {stats['updated_missions']} | Failed: {stats['failed_missions']}"
                        )
                    except:
                        pass  # Ignore edit errors
                
                # Batch rate limiting: after every X new/updated posts, take a longer break
                if (stats['new_missions'] + stats['updated_missions']) % self.POSTS_PER_BATCH == 0:
                    if stats['new_missions'] + stats['updated_missions'] > 0:
                        log.info(f"Rate limit pause after {self.POSTS_PER_BATCH} posts...")
                        await asyncio.sleep(self.BATCH_DELAY)
                        
            except Exception as e:
                stats['failed_missions'] += 1
                error_msg = f"{mission_id}: {str(e)[:100]}"
                errors.append(error_msg)
                log.error(f"Error processing mission {mission_id}: {e}", exc_info=True)
                continue
        
        # Update last sync timestamp
        await self.db.update_last_sync(guild.id)
        
        # Store errors for later viewing
        self.last_sync_errors = errors
        
        return stats
    
    async def _create_mission_post_with_retry(self, forum_channel: discord.ForumChannel, 
                                             mission_data: dict, mission_id: str, 
                                             mission_hash: str):
        """Create a mission post with retry logic."""
        for attempt in range(self.MAX_RETRIES):
            try:
                await self._create_mission_post(forum_channel, mission_data, mission_id, mission_hash)
                return
            except discord.errors.RateLimited as e:
                if attempt < self.MAX_RETRIES - 1:
                    log.warning(f"Rate limited creating {mission_id}, waiting {e.retry_after}s (attempt {attempt + 1})")
                    await asyncio.sleep(e.retry_after)
                else:
                    raise
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    log.warning(f"Error creating {mission_id}, retrying (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(2)
                else:
                    raise
    
    async def _update_mission_post_with_retry(self, forum_channel: discord.ForumChannel,
                                             mission_data: dict, mission_id: str, 
                                             mission_hash: str, existing: dict, config: dict):
        """Update a mission post with retry logic."""
        for attempt in range(self.MAX_RETRIES):
            try:
                await self._update_mission_post(
                    forum_channel, mission_data, mission_id, 
                    mission_hash, existing, config
                )
                return
            except discord.errors.RateLimited as e:
                if attempt < self.MAX_RETRIES - 1:
                    log.warning(f"Rate limited updating {mission_id}, waiting {e.retry_after}s (attempt {attempt + 1})")
                    await asyncio.sleep(e.retry_after)
                else:
                    raise
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    log.warning(f"Error updating {mission_id}, retrying (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(2)
                else:
                    raise
    
    async def _create_mission_post(self, forum_channel: discord.ForumChannel, 
                                   mission_data: dict, mission_id: str, mission_hash: str):
        """Create a new forum post for a mission."""
        title = self.formatter.get_mission_title(mission_data)
        content = self.formatter.format_mission_post(mission_data)
        
        # Get tags for this mission
        categories = mission_data.get('mission_categories', [])
        tag_names = get_tags_for_mission(categories)
        
        # Find matching tags in forum
        applied_tags = []
        for tag in forum_channel.available_tags:
            if tag.name in tag_names:
                applied_tags.append(tag)
        
        # Create the thread with tags
        thread = await forum_channel.create_thread(
            name=title,
            content=content,
            applied_tags=applied_tags[:5]  # Max 5 tags
        )
        
        # Save to database
        await self.db.add_mission_post(mission_id, thread.thread.id, mission_hash)
        
        log.info(f"Created mission post: {mission_id} - {title} (Tags: {', '.join(tag_names)})")
    
    async def _update_mission_post(self, forum_channel: discord.ForumChannel,
                                   mission_data: dict, mission_id: str, mission_hash: str,
                                   existing: dict, config: dict):
        """Update an existing mission post."""
        title = self.formatter.get_mission_title(mission_data)
        content = self.formatter.format_mission_post(mission_data)
        
        try:
            # Try to get the thread and update it
            thread = forum_channel.get_thread(int(existing['thread_id']))
            if thread:
                # Get the starter message
                starter_message = await thread.fetch_message(thread.id)
                await starter_message.edit(content=content)
                
                # Update tags
                categories = mission_data.get('mission_categories', [])
                tag_names = get_tags_for_mission(categories)
                
                # Find matching tags in forum
                applied_tags = []
                for tag in forum_channel.available_tags:
                    if tag.name in tag_names:
                        applied_tags.append(tag)
                
                # Update thread tags
                await thread.edit(applied_tags=applied_tags[:5])
                
                await self.db.update_mission_post(mission_id, thread.id, mission_hash)
                log.info(f"Updated mission post: {mission_id} - {title}")
                return
        except Exception as e:
            log.warning(f"Could not update thread for mission {mission_id}: {e}")
        
        # If we couldn't update, create a new post
        await self._create_mission_post(forum_channel, mission_data, mission_id, mission_hash)
        
        # Alert admins if configured
        if config.get('admin_alert_channel_id'):
            admin_channel = forum_channel.guild.get_channel(int(config['admin_alert_channel_id']))
            if admin_channel:
                await admin_channel.send(
                    f"⚠️ Could not update mission `{mission_id}` - created new post instead."
                )
    
    async def _update_single_mission(self, guild: discord.Guild, mission_data: dict):
        """Update a single mission."""
        config = await self.db.get_config(guild.id)
        if not config:
            raise ValueError("Guild not configured")
        
        forum_channel = guild.get_channel(int(config['forum_channel_id']))
        if not forum_channel:
            raise ValueError("Forum channel not found")
        
        mission_id = self.fetcher.parse_mission_id(mission_data)
        mission_hash = self.fetcher.calculate_hash(mission_data)
        
        existing = await self.db.get_mission_post(mission_id)
        
        if not existing:
            await self._create_mission_post_with_retry(forum_channel, mission_data, mission_id, mission_hash)
        else:
            await self._update_mission_post_with_retry(
                forum_channel, mission_data, mission_id,
                mission_hash, existing, config
            )
