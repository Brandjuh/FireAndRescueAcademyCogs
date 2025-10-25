import discord
from redbot.core import commands, Config, checks
from redbot.core.bot import Red
from redbot.core.data_manager import cog_data_path
import logging
import asyncio
import json
import re
from datetime import datetime, timedelta
from typing import Optional, List
from difflib import get_close_matches

from .database import AssetDatabase
from .github_sync import GitHubSync
from .utils.embeds import (
    create_vehicle_embed,
    create_building_embed,
    create_equipment_embed,
    create_education_embed,
    create_comparison_embed,
    create_sync_changelog_embed,
    create_error_embed,
    create_success_embed,
    create_list_embed
)

log = logging.getLogger("red.assetmanager")


class AssetManager(commands.Cog):
    """
    Asset database manager for Missionchief USA vehicles, buildings, equipment and trainings.
    """
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        default_global = {
            "changelog_channel_id": None,
            "auto_sync_enabled": True,
            "last_sync": None,
            "sync_hour": 0  # UTC hour for daily sync
        }
        
        self.config.register_global(**default_global)
        
        # Initialize database
        self.db_path = cog_data_path(self) / "assets.db"
        self.db = AssetDatabase(self.db_path)
        self.db.connect()
        self.db.initialize_tables()
        
        # Initialize GitHub sync
        self.github_sync = GitHubSync()
        
        # Background task
        self.sync_task = None
        
        log.info("AssetManager initialized")
    
    def cog_unload(self):
        """Cleanup when cog is unloaded."""
        if self.sync_task:
            self.sync_task.cancel()
        
        asyncio.create_task(self.github_sync.close_session())
        self.db.close()
        log.info("AssetManager unloaded")
    
    async def cog_load(self):
        """Start background tasks when cog is loaded."""
        self.sync_task = self.bot.loop.create_task(self.daily_sync_loop())
        log.info("Daily sync task started")
    
    # ========== BACKGROUND SYNC ==========
    
    async def daily_sync_loop(self):
        """Background task for daily syncing."""
        await self.bot.wait_until_red_ready()
        
        while True:
            try:
                # Check if auto sync is enabled
                auto_sync = await self.config.auto_sync_enabled()
                if not auto_sync:
                    await asyncio.sleep(3600)  # Check every hour
                    continue
                
                # Calculate time until next sync
                now = datetime.utcnow()
                sync_hour = await self.config.sync_hour()
                next_sync = now.replace(hour=sync_hour, minute=0, second=0, microsecond=0)
                
                if next_sync <= now:
                    next_sync += timedelta(days=1)
                
                wait_seconds = (next_sync - now).total_seconds()
                log.info(f"Next auto sync in {wait_seconds / 3600:.1f} hours")
                
                await asyncio.sleep(wait_seconds)
                
                # Perform sync
                log.info("Starting automated sync")
                await self.perform_full_sync(auto=True)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error in daily sync loop: {e}", exc_info=True)
                await asyncio.sleep(3600)  # Wait an hour before retrying
    
    async def perform_full_sync(self, auto: bool = False) -> dict:
        """Perform full sync of all data sources."""
        results = {
            'vehicles': {'success': False, 'changes': {}},
            'buildings': {'success': False, 'changes': {}},
            'equipment': {'success': False, 'changes': {}},
            'educations': {'success': False, 'changes': {}}
        }
        
        try:
            # Fetch all data
            log.info("Fetching data from GitHub...")
            all_data = await self.github_sync.fetch_all()
            
            # Sync vehicles
            if all_data['vehicles']:
                results['vehicles'] = await self.sync_vehicles(all_data['vehicles'])
            
            # Sync buildings
            if all_data['buildings']:
                results['buildings'] = await self.sync_buildings(all_data['buildings'])
            
            # Sync equipment
            if all_data['equipment']:
                results['equipment'] = await self.sync_equipment(all_data['equipment'])
            
            # Sync educations
            if all_data['educations']:
                results['educations'] = await self.sync_educations(all_data['educations'])
            
            # Update last sync time
            await self.config.last_sync.set(datetime.utcnow().isoformat())
            
            # Post changelog if auto sync and channel is set
            if auto:
                await self.post_changelog(results)
            
            log.info("Full sync completed successfully")
            
        except Exception as e:
            log.error(f"Error during full sync: {e}", exc_info=True)
            
            # Notify bot owner on error
            if auto:
                await self.notify_owner_error(str(e))
        
        return results
    
    async def sync_vehicles(self, vehicles_data: dict) -> dict:
        """Sync vehicles to database."""
        try:
            old_vehicles = self.db.get_all_vehicles()
            changes = self.github_sync.detect_changes(old_vehicles, vehicles_data)
            
            # Process each vehicle
            for game_id, raw_data in vehicles_data.items():
                normalized = self.github_sync.normalize_vehicle_data(game_id, raw_data)
                vehicle_id = self.db.insert_vehicle(normalized)
                
                # Clear old relations
                self.db.clear_all_relations(vehicle_id)
                
                # Link buildings
                possible_buildings = raw_data.get('possibleBuildings', [])
                for building_game_id in possible_buildings:
                    building = self.db.get_building_by_name(str(building_game_id))
                    if building:
                        self.db.link_vehicle_building(vehicle_id, building['id'])
            
            self.db.log_sync('vehicles', changes, True)
            log.info(f"Synced vehicles: {len(vehicles_data)} total")
            
            return {'success': True, 'changes': changes}
            
        except Exception as e:
            log.error(f"Error syncing vehicles: {e}", exc_info=True)
            self.db.log_sync('vehicles', {}, False, str(e))
            return {'success': False, 'changes': {}}
    
    async def sync_buildings(self, buildings_data: dict) -> dict:
        """Sync buildings to database."""
        try:
            old_buildings = self.db.get_all_buildings()
            changes = self.github_sync.detect_changes(old_buildings, buildings_data)
            
            for game_id, raw_data in buildings_data.items():
                normalized = self.github_sync.normalize_building_data(game_id, raw_data)
                self.db.insert_building(normalized)
            
            self.db.log_sync('buildings', changes, True)
            log.info(f"Synced buildings: {len(buildings_data)} total")
            
            return {'success': True, 'changes': changes}
            
        except Exception as e:
            log.error(f"Error syncing buildings: {e}", exc_info=True)
            self.db.log_sync('buildings', {}, False, str(e))
            return {'success': False, 'changes': {}}
    
    async def sync_equipment(self, equipment_data: dict) -> dict:
        """Sync equipment to database."""
        try:
            old_equipment = self.db.get_all_equipment()
            changes = self.github_sync.detect_changes(old_equipment, equipment_data)
            
            for game_id, raw_data in equipment_data.items():
                normalized = self.github_sync.normalize_equipment_data(game_id, raw_data)
                self.db.insert_equipment(normalized)
            
            self.db.log_sync('equipment', changes, True)
            log.info(f"Synced equipment: {len(equipment_data)} total")
            
            return {'success': True, 'changes': changes}
            
        except Exception as e:
            log.error(f"Error syncing equipment: {e}", exc_info=True)
            self.db.log_sync('equipment', {}, False, str(e))
            return {'success': False, 'changes': {}}
    
    async def sync_educations(self, educations_data: dict) -> dict:
        """Sync educations to database."""
        try:
            old_educations = self.db.get_all_educations()
            changes = self.github_sync.detect_changes(old_educations, educations_data)
            
            for game_id, raw_data in educations_data.items():
                normalized = self.github_sync.normalize_education_data(game_id, raw_data)
                self.db.insert_education(normalized)
            
            self.db.log_sync('educations', changes, True)
            log.info(f"Synced educations: {len(educations_data)} total")
            
            return {'success': True, 'changes': changes}
            
        except Exception as e:
            log.error(f"Error syncing educations: {e}", exc_info=True)
            self.db.log_sync('educations', {}, False, str(e))
            return {'success': False, 'changes': {}}
    
    async def post_changelog(self, results: dict):
        """Post changelog to configured channel."""
        channel_id = await self.config.changelog_channel_id()
        if not channel_id:
            return
        
        channel = self.bot.get_channel(channel_id)
        if not channel:
            log.warning(f"Changelog channel {channel_id} not found")
            return
        
        # Only post if there are actual changes
        has_changes = False
        for source, data in results.items():
            if data.get('success') and any(data.get('changes', {}).values()):
                has_changes = True
                break
        
        if not has_changes:
            return
        
        # Post embed for each source with changes
        for source, data in results.items():
            if data.get('success') and any(data.get('changes', {}).values()):
                embed = create_sync_changelog_embed(data['changes'], source)
                try:
                    await channel.send(embed=embed)
                except discord.HTTPException as e:
                    log.error(f"Failed to post changelog: {e}")
    
    async def notify_owner_error(self, error_message: str):
        """Notify bot owner of sync error."""
        try:
            app_info = await self.bot.application_info()
            owner = app_info.owner
            
            embed = create_error_embed(
                f"**AssetManager Sync Error**\n\n{error_message}\n\n"
                "Please check the logs for more details."
            )
            
            await owner.send(embed=embed)
        except Exception as e:
            log.error(f"Failed to notify owner: {e}")
    
    # ========== VEHICLE COMMANDS ==========
    
    @commands.group(name="vehicle", aliases=["v"])
    async def vehicle(self, ctx: commands.Context):
        """Vehicle information commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @vehicle.command(name="info")
    async def vehicle_info(self, ctx: commands.Context, *, vehicle_name: str):
        """
        Show detailed information about a vehicle.
        
        Example: [p]vehicle info Type 1 Engine
        """
        async with ctx.typing():
            # Try exact match first
            vehicle = self.db.get_vehicle_by_name(vehicle_name)
            
            # If no exact match, try fuzzy search
            if not vehicle:
                all_vehicles = self.db.get_all_vehicles()
                vehicle_names = [v['name'] for v in all_vehicles]
                matches = get_close_matches(vehicle_name, vehicle_names, n=5, cutoff=0.6)
                
                if not matches:
                    await ctx.send(embed=create_error_embed(
                        f"Vehicle '{vehicle_name}' not found.\n"
                        "Use `[p]vehicles list` to see all available vehicles."
                    ))
                    return
                
                if len(matches) == 1:
                    vehicle = self.db.get_vehicle_by_name(matches[0])
                else:
                    # Multiple matches - let user choose
                    match_list = "\n".join([f"{i+1}. {name}" for i, name in enumerate(matches)])
                    await ctx.send(
                        f"Multiple vehicles found matching '{vehicle_name}':\n```\n{match_list}\n```\n"
                        f"Please be more specific."
                    )
                    return
            
            # Get related data
            buildings = self.db.get_vehicle_buildings(vehicle['id'])
            educations = self.db.get_vehicle_educations(vehicle['id'])
            
            # Create and send embed
            embed = create_vehicle_embed(vehicle, buildings, educations)
            await ctx.send(embed=embed)
    
    @vehicle.command(name="list", aliases=["l", "all"])
    async def vehicle_list(self, ctx: commands.Context):
        """List all available vehicles."""
        async with ctx.typing():
            vehicles = self.db.get_all_vehicles()
            
            if not vehicles:
                await ctx.send(embed=create_error_embed(
                    "No vehicles found in database. "
                    "Use `[p]assetsync` to sync data from GitHub."
                ))
                return
            
            # For now, show first 20
            display_vehicles = vehicles[:20]
            embed = create_list_embed(display_vehicles, "vehicle", 1, 1)
            embed.set_footer(text=f"Showing 20 of {len(vehicles)} vehicles. Use [p]vehicle search for specific vehicles.")
            
            await ctx.send(embed=embed)
    
    @vehicle.command(name="search", aliases=["s", "find"])
    async def vehicle_search(self, ctx: commands.Context, *, query: str):
        """
        Search for vehicles by name.
        
        Example: [p]vehicle search engine
        """
        async with ctx.typing():
            results = self.db.search_vehicles(query)
            
            if not results:
                await ctx.send(embed=create_error_embed(
                    f"No vehicles found matching '{query}'."
                ))
                return
            
            # Show up to 15 results
            display_results = results[:15]
            embed = create_list_embed(display_results, "vehicle", 1, 1)
            
            if len(results) > 15:
                embed.set_footer(text=f"Showing 15 of {len(results)} results. Please refine your search.")
            else:
                embed.set_footer(text=f"Found {len(results)} result(s)")
            
            await ctx.send(embed=embed)
    
    # ========== ADMIN COMMANDS ==========
    
    @commands.command(name="assetsync")
    @checks.is_owner()
    async def manual_sync(self, ctx: commands.Context):
        """Manually sync asset data from GitHub. (Bot owner only)"""
        msg = await ctx.send("🔄 Starting manual sync... This may take a moment.")
        
        try:
            # Test GitHub connection first
            await msg.edit(content="🔄 Testing GitHub connection...")
            test_content = await self.github_sync.fetch_file(
                "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/vehicles.ts"
            )
            
            if not test_content:
                await msg.edit(
                    content=None,
                    embed=create_error_embed(
                        "❌ Failed to connect to GitHub.\n"
                        "Please check your internet connection and try again."
                    )
                )
                return
            
            await msg.edit(content="🔄 Syncing data from GitHub...")
            results = await self.perform_full_sync(auto=False)
            
            # Create summary embed
            embed = discord.Embed(
                title="✅ Sync Completed",
                color=0x00FF00,
                timestamp=datetime.utcnow()
            )
            
            errors = []
            for source, data in results.items():
                if data['success']:
                    changes = data.get('changes', {})
                    added = len(changes.get('added', []))
                    updated = len(changes.get('updated', []))
                    removed = len(changes.get('removed', []))
                    
                    embed.add_field(
                        name=f"📦 {source.title()}",
                        value=f"✅ +{added} | 🔄 {updated} | ❌ {removed}",
                        inline=True
                    )
                else:
                    embed.add_field(
                        name=f"📦 {source.title()}",
                        value="❌ Failed",
                        inline=True
                    )
                    errors.append(f"{source}: Check logs for details")
            
            if errors:
                embed.add_field(
                    name="⚠️ Errors",
                    value="\n".join(errors),
                    inline=False
                )
                embed.color = 0xFFA500  # Orange for partial failure
            
            await msg.edit(content=None, embed=embed)
            
            # Send detailed error info
            if errors:
                last_sync = self.db.get_last_sync()
                await ctx.send(
                    f"⚠️ Some sources failed. Check console logs for details.\n"
                    f"Try `[p]assetdebug` for more information."
                )
            
        except Exception as e:
            log.error(f"Manual sync error: {e}", exc_info=True)
            import traceback
            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            
            await msg.edit(
                content=None,
                embed=create_error_embed(f"Sync failed: {str(e)}")
            )
            
            # Send full traceback in separate message
            for i in range(0, len(tb), 1900):
                await ctx.send(f"```python\n{tb[i:i+1900]}\n```")
    
    @commands.group(name="assetset")
    @checks.admin_or_permissions(manage_guild=True)
    async def asset_settings(self, ctx: commands.Context):
        """Configure AssetManager settings."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @asset_settings.command(name="channel")
    async def set_changelog_channel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel for sync changelogs."""
        await self.config.changelog_channel_id.set(channel.id)
        await ctx.send(embed=create_success_embed(
            f"Changelog channel set to {channel.mention}"
        ))
    
    @asset_settings.command(name="autosync")
    async def toggle_autosync(self, ctx: commands.Context, enabled: bool):
        """Enable or disable automatic daily syncing."""
        await self.config.auto_sync_enabled.set(enabled)
        status = "enabled" if enabled else "disabled"
        await ctx.send(embed=create_success_embed(
            f"Automatic syncing {status}"
        ))
    
    @commands.command(name="assetdebug")
    @checks.is_owner()
    async def debug_sync(self, ctx: commands.Context):
        """Debug sync issues - shows detailed error info."""
        msg = await ctx.send("🔍 Testing GitHub connection...")
        
        try:
            # Test 1: Raw fetch
            await msg.edit(content="🔍 Step 1/5: Testing raw GitHub fetch...")
            url = "https://raw.githubusercontent.com/LSS-Manager/LSSM-V.4/dev/src/i18n/en_US/vehicles.ts"
            
            raw_content = await self.github_sync.fetch_file(url)
            
            if raw_content:
                await ctx.send(f"✅ Raw fetch succeeded! Content length: {len(raw_content)} characters")
                await ctx.send(f"First 500 characters:\n```typescript\n{raw_content[:500]}\n```")
            else:
                await ctx.send("❌ Raw fetch failed")
                return
            
            # Test 2: Manual parse with error details
            await msg.edit(content="🔍 Step 2/5: Testing TypeScript parsing...")
            
            # Try parsing with detailed error reporting
            import traceback
            try:
                parsed = self.github_sync.parse_typescript_export(raw_content)
                if parsed:
                    await ctx.send(f"✅ Parse succeeded! Found {len(parsed)} vehicles")
                    first_key = list(parsed.keys())[0]
                    first_vehicle = parsed[first_key]
                    sample = json.dumps(first_vehicle, indent=2)[:1000]
                    await ctx.send(f"First vehicle (ID {first_key}):\n```json\n{sample}\n```")
                else:
                    await ctx.send("❌ Parse returned None - check Red logs for details")
                    
                    # Show what the parser tried to do
                    await ctx.send("Attempting manual parse to see the error...")
                    
                    # Do manual steps
                    content = raw_content
                    content = re.sub(r'//.*', '', content)
                    content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
                    content = re.sub(r'import\s+.*?;', '', content, flags=re.DOTALL)
                    
                    pattern = r'export\s+default\s+(\{[\s\S]+?\})\s*(?:satisfies|as\s+const)?[^;]*;?\s*


async def setup(bot: Red):
    """Add cog to bot."""
    cog = AssetManager(bot)
    await bot.add_cog(cog)
    await cog.cog_load()

                    match = re.search(pattern, content)
                    
                    if not match:
                        await ctx.send("❌ Could not find export pattern")
                        return
                    
                    obj_str = match.group(1)
                    await ctx.send(f"Extracted object length: {len(obj_str)} chars")
                    
                    # Apply transformations
                    obj_str = re.sub(r'(\d)_(\d)', r'\1\2', obj_str)
                    obj_str = obj_str.replace("'", '"')
                    obj_str = re.sub(r'(\s+)(\d+)(\s*):', r'\1"\2"\3:', obj_str)
                    obj_str = re.sub(r'([,\{]\s*)([a-zA-Z_][a-zA-Z0-9_]*)(\s*):', r'\1"\2"\3:', obj_str)
                    obj_str = re.sub(r',(\s*[\}\]])', r'\1', obj_str)
                    
                    await ctx.send(f"After transformations, first 800 chars:\n```json\n{obj_str[:800]}\n```")
                    
                    # Try to parse and catch specific error
                    try:
                        test_parse = json.loads(obj_str)
                        await ctx.send("✅ Manual parse succeeded!")
                    except json.JSONDecodeError as je:
                        await ctx.send(f"❌ JSON Error at position {je.pos}: {je.msg}")
                        if je.pos:
                            start = max(0, je.pos - 150)
                            end = min(len(obj_str), je.pos + 150)
                            error_context = obj_str[start:end]
                            await ctx.send(f"Error context:\n```\n{error_context}\n```")
                    
                    return
                    
            except Exception as parse_error:
                tb = ''.join(traceback.format_exception(type(parse_error), parse_error, parse_error.__traceback__))
                await ctx.send(f"❌ Parse exception:\n```python\n{tb[:1800]}\n```")
                return
            
            # Test 3: Normalize
            await msg.edit(content="🔍 Step 3/5: Testing data normalization...")
            test_vehicle = self.github_sync.normalize_vehicle_data(
                list(parsed.keys())[0], 
                list(parsed.values())[0]
            )
            await ctx.send(f"✅ Normalization succeeded")
            await ctx.send(f"Normalized:\n```json\n{json.dumps(test_vehicle, indent=2)[:1000]}\n```")
            
            # Test 4: Database insert
            await msg.edit(content="🔍 Step 4/5: Testing database insert...")
            vehicle_id = self.db.insert_vehicle(test_vehicle)
            await ctx.send(f"✅ Database insert succeeded! Vehicle ID: {vehicle_id}")
            
            await msg.edit(content="✅ All tests passed!")
            
        except Exception as e:
            await ctx.send(f"❌ Error:\n```\n{str(e)}\n```")
            import traceback
            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            for i in range(0, len(tb), 1900):
                await ctx.send(f"```python\n{tb[i:i+1900]}\n```")


async def setup(bot: Red):
    """Add cog to bot."""
    cog = AssetManager(bot)
    await bot.add_cog(cog)
    await cog.cog_load()
