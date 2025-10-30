"""
FAQManager Cog - Main Discord Bot Interface
Advanced FAQ system with hybrid commands, Helpshift integration, and fuzzy search.
"""

import discord
from discord import app_commands
from discord.ext import commands
from redbot.core import Config, commands as red_commands, checks
from redbot.core.data_manager import cog_data_path
from redbot.core.utils.chat_formatting import box, pagify

import asyncio
import time
import logging
from typing import Optional, List, Literal
from pathlib import Path

from .models import FAQItem, SearchResult, Source, OutdatedReport
from .database import FAQDatabase
from .synonyms import SynonymManager
from .fuzzy_search import FuzzySearchEngine
from .helpshift_scraper import HelpshiftScraper

log = logging.getLogger("red.faqmanager")


class FAQManager(red_commands.Cog):
    """
    Advanced FAQ system with smart search, Helpshift integration, and role-based management.
    """
    
    # Predefined FAQ categories
    FAQ_CATEGORIES = [
        "Game Mechanics",
        "Economy & Credits",
        "Vehicles & Equipment",
        "Buildings & Stations",
        "Missions & Calls",
        "Alliance & Multiplayer",
        "Map & POIs",
        "Events & Specials",
        "Account & Settings",
        "Technical Support",
        "Getting Started",
        "Advanced Tips"
    ]
    
    def __init__(self, bot):
        self.bot = bot
        
        # Initialize config
        self.config = Config.get_conf(self, identifier=8203918209, force_registration=True)
        
        # Default guild settings
        default_guild = {
            "editor_roles": [],
            "outdated_log_channel": None,
            "suggestion_threshold": 75,
            "autocomplete_ttl": 600,
            "debug_mode": False
        }
        self.config.register_guild(**default_guild)
        
        # Initialize components
        self.data_path = cog_data_path(self) / "faq.db"
        self.database = FAQDatabase(self.data_path)
        self.synonym_manager = SynonymManager()
        self.fuzzy_search = FuzzySearchEngine(self.synonym_manager)
        self.helpshift_scraper = HelpshiftScraper()
        
        # Set database reference for compatibility wrapper
        self.helpshift_scraper.set_database(self.database)
        
        # Initialize crawler
        from .helpshift_scraper import HelpshiftCrawler
        self.crawler = HelpshiftCrawler(self.database, max_concurrency=4)
        
        # In-memory FAQ cache
        self._faq_cache: List[FAQItem] = []
        self._cache_loaded = False
        
        # Start initialization
        self.bot.loop.create_task(self._initialize())
    
    async def _initialize(self):
        """Initialize database and load cache."""
        try:
            await self.database.initialize()
            await self._reload_faq_cache()
            log.info("FAQManager initialized successfully")
        except Exception as e:
            log.error(f"Failed to initialize FAQManager: {e}", exc_info=True)
    
    async def _reload_faq_cache(self):
        """Reload FAQ cache from database."""
        self._faq_cache = await self.database.get_all_faqs()
        self._cache_loaded = True
        log.info(f"Loaded {len(self._faq_cache)} FAQs into cache")
    
    async def _is_editor(self, guild: discord.Guild, member: discord.Member) -> bool:
        """Check if user has editor permissions."""
        if member.guild_permissions.administrator:
            return True
        if member.guild_permissions.manage_guild:
            return True
        
        editor_role_ids = await self.config.guild(guild).editor_roles()
        user_role_ids = [role.id for role in member.roles]
        
        return any(role_id in user_role_ids for role_id in editor_role_ids)
    
    def cog_unload(self):
        """Cleanup on cog unload."""
        asyncio.create_task(self.helpshift_scraper.close())
        asyncio.create_task(self.crawler.close())
    
    # ==================== SEARCH COMMANDS ====================
    
    @red_commands.hybrid_command(name="faqsearch", aliases=["faq"])
    @app_commands.describe(query="Search term or question")
    async def faq_search(self, ctx: red_commands.Context, *, query: str):
        """
        Search for FAQ articles from both custom database and Mission Chief Help Center.
        """
        await ctx.defer(ephemeral=True)
        
        threshold = await self.config.guild(ctx.guild).suggestion_threshold()
        self.fuzzy_search.suggestion_threshold = threshold
        
        try:
            custom_main, custom_suggestions = self.fuzzy_search.search_custom(
                query, self._faq_cache, max_results=5
            )
            
            helpshift_articles = await self.helpshift_scraper.search_all_articles(query, max_articles=10)
            helpshift_main, helpshift_suggestions = self.fuzzy_search.search_helpshift(
                query, helpshift_articles, max_results=5
            )
            
            all_results = []
            if custom_main:
                all_results.append(custom_main)
            all_results.extend(custom_suggestions)
            if helpshift_main:
                all_results.append(helpshift_main)
            all_results.extend(helpshift_suggestions)
            
            all_results.sort(key=lambda x: x.score, reverse=True)
            
            if not all_results:
                embed = discord.Embed(
                    title="❌ No Results Found",
                    description=f"Could not find any FAQs matching **{query}**.\n\nTry different keywords or check spelling.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed, ephemeral=True)
                return
            
            top_result = all_results[0]
            remaining_results = all_results[1:5]
            
            embed = await self._create_result_embed(top_result, query)
            view = FAQResultView(self, top_result, remaining_results, query, ctx.author.id)
            
            await ctx.send(embed=embed, view=view, ephemeral=True)
        
        except Exception as e:
            log.error(f"Error in faq_search: {e}", exc_info=True)
            embed = discord.Embed(
                title="❌ Search Error",
                description="An error occurred while searching. Please try again later.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, ephemeral=True)
    
    @red_commands.hybrid_command(name="faqsuggest")
    @app_commands.describe(query="Search term")
    async def faq_suggest(self, ctx: red_commands.Context, *, query: str):
        """Show only suggestions for a search query (no main result)."""
        await ctx.defer(ephemeral=True)
        
        try:
            helpshift_articles = await self.helpshift_scraper.search_all_articles(query, max_articles=10)
            
            main, suggestions = self.fuzzy_search.search_combined(
                query, self._faq_cache, helpshift_articles, max_results=8
            )
            
            all_suggestions = [main] if main else []
            all_suggestions.extend(suggestions)
            
            if not all_suggestions:
                embed = discord.Embed(
                    title="❌ No Suggestions",
                    description=f"Could not find any suggestions for **{query}**.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed, ephemeral=True)
                return
            
            embed = discord.Embed(
                title=f"💡 Suggestions for: {query}",
                description="Select one of the options below:",
                color=discord.Color.blue()
            )
            
            for i, result in enumerate(all_suggestions[:8], 1):
                source_icon = "📝" if result.source == Source.CUSTOM else "🌐"
                score_bar = self._create_score_bar(result.score)
                embed.add_field(
                    name=f"{i}. {source_icon} {result.title}",
                    value=f"{score_bar} • {result.category or 'General'}",
                    inline=False
                )
            
            view = SuggestionView(self, all_suggestions, query, ctx.author.id)
            await ctx.send(embed=embed, view=view, ephemeral=True)
        
        except Exception as e:
            log.error(f"Error in faq_suggest: {e}", exc_info=True)
            await ctx.send("❌ An error occurred. Please try again.", ephemeral=True)
    
    @red_commands.hybrid_command(name="faqme")
    async def faq_me(self, ctx: red_commands.Context):
        """Open a personal FAQ search mode with an interactive search field."""
        embed = discord.Embed(
            title="🔍 Personal FAQ Search",
            description="Use the button below to start searching FAQs.\n\nThis is a private search just for you!",
            color=discord.Color.green()
        )
        
        view = PersonalSearchView(self, ctx.author.id)
        await ctx.send(embed=embed, view=view, ephemeral=True)
    
    # ==================== ADMIN COMMANDS ====================
    
    @red_commands.hybrid_group(name="faqadmin", aliases=["faqmanage"])
    @checks.admin_or_permissions(manage_guild=True)
    async def faq_admin(self, ctx: red_commands.Context):
        """FAQ management commands (Admin only)."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @faq_admin.command(name="add")
    async def faq_add(self, ctx: red_commands.Context):
        """Add a new custom FAQ entry with category selection."""
        if not await self._is_editor(ctx.guild, ctx.author):
            await ctx.send("❌ You don't have permission to add FAQs.", ephemeral=True)
            return
        
        view = CategorySelectView(self, ctx.author.id)
        embed = discord.Embed(
            title="📝 Add New FAQ",
            description="First, select a category for your FAQ:",
            color=discord.Color.green()
        )
        await ctx.send(embed=embed, view=view, ephemeral=True)
    
    @faq_admin.command(name="edit")
    @app_commands.describe(faq_id="ID of the FAQ to edit")
    async def faq_edit(self, ctx: red_commands.Context, faq_id: int):
        """Edit an existing custom FAQ entry."""
        if not await self._is_editor(ctx.guild, ctx.author):
            await ctx.send("❌ You don't have permission to edit FAQs.", ephemeral=True)
            return
        
        faq = await self.database.get_faq(faq_id)
        if not faq:
            await ctx.send(f"❌ FAQ with ID `{faq_id}` not found.", ephemeral=True)
            return
        
        if not ctx.interaction:
            await ctx.send("❌ This command only works as a slash command. Use `/faqadmin edit` instead.")
            return
        
        modal = EditFAQModal(self, faq, ctx.author.id)
        await ctx.interaction.response.send_modal(modal)
    
    @faq_admin.command(name="remove")
    @app_commands.describe(faq_id="ID of the FAQ to remove")
    async def faq_remove(self, ctx: red_commands.Context, faq_id: int):
        """Remove a custom FAQ entry (requires confirmation)."""
        if not await self._is_editor(ctx.guild, ctx.author):
            await ctx.send("❌ You don't have permission to remove FAQs.", ephemeral=True)
            return
        
        faq = await self.database.get_faq(faq_id)
        if not faq:
            await ctx.send(f"❌ FAQ with ID `{faq_id}` not found.", ephemeral=True)
            return
        
        view = ConfirmDeleteView(self, faq, ctx.author.id)
        embed = discord.Embed(
            title="⚠️ Confirm Deletion",
            description=f"Are you sure you want to delete this FAQ?\n\n**Question:** {faq.question}\n**ID:** {faq_id}",
            color=discord.Color.orange()
        )
        
        await ctx.send(embed=embed, view=view, ephemeral=True)
    
    @faq_admin.command(name="post")
    @app_commands.describe(query="Search for FAQ to post publicly")
    async def faq_post(self, ctx: red_commands.Context, *, query: str):
        """Search and post a FAQ publicly in the current channel."""
        if not await self._is_editor(ctx.guild, ctx.author):
            await ctx.send("❌ You don't have permission to post FAQs.", ephemeral=True)
            return
        
        await ctx.defer(ephemeral=True)
        
        helpshift_articles = await self.helpshift_scraper.search_all_articles(query, max_articles=5)
        main, suggestions = self.fuzzy_search.search_combined(
            query, self._faq_cache, helpshift_articles, max_results=5
        )
        
        all_results = [main] if main else []
        all_results.extend(suggestions)
        
        if not all_results:
            await ctx.send(f"❌ No FAQs found matching **{query}**.", ephemeral=True)
            return
        
        if len(all_results) == 1:
            embed = await self._create_result_embed(all_results[0], query, public=True)
            await ctx.send(embed=embed)
            await ctx.send("✅ FAQ posted!", ephemeral=True)
        else:
            view = PostSelectView(self, all_results, ctx.channel, ctx.author.id)
            embed = discord.Embed(
                title="📤 Select FAQ to Post",
                description="Choose which FAQ to post publicly:",
                color=discord.Color.blue()
            )
            await ctx.send(embed=embed, view=view, ephemeral=True)
    
    @faq_admin.command(name="list")
    async def faq_list(self, ctx: red_commands.Context):
        """List all custom FAQs with their IDs."""
        if not self._faq_cache:
            await ctx.send("📝 No custom FAQs found.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="📚 Custom FAQs",
            description=f"Total: {len(self._faq_cache)} FAQ(s)",
            color=discord.Color.green()
        )
        
        categorized = {}
        for faq in self._faq_cache[:50]:
            category = faq.category or "Uncategorized"
            if category not in categorized:
                categorized[category] = []
            categorized[category].append(faq)
        
        for category, faqs in sorted(categorized.items()):
            lines = []
            for faq in faqs[:10]:
                lines.append(f"`ID {faq.id:03d}` • {faq.question[:60]}")
            
            embed.add_field(
                name=f"📂 {category}",
                value="\n".join(lines),
                inline=False
            )
        
        embed.set_footer(text="Use /faqadmin edit <id> to modify an entry")
        
        await ctx.send(embed=embed, ephemeral=True)
    
    # ==================== ROLE MANAGEMENT ====================
    
    @faq_admin.group(name="roles")
    async def faq_roles(self, ctx: red_commands.Context):
        """Manage editor roles."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @faq_roles.command(name="add")
    @app_commands.describe(role="Role to add as editor")
    async def roles_add(self, ctx: red_commands.Context, role: discord.Role):
        """Add a role as FAQ editor."""
        async with self.config.guild(ctx.guild).editor_roles() as roles:
            if role.id in roles:
                await ctx.send(f"❌ {role.mention} is already an editor role.", ephemeral=True)
                return
            roles.append(role.id)
        
        await ctx.send(f"✅ Added {role.mention} as editor role.", ephemeral=True)
    
    @faq_roles.command(name="remove")
    @app_commands.describe(role="Role to remove from editors")
    async def roles_remove(self, ctx: red_commands.Context, role: discord.Role):
        """Remove a role from FAQ editors."""
        async with self.config.guild(ctx.guild).editor_roles() as roles:
            if role.id not in roles:
                await ctx.send(f"❌ {role.mention} is not an editor role.", ephemeral=True)
                return
            roles.remove(role.id)
        
        await ctx.send(f"✅ Removed {role.mention} from editor roles.", ephemeral=True)
    
    @faq_roles.command(name="list")
    async def roles_list(self, ctx: red_commands.Context):
        """List all editor roles."""
        role_ids = await self.config.guild(ctx.guild).editor_roles()
        
        if not role_ids:
            await ctx.send("📝 No editor roles configured.", ephemeral=True)
            return
        
        roles = [ctx.guild.get_role(rid) for rid in role_ids]
        roles = [r for r in roles if r is not None]
        
        if not roles:
            await ctx.send("⚠️ Editor roles configured but none found.", ephemeral=True)
            return
        
        role_mentions = [r.mention for r in roles]
        embed = discord.Embed(
            title="🛡️ FAQ Editor Roles",
            description="\n".join(role_mentions),
            color=discord.Color.blue()
        )
        
        await ctx.send(embed=embed, ephemeral=True)
    
    # ==================== SETTINGS ====================
    
    @faq_admin.group(name="settings")
    async def faq_settings(self, ctx: red_commands.Context):
        """Configure FAQ system settings."""
        if ctx.invoked_subcommand is None:
            settings = await self.config.guild(ctx.guild).all()
            
            outdated_channel = ctx.guild.get_channel(settings['outdated_log_channel']) if settings['outdated_log_channel'] else None
            
            embed = discord.Embed(
                title="⚙️ FAQ Settings",
                color=discord.Color.blue()
            )
            embed.add_field(name="Suggestion Threshold", value=str(settings['suggestion_threshold']), inline=True)
            embed.add_field(name="Autocomplete TTL", value=f"{settings['autocomplete_ttl']}s", inline=True)
            embed.add_field(name="Debug Mode", value="✅ Enabled" if settings['debug_mode'] else "❌ Disabled", inline=True)
            embed.add_field(name="Outdated Log Channel", value=outdated_channel.mention if outdated_channel else "Not set", inline=False)
            
            await ctx.send(embed=embed, ephemeral=True)
    
    @faq_settings.command(name="threshold")
    @app_commands.describe(value="Score threshold (0-100)")
    async def settings_threshold(self, ctx: red_commands.Context, value: int):
        """Set the suggestion threshold score."""
        if not 0 <= value <= 100:
            await ctx.send("❌ Threshold must be between 0 and 100.", ephemeral=True)
            return
        
        await self.config.guild(ctx.guild).suggestion_threshold.set(value)
        await ctx.send(f"✅ Suggestion threshold set to **{value}**.", ephemeral=True)
    
    @faq_settings.command(name="outdatedlog")
    @app_commands.describe(channel="Channel for outdated reports")
    async def settings_outdated_log(self, ctx: red_commands.Context, channel: discord.TextChannel):
        """Set the channel for outdated content reports."""
        await self.config.guild(ctx.guild).outdated_log_channel.set(channel.id)
        await ctx.send(f"✅ Outdated reports will be sent to {channel.mention}.", ephemeral=True)
    
    @faq_settings.command(name="debug")
    @app_commands.describe(enabled="Enable or disable debug mode")
    async def settings_debug(self, ctx: red_commands.Context, enabled: bool):
        """Toggle debug logging."""
        await self.config.guild(ctx.guild).debug_mode.set(enabled)
        
        if enabled:
            log.setLevel(logging.DEBUG)
            await ctx.send("✅ Debug mode **enabled**.", ephemeral=True)
        else:
            log.setLevel(logging.INFO)
            await ctx.send("✅ Debug mode **disabled**.", ephemeral=True)
    
    # ==================== CRAWL COMMANDS ====================
    
    @red_commands.hybrid_group(name="faqcrawl", aliases=["crawl"])
    @checks.admin_or_permissions(manage_guild=True)
    async def faq_crawl_group(self, ctx: red_commands.Context):
        """Helpshift crawler management commands."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @faq_crawl_group.command(name="now")
    async def crawl_now(self, ctx: red_commands.Context):
        """Start a full crawl immediately."""
        if ctx.interaction:
            await ctx.defer(ephemeral=True)
            send_msg = lambda content=None, embed=None: ctx.send(content=content, embed=embed, ephemeral=True)
        else:
            send_msg = ctx.send
        
        await send_msg("🔄 Starting Helpshift crawl... This may take several minutes.")
        
        try:
            report = await self.crawler.crawl_full()
            
            embed = discord.Embed(
                title="📊 Crawl Report",
                color=discord.Color.green() if not report.errors else discord.Color.orange()
            )
            
            embed.add_field(name="Duration", value=f"{report.duration_seconds:.1f}s", inline=True)
            embed.add_field(name="Sections", value=str(report.sections_found), inline=True)
            embed.add_field(name="Total Articles", value=str(report.articles_total), inline=True)
            
            embed.add_field(name="📝 New", value=str(report.articles_new), inline=True)
            embed.add_field(name="🔄 Updated", value=str(report.articles_updated), inline=True)
            embed.add_field(name="✓ Unchanged", value=str(report.articles_unchanged), inline=True)
            
            if report.articles_deleted > 0:
                embed.add_field(name="🗑️ Deleted", value=str(report.articles_deleted), inline=True)
            
            if report.errors:
                error_text = "\n".join(f"• {err[:100]}" for err in report.errors[:5])
                if len(report.errors) > 5:
                    error_text += f"\n... and {len(report.errors) - 5} more errors"
                embed.add_field(name="⚠️ Errors", value=error_text, inline=False)
            
            embed.set_footer(text=f"Started: {report.started_at}")
            
            await ctx.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            log.error(f"Crawl failed: {e}", exc_info=True)
            await ctx.send(f"❌ Crawl failed: {str(e)}", ephemeral=True)
    
    @faq_crawl_group.command(name="status")
    async def crawl_status(self, ctx: red_commands.Context):
        """Show the last crawl report."""
        try:
            report = await self.database.get_last_crawl_report()
            
            if not report:
                await ctx.send("📭 No crawl reports found. Run `/faqcrawl now` to start a crawl.", ephemeral=True)
                return
            
            embed = discord.Embed(
                title="📊 Last Crawl Report",
                color=discord.Color.blue()
            )
            
            embed.add_field(name="Started", value=report.started_at, inline=True)
            embed.add_field(name="Duration", value=f"{report.duration_seconds:.1f}s", inline=True)
            embed.add_field(name="Sections", value=str(report.sections_found), inline=True)
            
            embed.add_field(name="Total Articles", value=str(report.articles_total), inline=True)
            embed.add_field(name="📝 New", value=str(report.articles_new), inline=True)
            embed.add_field(name="🔄 Updated", value=str(report.articles_updated), inline=True)
            
            embed.add_field(name="✓ Unchanged", value=str(report.articles_unchanged), inline=True)
            embed.add_field(name="🗑️ Deleted", value=str(report.articles_deleted), inline=True)
            embed.add_field(name="Errors", value=str(len(report.errors)), inline=True)
            
            if report.errors:
                error_text = "\n".join(f"• {err[:100]}" for err in report.errors[:3])
                if len(report.errors) > 3:
                    error_text += f"\n... and {len(report.errors) - 3} more"
                embed.add_field(name="⚠️ Recent Errors", value=error_text, inline=False)
            
            stats = await self.database.get_statistics()
            embed.add_field(
                name="📚 Database",
                value=f"Articles: {stats['helpshift_articles']}\nSections: {stats['helpshift_sections']}",
                inline=False
            )
            
            await ctx.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            log.error(f"Failed to get crawl status: {e}", exc_info=True)
            await ctx.send(f"❌ Error getting status: {str(e)}", ephemeral=True)
    
    @faq_crawl_group.command(name="test")
    async def crawl_test(self, ctx: red_commands.Context):
        """Run a test crawl (doesn't save to database)."""
        await ctx.defer(ephemeral=True)
        
        try:
            results = await self.crawler.test_crawl(max_sections=2, max_articles=3)
            
            if 'error' in results:
                await ctx.send(f"❌ Test failed: {results['error']}", ephemeral=True)
                return
            
            embed = discord.Embed(
                title="🧪 Test Crawl Results",
                description=f"Found {results['sections_found']} sections on home page",
                color=discord.Color.blue()
            )
            
            if results['sections_tested']:
                section_text = "\n".join(
                    f"• {s['name']}" 
                    for s in results['sections_tested']
                )
                embed.add_field(name="Sections Tested", value=section_text, inline=False)
            
            if results['articles_tested']:
                article_text = "\n".join(
                    f"• {a['title'][:60]} ({a['body_length']} chars)"
                    for a in results['articles_tested'][:5]
                )
                embed.add_field(
                    name=f"Articles Tested ({len(results['articles_tested'])})",
                    value=article_text,
                    inline=False
                )
                
                if results['articles_tested']:
                    first = results['articles_tested'][0]
                    embed.add_field(
                        name="Preview",
                        value=first['body_preview'][:200],
                        inline=False
                    )
            
            await ctx.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            log.error(f"Test crawl failed: {e}", exc_info=True)
            await ctx.send(f"❌ Test failed: {str(e)}", ephemeral=True)
    
    # ==================== AUTOCOMPLETE ====================
    
    @faq_search.autocomplete('query')
    @faq_post.autocomplete('query')
    async def search_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """Autocomplete for search queries."""
        if not current or len(current) < 2:
            return []
        
        try:
            helpshift_titles = self.helpshift_scraper.get_cached_titles()
            
            results = self.fuzzy_search.autocomplete_search(
                current,
                self._faq_cache,
                helpshift_titles,
                max_results=20
            )
            
            choices = [
                app_commands.Choice(name=title[:100], value=title[:100])
                for title, score in results
            ]
            
            return choices[:25]
        
        except Exception as e:
            log.error(f"Autocomplete error: {e}")
            return []
    
    # ==================== HELPER METHODS ====================
    
    async def _create_result_embed(self, result: SearchResult, query: str, public: bool = False) -> discord.Embed:
        """Create an embed for a search result."""
        if result.source == Source.CUSTOM:
            color = discord.Color.green()
            source_text = "FARA Custom"
        elif result.source == Source.HELPSHIFT_LOCAL:
            color = discord.Color.blue()
            source_text = "Mission Chief Help Center (Local)"
        else:
            color = discord.Color.blurple()
            source_text = "Mission Chief Help Center (Live)"
        
        # Discord embed description limit is 4096 chars
        # We show up to 1900 chars to leave room for formatting
        MAX_EMBED_LENGTH = 1900
        full_content = result.content
        
        # Check if content is truncated
        is_truncated = len(full_content) > MAX_EMBED_LENGTH
        display_content = result.get_excerpt(MAX_EMBED_LENGTH) if is_truncated else full_content
        
        # Add truncation notice
        if is_truncated:
            display_content += f"\n\n*[Content truncated - {len(full_content)} total characters. Click 'View Full Answer' to see complete text]*"
        
        embed = discord.Embed(
            title=result.title,
            description=display_content,
            color=color
        )
        
        if result.category:
            embed.add_field(name="📂 Category", value=result.category, inline=True)
        
        if result.last_updated:
            embed.add_field(name="🕒 Last Updated", value=result.last_updated, inline=True)
        
        if result.source == Source.CUSTOM and result.faq_id:
            embed.add_field(name="🔢 FAQ ID", value=f"`{result.faq_id}`", inline=True)
        
        if result.source == Source.HELPSHIFT_LOCAL and result.article_id:
            embed.add_field(name="🔢 Article ID", value=f"`{result.article_id}`", inline=True)
        
        footer_parts = [f"Source: {source_text}"]
        if result.source == Source.CUSTOM and result.faq_id:
            footer_parts.append(f"ID: {result.faq_id}")
        elif result.source == Source.HELPSHIFT_LOCAL and result.article_id:
            footer_parts.append(f"Article ID: {result.article_id}")
        footer_parts.append(f"Score: {result.score:.0f}")
        
        embed.set_footer(text=" • ".join(footer_parts))
        
        return embed
    
    def _create_score_bar(self, score: float) -> str:
        """Create a visual score bar."""
        filled = int(score / 10)
        empty = 10 - filled
        return f"[{'█' * filled}{'░' * empty}] {score:.0f}%"
    
    async def _log_outdated_report(self, report: OutdatedReport, guild: discord.Guild):
        """Log an outdated content report."""
        channel_id = await self.config.guild(guild).outdated_log_channel()
        if not channel_id:
            return
        
        channel = guild.get_channel(channel_id)
        if not channel:
            return
        
        reporter = guild.get_member(report.reporter_id)
        report_channel = guild.get_channel(report.channel_id)
        
        embed = discord.Embed(
            title="⚠️ Outdated Content Report",
            color=discord.Color.orange(),
            timestamp=discord.utils.utcnow()
        )
        
        embed.add_field(name="Title", value=report.title, inline=False)
        embed.add_field(name="Source", value=report.source.value.title(), inline=True)
        embed.add_field(name="Reported By", value=reporter.mention if reporter else "Unknown", inline=True)
        embed.add_field(name="Channel", value=report_channel.mention if report_channel else "Unknown", inline=True)
        embed.add_field(name="Search Query", value=f"`{report.query}`", inline=False)
        
        if report.url:
            embed.add_field(name="URL", value=report.url, inline=False)
        
        try:
            await channel.send(embed=embed)
        except discord.Forbidden:
            log.warning(f"Cannot send to outdated log channel {channel_id}")


# ==================== VIEWS & MODALS ====================

class FAQResultView(discord.ui.View):
    """View for FAQ search results with suggestions button."""
    
    def __init__(self, cog: FAQManager, main_result: SearchResult, suggestions: List[SearchResult], query: str, user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.main_result = main_result
        self.suggestions = suggestions
        self.query = query
        self.user_id = user_id
        
        if main_result.url:
            self.add_item(discord.ui.Button(
                label="View on Help Center",
                url=main_result.url,
                style=discord.ButtonStyle.link
            ))
        
        # Add "View Full Answer" button if content is long
        if len(main_result.content) > 1900:
            self.view_full_button = discord.ui.Button(
                label="📄 View Full Answer",
                style=discord.ButtonStyle.primary,
                emoji="📄"
            )
            self.view_full_button.callback = self.view_full_callback
            self.add_item(self.view_full_button)
        
        if suggestions:
            self.show_suggestions_button = discord.ui.Button(
                label=f"Show {len(suggestions)} Suggestions",
                style=discord.ButtonStyle.secondary
            )
            self.show_suggestions_button.callback = self.show_suggestions_callback
            self.add_item(self.show_suggestions_button)
        
        self.outdated_button = discord.ui.Button(
            label="Report Outdated",
            style=discord.ButtonStyle.danger,
            emoji="⚠️"
        )
        self.outdated_button.callback = self.report_outdated_callback
        self.add_item(self.outdated_button)
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id
    
    async def view_full_callback(self, interaction: discord.Interaction):
        """Show full answer text in a new message."""
        await interaction.response.defer(ephemeral=True)
        
        # Split long content into chunks (Discord message limit: 2000 chars)
        content = self.main_result.content
        
        if len(content) <= 1900:
            # Fits in one message
            embed = discord.Embed(
                title=f"📄 Full Answer: {self.main_result.title}",
                description=content,
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            # Need to split into multiple messages
            chunks = []
            current_chunk = ""
            
            # Split by paragraphs first
            paragraphs = content.split('\n\n')
            
            for para in paragraphs:
                if len(current_chunk) + len(para) + 2 <= 1900:
                    current_chunk += para + "\n\n"
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = para + "\n\n"
            
            if current_chunk:
                chunks.append(current_chunk.strip())
            
            # Send first chunk with title
            embed = discord.Embed(
                title=f"📄 Full Answer: {self.main_result.title} (Part 1/{len(chunks)})",
                description=chunks[0],
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Send remaining chunks
            for i, chunk in enumerate(chunks[1:], 2):
                embed = discord.Embed(
                    title=f"📄 Full Answer: {self.main_result.title} (Part {i}/{len(chunks)})",
                    description=chunk,
                    color=discord.Color.green()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def show_suggestions_callback(self, interaction: discord.Interaction):
        """Show suggestions embed."""
        embed = discord.Embed(
            title=f"💡 More results for: {self.query}",
            color=discord.Color.blue()
        )
        
        for i, result in enumerate(self.suggestions, 1):
            source_icon = "📝" if result.source == Source.CUSTOM else "🌐"
            embed.add_field(
                name=f"{i}. {source_icon} {result.title}",
                value=f"Score: {result.score:.0f} • {result.category or 'General'}",
                inline=False
            )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)
    
    async def report_outdated_callback(self, interaction: discord.Interaction):
        """Handle outdated report - show confirmation first."""
        view = ConfirmOutdatedView(self.cog, self.main_result, self.query, interaction.user.id, interaction.channel_id, interaction.guild)
        
        embed = discord.Embed(
            title="⚠️ Report Outdated Content",
            description=f"Are you sure you want to report this content as outdated?\n\n**Title:** {self.main_result.title}\n**Source:** {self.main_result.source.value.title()}",
            color=discord.Color.orange()
        )
        embed.set_footer(text="This will notify the moderators")
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)


class SuggestionView(discord.ui.View):
    """View for showing multiple suggestions with select menu."""
    
    def __init__(self, cog: FAQManager, results: List[SearchResult], query: str, user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.results = results
        self.query = query
        self.user_id = user_id
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class PersonalSearchView(discord.ui.View):
    """View for personal search mode."""
    
    def __init__(self, cog: FAQManager, user_id: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.user_id = user_id
    
    @discord.ui.button(label="Start Search", style=discord.ButtonStyle.primary, emoji="🔍")
    async def start_search(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SearchModal(self.cog)
        await interaction.response.send_modal(modal)
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class SearchModal(discord.ui.Modal, title="Search FAQs"):
    """Modal for search input."""
    
    search_query = discord.ui.TextInput(
        label="Search Query",
        placeholder="Enter your question or search term...",
        style=discord.TextStyle.short,
        required=True,
        min_length=2,
        max_length=200
    )
    
    def __init__(self, cog: FAQManager):
        super().__init__()
        self.cog = cog
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        query = self.search_query.value
        
        helpshift_articles = await self.cog.helpshift_scraper.search_all_articles(query, max_articles=10)
        main, suggestions = self.cog.fuzzy_search.search_combined(
            query, self.cog._faq_cache, helpshift_articles, max_results=5
        )
        
        if not main and not suggestions:
            await interaction.followup.send("❌ No results found.", ephemeral=True)
            return
        
        result = main or suggestions[0]
        embed = await self.cog._create_result_embed(result, query)
        
        await interaction.followup.send(embed=embed, ephemeral=True)


class EditFAQModal(discord.ui.Modal, title="Edit FAQ"):
    """Modal for editing an existing FAQ."""
    
    question = discord.ui.TextInput(
        label="Question",
        style=discord.TextStyle.short,
        required=True,
        max_length=300
    )
    
    answer = discord.ui.TextInput(
        label="Answer (max 2000 chars)",
        placeholder="Long answers will show 'View Full Answer' button",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000
    )
    
    category = discord.ui.TextInput(
        label="Category",
        style=discord.TextStyle.short,
        required=False,
        max_length=100
    )
    
    synonyms = discord.ui.TextInput(
        label="Synonyms",
        style=discord.TextStyle.short,
        required=False,
        max_length=500
    )
    
    def __init__(self, cog: FAQManager, faq: FAQItem, editor_id: int):
        super().__init__()
        self.cog = cog
        self.faq = faq
        self.editor_id = editor_id
        
        self.question.default = faq.question
        self.answer.default = faq.answer_md
        self.category.default = faq.category or ""
        self.synonyms.default = ", ".join(faq.synonyms) if faq.synonyms else ""
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        self.faq.question = self.question.value
        self.faq.answer_md = self.answer.value
        self.faq.category = self.category.value or None
        self.faq.synonyms = [s.strip() for s in self.synonyms.value.split(',')] if self.synonyms.value else []
        
        try:
            success = await self.cog.database.update_faq(self.faq, self.editor_id)
            if success:
                await self.cog._reload_faq_cache()
                await interaction.followup.send(f"✅ FAQ **{self.faq.id}** updated successfully.", ephemeral=True)
            else:
                await interaction.followup.send("❌ Failed to update FAQ.", ephemeral=True)
        
        except Exception as e:
            log.error(f"Error updating FAQ: {e}", exc_info=True)
            await interaction.followup.send("❌ An error occurred. Please try again.", ephemeral=True)


class ConfirmDeleteView(discord.ui.View):
    """View for confirming FAQ deletion."""
    
    def __init__(self, cog: FAQManager, faq: FAQItem, user_id: int):
        super().__init__(timeout=60)
        self.cog = cog
        self.faq = faq
        self.user_id = user_id
    
    @discord.ui.button(label="Confirm Delete", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        try:
            await self.cog.database.delete_faq(self.faq.id, soft=True)
            await self.cog._reload_faq_cache()
            
            await interaction.followup.send(f"✅ FAQ **{self.faq.id}** has been deleted.", ephemeral=True)
            self.stop()
        
        except Exception as e:
            log.error(f"Error deleting FAQ: {e}", exc_info=True)
            await interaction.followup.send("❌ Failed to delete FAQ.", ephemeral=True)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Deletion cancelled.", ephemeral=True)
        self.stop()
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class PostSelectView(discord.ui.View):
    """View for selecting which FAQ to post publicly."""
    
    def __init__(self, cog: FAQManager, results: List[SearchResult], channel: discord.TextChannel, user_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.results = results
        self.channel = channel
        self.user_id = user_id
        
        options = []
        for i, result in enumerate(results[:25], 1):
            source_icon = "📝" if result.source == Source.CUSTOM else "🌐"
            options.append(discord.SelectOption(
                label=f"{source_icon} {result.title[:80]}",
                description=f"Score: {result.score:.0f} • {result.category or 'General'}",
                value=str(i - 1)
            ))
        
        select = discord.ui.Select(
            placeholder="Select FAQ to post...",
            options=options
        )
        select.callback = self.select_callback
        self.add_item(select)
    
    async def select_callback(self, interaction: discord.Interaction):
        index = int(interaction.data['values'][0])
        result = self.results[index]
        
        embed = await self.cog._create_result_embed(result, "", public=True)
        
        try:
            await self.channel.send(embed=embed)
            await interaction.response.send_message("✅ FAQ posted successfully!", ephemeral=True)
            self.stop()
        except discord.Forbidden:
            await interaction.response.send_message("❌ I don't have permission to post in that channel.", ephemeral=True)
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class ConfirmOutdatedView(discord.ui.View):
    """View for confirming outdated content report."""
    
    def __init__(self, cog: FAQManager, result: SearchResult, query: str, user_id: int, channel_id: int, guild: discord.Guild):
        super().__init__(timeout=60)
        self.cog = cog
        self.result = result
        self.query = query
        self.user_id = user_id
        self.channel_id = channel_id
        self.guild = guild
    
    @discord.ui.button(label="Confirm Report", style=discord.ButtonStyle.danger, emoji="⚠️")
    async def confirm_report(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Confirm and submit the outdated report."""
        report = OutdatedReport(
            source=self.result.source,
            title=self.result.title,
            reporter_id=self.user_id,
            channel_id=self.channel_id,
            query=self.query,
            timestamp=int(time.time()),
            url=self.result.url,
            faq_id=self.result.faq_id
        )
        
        await self.cog._log_outdated_report(report, self.guild)
        await interaction.response.send_message("✅ Thank you! Outdated content has been reported to moderators.", ephemeral=True)
        self.stop()
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the report."""
        await interaction.response.send_message("❌ Report cancelled.", ephemeral=True)
        self.stop()
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class CategorySelectView(discord.ui.View):
    """View for selecting FAQ category."""
    
    def __init__(self, cog: FAQManager, user_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id
        
        options = [
            discord.SelectOption(label=category, value=category)
            for category in cog.FAQ_CATEGORIES
        ]
        
        select = discord.ui.Select(
            placeholder="Choose a category...",
            options=options,
            custom_id="category_select"
        )
        select.callback = self.category_selected
        self.add_item(select)
    
    async def category_selected(self, interaction: discord.Interaction):
        """Handle category selection."""
        category = interaction.data['values'][0]
        
        modal = AddFAQModalWithCategory(self.cog, interaction.user.id, category)
        await interaction.response.send_modal(modal)
        self.stop()
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class AddFAQModalWithCategory(discord.ui.Modal, title="Add New FAQ"):
    """Modal for adding a new FAQ with pre-selected category."""
    
    question = discord.ui.TextInput(
        label="Question",
        placeholder="What is ARR?",
        style=discord.TextStyle.short,
        required=True,
        max_length=300
    )
    
    answer = discord.ui.TextInput(
        label="Answer (Markdown supported, max 2000 chars)",
        placeholder="Full answer text. Long answers will show 'View Full Answer' button.",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000
    )
    
    synonyms = discord.ui.TextInput(
        label="Synonyms (optional - leave empty for preview)",
        placeholder="Leave empty to preview auto-generated synonyms",
        style=discord.TextStyle.short,
        required=False,
        max_length=500
    )
    
    def __init__(self, cog: FAQManager, author_id: int, category: str):
        super().__init__()
        self.cog = cog
        self.author_id = author_id
        self.category = category
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        if not self.synonyms.value or self.synonyms.value.strip() == "":
            # Auto-generate and show preview
            synonym_list = self._auto_generate_synonyms(
                self.question.value,
                self.answer.value
            )
            
            view = SynonymPreviewView(
                self.cog,
                self.author_id,
                self.question.value,
                self.answer.value,
                self.category,
                synonym_list
            )
            
            synonym_text = ", ".join(synonym_list) if synonym_list else "None"
            
            embed = discord.Embed(
                title="🔍 Preview Auto-Generated Synonyms",
                color=discord.Color.blue()
            )
            embed.add_field(name="Question", value=self.question.value, inline=False)
            embed.add_field(name="Category", value=self.category, inline=True)
            embed.add_field(name="🤖 Auto-Generated Synonyms", value=synonym_text[:1024], inline=False)
            embed.set_footer(text="Click 'Accept' to save, or 'Edit' to modify synonyms")
            
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        else:
            # User provided synonyms manually - save directly
            synonym_list = [s.strip() for s in self.synonyms.value.split(',')]
            await self._save_faq(interaction, synonym_list)
    
    async def _save_faq(self, interaction: discord.Interaction, synonym_list: List[str]):
        """Save FAQ to database."""
        faq = FAQItem(
            question=self.question.value,
            answer_md=self.answer.value,
            category=self.category,
            synonyms=synonym_list,
            author_id=self.author_id
        )
        
        try:
            faq_id = await self.cog.database.add_faq(faq)
            await self.cog._reload_faq_cache()
            
            synonym_text = ", ".join(synonym_list) if synonym_list else "None"
            
            embed = discord.Embed(
                title="✅ FAQ Added Successfully",
                color=discord.Color.green()
            )
            embed.add_field(name="Question", value=self.question.value, inline=False)
            embed.add_field(name="Category", value=self.category, inline=True)
            embed.add_field(name="FAQ ID", value=f"`{faq_id}`", inline=True)
            embed.add_field(name="Synonyms", value=synonym_text[:1024], inline=False)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            log.error(f"Error adding FAQ: {e}", exc_info=True)
            await interaction.followup.send("❌ Failed to add FAQ. Please try again.", ephemeral=True)
    
    def _auto_generate_synonyms(self, question: str, answer: str) -> List[str]:
        """Auto-generate synonyms from question and answer text."""
        stop_words = {
            'the', 'is', 'at', 'which', 'on', 'a', 'an', 'as', 'are', 'was', 'were',
            'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
            'would', 'should', 'could', 'may', 'might', 'must', 'can', 'to', 'of', 'in',
            'for', 'with', 'about', 'how', 'what', 'when', 'where', 'why', 'who', 'i',
            'you', 'my', 'your', 'this', 'that', 'these', 'those', 'it', 'its', 'there',
            'their', 'they', 'them', 'or', 'and', 'but', 'if', 'then', 'than', 'so'
        }
        
        text = (question + " " + answer).lower()
        
        import re
        words = re.findall(r'\b\w+\b', text)
        
        word_counts = {}
        question_words = set(re.findall(r'\b\w+\b', question.lower()))
        
        for word in words:
            if len(word) >= 3 and word not in stop_words:
                word_counts[word] = word_counts.get(word, 0) + 1
        
        synonyms = []
        for word, count in word_counts.items():
            if word in question_words or count >= 2:
                if word not in synonyms:
                    synonyms.append(word)
        
        synonyms = sorted(synonyms, key=lambda w: (
            2 if w in question_words else 1,
            word_counts.get(w, 0)
        ), reverse=True)[:10]
        
        return synonyms


class SynonymPreviewView(discord.ui.View):
    """View for previewing and accepting/editing auto-generated synonyms."""
    
    def __init__(
        self,
        cog: FAQManager,
        user_id: int,
        question: str,
        answer: str,
        category: str,
        synonyms: List[str]
    ):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id
        self.question = question
        self.answer = answer
        self.category = category
        self.synonyms = synonyms
    
    @discord.ui.button(label="✅ Accept & Save", style=discord.ButtonStyle.success)
    async def accept_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Accept auto-generated synonyms and save FAQ."""
        await interaction.response.defer(ephemeral=True)
        
        faq = FAQItem(
            question=self.question,
            answer_md=self.answer,
            category=self.category,
            synonyms=self.synonyms,
            author_id=self.user_id
        )
        
        try:
            faq_id = await self.cog.database.add_faq(faq)
            await self.cog._reload_faq_cache()
            
            synonym_text = ", ".join(self.synonyms) if self.synonyms else "None"
            
            embed = discord.Embed(
                title="✅ FAQ Added Successfully",
                color=discord.Color.green()
            )
            embed.add_field(name="Question", value=self.question, inline=False)
            embed.add_field(name="Category", value=self.category, inline=True)
            embed.add_field(name="FAQ ID", value=f"`{faq_id}`", inline=True)
            embed.add_field(name="Synonyms", value=synonym_text[:1024], inline=False)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            self.stop()
        
        except Exception as e:
            log.error(f"Error adding FAQ: {e}", exc_info=True)
            await interaction.followup.send("❌ Failed to add FAQ. Please try again.", ephemeral=True)
    
    @discord.ui.button(label="✏️ Edit Synonyms", style=discord.ButtonStyle.primary)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open modal to edit synonyms before saving."""
        modal = EditSynonymsModal(
            self.cog,
            self.user_id,
            self.question,
            self.answer,
            self.category,
            self.synonyms
        )
        await interaction.response.send_modal(modal)
        self.stop()
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel FAQ creation."""
        await interaction.response.send_message("❌ FAQ creation cancelled.", ephemeral=True)
        self.stop()
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id


class EditSynonymsModal(discord.ui.Modal, title="Edit Synonyms"):
    """Modal for editing auto-generated synonyms."""
    
    synonyms_input = discord.ui.TextInput(
        label="Synonyms (comma separated)",
        placeholder="arr, alarm rules, response regulation",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500
    )
    
    def __init__(
        self,
        cog: FAQManager,
        author_id: int,
        question: str,
        answer: str,
        category: str,
        current_synonyms: List[str]
    ):
        super().__init__()
        self.cog = cog
        self.author_id = author_id
        self.question = question
        self.answer = answer
        self.category = category
        
        self.synonyms_input.default = ", ".join(current_synonyms)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        synonym_list = [s.strip() for s in self.synonyms_input.value.split(',')] if self.synonyms_input.value else []
        
        faq = FAQItem(
            question=self.question,
            answer_md=self.answer,
            category=self.category,
            synonyms=synonym_list,
            author_id=self.author_id
        )
        
        try:
            faq_id = await self.cog.database.add_faq(faq)
            await self.cog._reload_faq_cache()
            
            synonym_text = ", ".join(synonym_list) if synonym_list else "None"
            
            embed = discord.Embed(
                title="✅ FAQ Added Successfully",
                color=discord.Color.green()
            )
            embed.add_field(name="Question", value=self.question, inline=False)
            embed.add_field(name="Category", value=self.category, inline=True)
            embed.add_field(name="FAQ ID", value=f"`{faq_id}`", inline=True)
            embed.add_field(name="Edited Synonyms", value=synonym_text[:1024], inline=False)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            log.error(f"Error adding FAQ: {e}", exc_info=True)
            await interaction.followup.send("❌ Failed to add FAQ. Please try again.", ephemeral=True)
