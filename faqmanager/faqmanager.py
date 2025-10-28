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
    
    def __init__(self, bot):
        self.bot = bot
        
        # Initialize config
        self.config = Config.get_conf(self, identifier=8203918209, force_registration=True)
        
        # Default guild settings
        default_guild = {
            "editor_roles": [],  # List of role IDs that can edit FAQs
            "outdated_log_channel": None,  # Channel ID for outdated reports
            "suggestion_threshold": 75,  # Score threshold for showing suggestions
            "autocomplete_ttl": 600,  # Autocomplete cache TTL (10 min)
            "debug_mode": False  # Enable debug logging
        }
        self.config.register_guild(**default_guild)
        
        # Initialize components
        self.data_path = cog_data_path(self) / "faq.db"
        self.database = FAQDatabase(self.data_path)
        self.synonym_manager = SynonymManager()
        self.fuzzy_search = FuzzySearchEngine(self.synonym_manager)
        self.helpshift_scraper = HelpshiftScraper()
        
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
        # Server admins always have access
        if member.guild_permissions.administrator:
            return True
        if member.guild_permissions.manage_guild:
            return True
        
        # Check configured editor roles
        editor_role_ids = await self.config.guild(guild).editor_roles()
        user_role_ids = [role.id for role in member.roles]
        
        return any(role_id in user_role_ids for role_id in editor_role_ids)
    
    def cog_unload(self):
        """Cleanup on cog unload."""
        asyncio.create_task(self.helpshift_scraper.close())
    
    # ==================== SEARCH COMMANDS ====================
    
    @red_commands.hybrid_command(name="faqsearch", aliases=["faq"])
    @app_commands.describe(query="Search term or question")
    async def faq_search(self, ctx: red_commands.Context, *, query: str):
        """
        Search for FAQ articles from both custom database and Mission Chief Help Center.
        """
        await ctx.defer(ephemeral=True)
        
        # Get settings
        threshold = await self.config.guild(ctx.guild).suggestion_threshold()
        self.fuzzy_search.suggestion_threshold = threshold
        
        try:
            # Search custom FAQs
            custom_main, custom_suggestions = self.fuzzy_search.search_custom(
                query, self._faq_cache, max_results=5
            )
            
            # Search Helpshift (async)
            helpshift_articles = await self.helpshift_scraper.search_all_articles(query, max_articles=10)
            helpshift_main, helpshift_suggestions = self.fuzzy_search.search_helpshift(
                query, helpshift_articles, max_results=5
            )
            
            # Combine results (custom gets slight boost)
            all_results = []
            if custom_main:
                all_results.append(custom_main)
            all_results.extend(custom_suggestions)
            if helpshift_main:
                all_results.append(helpshift_main)
            all_results.extend(helpshift_suggestions)
            
            # Sort by score
            all_results.sort(key=lambda x: x.score, reverse=True)
            
            if not all_results:
                embed = discord.Embed(
                    title="❌ No Results Found",
                    description=f"Could not find any FAQs matching **{query}**.\n\nTry different keywords or check spelling.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed, ephemeral=True)
                return
            
            # Determine main result
            top_result = all_results[0]
            remaining_results = all_results[1:5]
            
            # Create embed
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
        """
        Show only suggestions for a search query (no main result).
        """
        await ctx.defer(ephemeral=True)
        
        try:
            # Combined search
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
            
            # Create suggestion embed
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
        """
        Open a personal FAQ search mode with an interactive search field.
        """
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
        """Add a new custom FAQ entry."""
        # Check permissions
        if not await self._is_editor(ctx.guild, ctx.author):
            await ctx.send("❌ You don't have permission to add FAQs.", ephemeral=True)
            return
        
        # Modals only work with slash commands
        if not ctx.interaction:
            await ctx.send("❌ This command only works as a slash command. Use `/faqadmin add` instead.")
            return
        
        modal = AddFAQModal(self, ctx.author.id)
        await ctx.interaction.response.send_modal(modal)
    
    @faq_admin.command(name="edit")
    @app_commands.describe(faq_id="ID of the FAQ to edit")
    async def faq_edit(self, ctx: red_commands.Context, faq_id: int):
        """Edit an existing custom FAQ entry."""
        if not await self._is_editor(ctx.guild, ctx.author):
            await ctx.send("❌ You don't have permission to edit FAQs.", ephemeral=True)
            return
        
        # Get FAQ
        faq = await self.database.get_faq(faq_id)
        if not faq:
            await ctx.send(f"❌ FAQ with ID `{faq_id}` not found.", ephemeral=True)
            return
        
        # Modals only work with slash commands
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
        
        # Search for FAQ
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
            # Post immediately
            embed = await self._create_result_embed(all_results[0], query, public=True)
            await ctx.send(embed=embed)
            await ctx.send("✅ FAQ posted!", ephemeral=True)
        else:
            # Show selection menu
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
        
        # Group FAQs by category
        categorized = {}
        for faq in self._faq_cache[:50]:  # Limit to 50
            category = faq.category or "Uncategorized"
            if category not in categorized:
                categorized[category] = []
            categorized[category].append(faq)
        
        # Add fields per category
        for category, faqs in sorted(categorized.items()):
            lines = []
            for faq in faqs[:10]:  # Max 10 per category to avoid embed limits
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
            # Show current settings
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
        
        # Update log level
        if enabled:
            log.setLevel(logging.DEBUG)
            await ctx.send("✅ Debug mode **enabled**.", ephemeral=True)
        else:
            log.setLevel(logging.INFO)
            await ctx.send("✅ Debug mode **disabled**.", ephemeral=True)
    
    # ==================== AUTOCOMPLETE ====================
    
    @faq_search.autocomplete('query')
    @faq_post.autocomplete('query')
    async def search_autocomplete(self, interaction: discord.Interaction, current: str) -> List[app_commands.Choice[str]]:
        """Autocomplete for search queries."""
        if not current or len(current) < 2:
            return []
        
        try:
            # Get cached Helpshift titles
            helpshift_titles = self.helpshift_scraper.get_cached_titles()
            
            # Perform autocomplete search
            results = self.fuzzy_search.autocomplete_search(
                current,
                self._faq_cache,
                helpshift_titles,
                max_results=20
            )
            
            # Convert to choices
            choices = [
                app_commands.Choice(name=title[:100], value=title[:100])
                for title, score in results
            ]
            
            return choices[:25]  # Discord limit
        
        except Exception as e:
            log.error(f"Autocomplete error: {e}")
            return []
    
    # ==================== HELPER METHODS ====================
    
    async def _create_result_embed(self, result: SearchResult, query: str, public: bool = False) -> discord.Embed:
        """Create an embed for a search result."""
        if result.source == Source.CUSTOM:
            color = discord.Color.green()
            source_text = "FARA Custom"
        else:
            color = discord.Color.blue()
            source_text = "Mission Chief Help Center"
        
        embed = discord.Embed(
            title=result.title,
            description=result.get_excerpt(500),
            color=color
        )
        
        if result.category:
            embed.add_field(name="📂 Category", value=result.category, inline=True)
        
        if result.last_updated:
            embed.add_field(name="🕒 Last Updated", value=result.last_updated, inline=True)
        
        # Add FAQ ID for custom items (very important for editing)
        if result.source == Source.CUSTOM and result.faq_id:
            embed.add_field(name="🔢 FAQ ID", value=f"`{result.faq_id}`", inline=True)
        
        # Build footer text
        footer_parts = [f"Source: {source_text}"]
        if result.source == Source.CUSTOM and result.faq_id:
            footer_parts.append(f"ID: {result.faq_id}")
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
        
        # Add view link button if URL exists
        if main_result.url:
            self.add_item(discord.ui.Button(
                label="View on Help Center",
                url=main_result.url,
                style=discord.ButtonStyle.link
            ))
        
        # Add suggestions button if there are suggestions
        if suggestions:
            self.show_suggestions_button = discord.ui.Button(
                label=f"Show {len(suggestions)} Suggestions",
                style=discord.ButtonStyle.secondary
            )
            self.show_suggestions_button.callback = self.show_suggestions_callback
            self.add_item(self.show_suggestions_button)
        
        # Add outdated button
        self.outdated_button = discord.ui.Button(
            label="Report Outdated",
            style=discord.ButtonStyle.danger,
            emoji="⚠️"
        )
        self.outdated_button.callback = self.report_outdated_callback
        self.add_item(self.outdated_button)
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id
    
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
        
        # Perform search (simplified - reuse search command logic)
        query = self.search_query.value
        
        # Search both sources
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


class AddFAQModal(discord.ui.Modal, title="Add New FAQ"):
    """Modal for adding a new FAQ."""
    
    question = discord.ui.TextInput(
        label="Question",
        placeholder="What is ARR?",
        style=discord.TextStyle.short,
        required=True,
        max_length=300
    )
    
    answer = discord.ui.TextInput(
        label="Answer (Markdown supported)",
        placeholder="Alarm and Response Regulation allows...",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=2000
    )
    
    category = discord.ui.TextInput(
        label="Category",
        placeholder="Game Mechanics",
        style=discord.TextStyle.short,
        required=False,
        max_length=100
    )
    
    synonyms = discord.ui.TextInput(
        label="Synonyms (comma separated)",
        placeholder="arr, alarm rules, response regulation",
        style=discord.TextStyle.short,
        required=False,
        max_length=500
    )
    
    def __init__(self, cog: FAQManager, author_id: int):
        super().__init__()
        self.cog = cog
        self.author_id = author_id
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # Parse synonyms
        synonym_list = [s.strip() for s in self.synonyms.value.split(',')] if self.synonyms.value else []
        
        # Create FAQ
        faq = FAQItem(
            question=self.question.value,
            answer_md=self.answer.value,
            category=self.category.value or None,
            synonyms=synonym_list,
            author_id=self.author_id
        )
        
        # Save to database
        try:
            faq_id = await self.cog.database.add_faq(faq)
            await self.cog._reload_faq_cache()
            
            embed = discord.Embed(
                title="✅ FAQ Added",
                description=f"**Question:** {self.question.value}\n**ID:** {faq_id}",
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
        
        except Exception as e:
            log.error(f"Error adding FAQ: {e}", exc_info=True)
            await interaction.followup.send("❌ Failed to add FAQ. Please try again.", ephemeral=True)


class EditFAQModal(discord.ui.Modal, title="Edit FAQ"):
    """Modal for editing an existing FAQ."""
    
    question = discord.ui.TextInput(
        label="Question",
        style=discord.TextStyle.short,
        required=True,
        max_length=300
    )
    
    answer = discord.ui.TextInput(
        label="Answer",
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
        
        # Pre-fill with existing data
        self.question.default = faq.question
        self.answer.default = faq.answer_md
        self.category.default = faq.category or ""
        self.synonyms.default = ", ".join(faq.synonyms) if faq.synonyms else ""
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        # Update FAQ
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
        
        # Add select menu
        options = []
        for i, result in enumerate(results[:25], 1):  # Discord limit: 25 options
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
