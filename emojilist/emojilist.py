import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from typing import List, Optional


class EmojiListView(discord.ui.View):
    """Pagination view for emoji list."""
    
    def __init__(self, embeds: List[discord.Embed], author_id: int, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.embeds = embeds
        self.author_id = author_id
        self.current_page = 0
        self.message: Optional[discord.Message] = None
        self._update_buttons()
    
    def _update_buttons(self):
        """Update button states based on current page."""
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == len(self.embeds) - 1
        
        # Update page counter in button label
        self.page_counter.label = f"{self.current_page + 1}/{len(self.embeds)}"
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the command author to use the buttons."""
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "You cannot control this pagination menu.", 
                ephemeral=True
            )
            return False
        return True
    
    @discord.ui.button(label="◀", style=discord.ButtonStyle.primary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to previous page."""
        self.current_page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)
    
    @discord.ui.button(label="1/1", style=discord.ButtonStyle.secondary, disabled=True)
    async def page_counter(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Display current page number."""
        pass
    
    @discord.ui.button(label="▶", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go to next page."""
        self.current_page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.embeds[self.current_page], view=self)
    
    @discord.ui.button(label="⏹️", style=discord.ButtonStyle.danger)
    async def stop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Stop the pagination and disable all buttons."""
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()
    
    async def on_timeout(self):
        """Disable all buttons when view times out."""
        if self.message:
            for child in self.children:
                child.disabled = True
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class EmojiList(commands.Cog):
    """Display all emojis the bot can use with their code format."""
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
    
    @commands.hybrid_command(name="emojilist")
    @commands.is_owner()
    async def emoji_list(self, ctx: commands.Context, server_id: Optional[int] = None, export: bool = False):
        """Display all emojis the bot can access with their code format.
        
        Shows both regular and animated emojis in paginated embeds.
        
        Parameters
        ----------
        server_id: Optional[int]
            The ID of the server to get emojis from. If not provided, shows all emojis from all servers.
        export: bool
            If True, exports all emojis to a TXT file instead of displaying them.
        """
        # Defer response for slash commands
        if ctx.interaction:
            await ctx.defer()
        
        # Collect emojis based on server_id
        if server_id:
            guild = self.bot.get_guild(server_id)
            if not guild:
                embed = discord.Embed(
                    title="❌ Server Not Found",
                    description=f"Could not find a server with ID `{server_id}`.\nMake sure the bot is in that server.",
                    color=discord.Color.red()
                )
                await ctx.send(embed=embed)
                return
            all_emojis = list(guild.emojis)
            server_name = guild.name
        else:
            all_emojis = list(self.bot.emojis)
            server_name = "All Servers"
        
        if not all_emojis:
            embed = discord.Embed(
                title="❌ No Emojis Found",
                description=f"**{server_name}** doesn't have any custom emojis.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Sort emojis: non-animated first, then animated, alphabetically within each group
        all_emojis.sort(key=lambda e: (e.animated, e.name.lower()))
        
        # If export is True, generate TXT file
        if export:
            await self._export_to_txt(ctx, all_emojis, server_name)
            return
        
        # Create embeds with pagination (10 emojis per page)
        embeds = []
        emojis_per_page = 10
        
        for i in range(0, len(all_emojis), emojis_per_page):
            chunk = all_emojis[i:i + emojis_per_page]
            
            embed = discord.Embed(
                title=f"🎭 Emoji List - {server_name}",
                description=f"Total Emojis: **{len(all_emojis)}**",
                color=discord.Color.blurple()
            )
            
            for emoji in chunk:
                # Format code based on whether emoji is animated
                if emoji.animated:
                    code = f"`<a:{emoji.name}:{emoji.id}>`"
                    emoji_display = f"<a:{emoji.name}:{emoji.id}>"
                else:
                    code = f"`<:{emoji.name}:{emoji.id}>`"
                    emoji_display = f"<:{emoji.name}:{emoji.id}>"
                
                # Add field with emoji and code
                embed.add_field(
                    name=f"{emoji_display} {emoji.name}",
                    value=code,
                    inline=False
                )
            
            # Add footer with page info
            embed.set_footer(text=f"Page {len(embeds) + 1}/{(len(all_emojis) + emojis_per_page - 1) // emojis_per_page}")
            embeds.append(embed)
        
        # Send with pagination if multiple pages
        if len(embeds) == 1:
            await ctx.send(embed=embeds[0])
        else:
            view = EmojiListView(embeds, ctx.author.id)
            message = await ctx.send(embed=embeds[0], view=view)
            view.message = message
    
    @commands.hybrid_command(name="emojiservers")
    @commands.is_owner()
    async def emoji_servers(self, ctx: commands.Context):
        """List all servers the bot is in with their IDs and emoji counts.
        
        Use the server ID with the emojilist command to view emojis from a specific server.
        """
        # Defer response for slash commands
        if ctx.interaction:
            await ctx.defer()
        
        # Get all guilds with emojis
        guilds_with_emojis = []
        for guild in self.bot.guilds:
            emoji_count = len(guild.emojis)
            if emoji_count > 0:
                guilds_with_emojis.append((guild.name, guild.id, emoji_count))
        
        if not guilds_with_emojis:
            embed = discord.Embed(
                title="❌ No Servers with Emojis",
                description="The bot is not in any servers with custom emojis.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Sort by emoji count descending
        guilds_with_emojis.sort(key=lambda x: x[2], reverse=True)
        
        # Create embed
        embed = discord.Embed(
            title="🏢 Servers with Emojis",
            description=f"The bot has access to **{len(guilds_with_emojis)}** servers with custom emojis.\n\nUse `[p]emojilist <server_id>` to view emojis from a specific server.",
            color=discord.Color.blurple()
        )
        
        # Add fields for each server (max 25 fields per embed)
        for name, guild_id, emoji_count in guilds_with_emojis[:25]:
            embed.add_field(
                name=f"{name}",
                value=f"ID: `{guild_id}`\nEmojis: **{emoji_count}**",
                inline=True
            )
        
        if len(guilds_with_emojis) > 25:
            embed.set_footer(text=f"Showing first 25 of {len(guilds_with_emojis)} servers")
        
        await ctx.send(embed=embed)
        
        if not all_emojis:
            embed = discord.Embed(
                title="❌ No Emojis Found",
                description=f"**{server_name}** doesn't have any custom emojis.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Sort emojis: non-animated first, then animated, alphabetically within each group
        all_emojis.sort(key=lambda e: (e.animated, e.name.lower()))
        
        # If export is True, generate TXT file
        if export:
            await self._export_to_txt(ctx, all_emojis, server_name)
            return
        
        # Create embeds with pagination (10 emojis per page)
        embeds = []
        emojis_per_page = 10
        
        for i in range(0, len(all_emojis), emojis_per_page):
            chunk = all_emojis[i:i + emojis_per_page]
            
            embed = discord.Embed(
                title=f"🎭 Emoji List - {server_name}",
                description=f"Total Emojis: **{len(all_emojis)}**",
                color=discord.Color.blurple()
            )
            
            for emoji in chunk:
                # Format code based on whether emoji is animated
                if emoji.animated:
                    code = f"`<a:{emoji.name}:{emoji.id}>`"
                    emoji_display = f"<a:{emoji.name}:{emoji.id}>"
                else:
                    code = f"`<:{emoji.name}:{emoji.id}>`"
                    emoji_display = f"<:{emoji.name}:{emoji.id}>"
                
                # Add field with emoji and code
                embed.add_field(
                    name=f"{emoji_display} {emoji.name}",
                    value=code,
                    inline=False
                )
            
            # Add footer with page info
            embed.set_footer(text=f"Page {len(embeds) + 1}/{(len(all_emojis) + emojis_per_page - 1) // emojis_per_page}")
            embeds.append(embed)
        
        # Send with pagination if multiple pages
        if len(embeds) == 1:
            await ctx.send(embed=embeds[0])
        else:
            view = EmojiListView(embeds, ctx.author.id)
            message = await ctx.send(embed=embeds[0], view=view)
            view.message = message
    
    async def _export_to_txt(self, ctx: commands.Context, emojis: list, server_name: str):
        """Export emojis to a TXT file and send it to the user.
        
        Parameters
        ----------
        ctx: commands.Context
            The command context.
        emojis: list
            List of emoji objects to export.
        server_name: str
            Name of the server the emojis are from.
        """
        # Create the text content
        lines = []
        lines.append(f"Emoji List - {server_name}")
        lines.append(f"Total: {len(emojis)}")
        lines.append("=" * 50)
        lines.append("")
        
        for emoji in emojis:
            if emoji.animated:
                code = f"<a:{emoji.name}:{emoji.id}>"
            else:
                code = f"<:{emoji.name}:{emoji.id}>"
            
            lines.append(f"{emoji.name}: {code}")
        
        # Join all lines
        content = "\n".join(lines)
        
        # Create a file-like object
        from io import BytesIO
        file_data = BytesIO(content.encode('utf-8'))
        file_data.seek(0)
        
        # Create Discord file
        file = discord.File(file_data, filename=f"emoji_list_{server_name.replace(' ', '_')}.txt")
        
        # Send file with embed
        embed = discord.Embed(
            title="📥 Emoji List Export",
            description=f"Exported **{len(emojis)}** emojis from **{server_name}** to TXT file.",
            color=discord.Color.green()
        )
        
        await ctx.send(embed=embed, file=file)
    
    def cog_unload(self):
        """Cleanup when cog is unloaded."""
        pass
