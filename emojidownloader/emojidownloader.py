import discord
from redbot.core import commands
from redbot.core.bot import Red
import aiohttp
import zipfile
from io import BytesIO
from typing import Optional


class EmojiDownloader(commands.Cog):
    """Download all emojis from a server to a ZIP file."""
    
    def __init__(self, bot: Red):
        self.bot = bot
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def cog_load(self):
        """Initialize aiohttp session when cog loads."""
        self.session = aiohttp.ClientSession()
    
    async def cog_unload(self):
        """Close aiohttp session when cog unloads."""
        if self.session:
            await self.session.close()
    
    @commands.hybrid_command(name="emojidownload")
    @commands.is_owner()
    @commands.guild_only()
    async def emoji_download(self, ctx: commands.Context):
        """Download all emojis from the current server as a ZIP file.
        
        This command downloads all custom emojis (both regular and animated)
        from the current server and packages them into a ZIP file.
        """
        # Defer response as this might take a while
        if ctx.interaction:
            await ctx.defer()
        else:
            async with ctx.typing():
                pass
        
        # Get all emojis from the current guild
        emojis = ctx.guild.emojis
        
        if not emojis:
            embed = discord.Embed(
                title="❌ No Emojis Found",
                description="This server doesn't have any custom emojis.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed)
            return
        
        # Send initial status
        status_embed = discord.Embed(
            title="📥 Downloading Emojis...",
            description=f"Downloading **{len(emojis)}** emojis from **{ctx.guild.name}**\nThis may take a moment...",
            color=discord.Color.blue()
        )
        status_msg = await ctx.send(embed=status_embed)
        
        # Create ZIP file in memory
        zip_buffer = BytesIO()
        
        try:
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                downloaded = 0
                failed = []
                
                for emoji in emojis:
                    try:
                        # Determine file extension based on whether emoji is animated
                        extension = "gif" if emoji.animated else "png"
                        filename = f"{emoji.name}.{extension}"
                        
                        # Download emoji
                        async with self.session.get(str(emoji.url)) as resp:
                            if resp.status == 200:
                                emoji_data = await resp.read()
                                # Add to ZIP
                                zip_file.writestr(filename, emoji_data)
                                downloaded += 1
                            else:
                                failed.append(f"{emoji.name} (HTTP {resp.status})")
                    
                    except Exception as e:
                        failed.append(f"{emoji.name} ({str(e)})")
                        continue
            
            # Prepare ZIP for upload
            zip_buffer.seek(0)
            
            # Create Discord file
            zip_filename = f"{ctx.guild.name.replace(' ', '_')}_emojis.zip"
            file = discord.File(zip_buffer, filename=zip_filename)
            
            # Create success embed
            success_embed = discord.Embed(
                title="✅ Emojis Downloaded Successfully!",
                description=f"Downloaded **{downloaded}/{len(emojis)}** emojis from **{ctx.guild.name}**",
                color=discord.Color.green()
            )
            
            if failed:
                failed_list = "\n".join(failed[:10])  # Show max 10 failed emojis
                if len(failed) > 10:
                    failed_list += f"\n... and {len(failed) - 10} more"
                success_embed.add_field(
                    name="⚠️ Failed Downloads",
                    value=f"```{failed_list}```",
                    inline=False
                )
            
            success_embed.set_footer(text=f"Server ID: {ctx.guild.id}")
            
            # Edit status message and send file
            await status_msg.edit(embed=success_embed)
            await ctx.send(file=file)
        
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Download Failed",
                description=f"An error occurred while creating the ZIP file:\n```{str(e)}```",
                color=discord.Color.red()
            )
            await status_msg.edit(embed=error_embed)
