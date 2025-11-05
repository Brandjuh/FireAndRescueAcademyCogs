import discord
from redbot.core import commands
import os


class RoleExporter(commands.Cog):
    """Export all server roles to a text file"""

    def __init__(self, bot):
        self.bot = bot

    @commands.command(name="exportroles")
    @commands.is_owner()
    @commands.guild_only()
    async def export_roles(self, ctx):
        """Export all server roles with their IDs to a text file"""
        
        # Get all roles sorted by position (like Discord displays them)
        roles = sorted(ctx.guild.roles, key=lambda r: r.position, reverse=True)
        
        # Create the text content
        lines = []
        for role in roles:
            lines.append(f"{role.name} - {role.id}")
        
        content = "\n".join(lines)
        
        # Create temporary file
        filename = "roles.txt"
        filepath = f"/tmp/{filename}"
        
        try:
            # Write to file
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(content)
            
            # Send as attachment
            file = discord.File(filepath, filename=filename)
            await ctx.send(
                f"📋 Exported {len(roles)} roles from **{ctx.guild.name}**",
                file=file
            )
            
        except Exception as e:
            await ctx.send(f"❌ Error creating export: {e}")
        
        finally:
            # Clean up temporary file
            if os.path.exists(filepath):
                os.remove(filepath)
