"""
IconGen - Generate custom vehicle icons for MissionChief
"""

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import box, pagify
from typing import Literal, Optional
import re

from .generator import IconGenerator
from .presets import get_preset, get_all_presets, is_valid_preset, hex_to_rgb


class IconPreviewView(discord.ui.View):
    """Interactive preview with generation buttons"""
    
    def __init__(self, cog, ctx, text: str, color: str, case_style: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.ctx = ctx
        self.text = text
        self.color = color
        self.case_style = case_style
        self.message = None
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Only allow the command author to use buttons"""
        if interaction.user.id != self.ctx.author.id:
            await interaction.response.send_message(
                "Only the person who ran the command can use these buttons!",
                ephemeral=True
            )
            return False
        return True
    
    async def on_timeout(self):
        """Disable buttons on timeout"""
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except:
                pass
    
    @discord.ui.button(label="Normal", style=discord.ButtonStyle.secondary, emoji="📋")
    async def generate_normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, False, "glow", self.case_style
        )
        await interaction.followup.send("✅ Generated normal icon!", ephemeral=True)
    
    @discord.ui.button(label="Emergency (Glow)", style=discord.ButtonStyle.danger, emoji="🔴")
    async def generate_emergency_glow(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, "glow", self.case_style
        )
        await interaction.followup.send("✅ Generated emergency icon with glow!", ephemeral=True)
    
    @discord.ui.button(label="Emergency (Border)", style=discord.ButtonStyle.danger, emoji="🔵")
    async def generate_emergency_border(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, "border", self.case_style
        )
        await interaction.followup.send("✅ Generated emergency icon with border!", ephemeral=True)
    
    @discord.ui.button(label="Emergency (Both)", style=discord.ButtonStyle.danger, emoji="⚡")
    async def generate_emergency_both(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, "both", self.case_style
        )
        await interaction.followup.send("✅ Generated emergency icon with glow + border!", ephemeral=True)
    
    @discord.ui.button(label="Generate All", style=discord.ButtonStyle.success, emoji="📦")
    async def generate_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        
        # Generate all 4 variants
        files = []
        generator = IconGenerator()
        
        variants = [
            ("normal", False, "glow"),
            ("emergency_glow", True, "glow"),
            ("emergency_border", True, "border"),
            ("emergency_both", True, "both")
        ]
        
        for variant_name, emergency, style in variants:
            buffer = generator.generate_icon(
                text=self.text,
                color=self.color,
                emergency=emergency,
                emergency_style=style,
                case_style=self.case_style
            )
            filename = f"{self.text}_{variant_name}.png"
            files.append(discord.File(buffer, filename=filename))
        
        await self.ctx.send(
            f"**All variants for `{self.text}`:**",
            files=files
        )
        await interaction.followup.send("✅ Generated all 4 variants!", ephemeral=True)
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        for item in self.children:
            item.disabled = True
        await self.message.edit(content="Preview cancelled.", view=self)
        self.stop()


class IconGen(commands.Cog):
    """Generate custom vehicle icons for MissionChief"""
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        default_global = {
            "default_case": "upper",
            "default_emergency_style": "glow"
        }
        
        default_guild = {
            "default_case": "upper",
            "default_emergency_style": "glow"
        }
        
        self.config.register_global(**default_global)
        self.config.register_guild(**default_guild)
        
        self.generator = IconGenerator()
    
    def _parse_color(self, color_input: str) -> tuple:
        """
        Parse color input (preset name or hex code)
        Returns: (hex_color, preset_name or None)
        """
        # Check if it's a preset
        if is_valid_preset(color_input):
            preset = get_preset(color_input)
            return (preset["color"], color_input)
        
        # Check if it's a hex code
        hex_pattern = r'^#?([A-Fa-f0-9]{6})$'
        match = re.match(hex_pattern, color_input)
        if match:
            hex_color = f"#{match.group(1)}"
            return (hex_color, None)
        
        return (None, None)
    
    async def _generate_and_send(
        self,
        ctx: commands.Context,
        text: str,
        color: str,
        emergency: bool,
        emergency_style: str,
        case_style: str
    ):
        """Generate icon and send to channel"""
        buffer = self.generator.generate_icon(
            text=text,
            color=color,
            emergency=emergency,
            emergency_style=emergency_style,
            case_style=case_style
        )
        
        # Create filename
        emergency_suffix = "emergency" if emergency else "normal"
        style_suffix = f"_{emergency_style}" if emergency else ""
        filename = f"{text}_{emergency_suffix}{style_suffix}.png"
        
        file = discord.File(buffer, filename=filename)
        await ctx.send(file=file)
    
    @commands.group(name="icon", aliases=["icongen"])
    async def icon_group(self, ctx: commands.Context):
        """Icon generation commands"""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)
    
    @icon_group.command(name="generate", aliases=["gen", "create"])
    async def icon_generate(
        self,
        ctx: commands.Context,
        text: str,
        color: str,
        emergency: Optional[bool] = False,
        emergency_style: Optional[Literal["glow", "border", "both"]] = None,
        case: Optional[Literal["upper", "lower", "normal"]] = None
    ):
        """
        Generate a single icon
        
        **Arguments:**
        - `text`: Text to display on the icon
        - `color`: Color preset (fire/police/ems/etc.) or hex code (#DC2626)
        - `emergency`: Whether to generate emergency variant (default: False)
        - `emergency_style`: Emergency effect style - glow/border/both (default: glow)
        - `case`: Text case - upper/lower/normal (default: upper)
        
        **Examples:**
        - `[p]icon gen "ENGINE" fire`
        - `[p]icon gen "TRUCK" #DC2626 true glow`
        - `[p]icon gen "patrol" police false glow lower`
        """
        # Parse color
        hex_color, preset_name = self._parse_color(color)
        if hex_color is None:
            await ctx.send(
                f"❌ Invalid color! Use a preset name (see `{ctx.prefix}icon presets`) "
                f"or a hex code (e.g., #DC2626)"
            )
            return
        
        # Get defaults
        if case is None:
            case = await self.config.guild(ctx.guild).default_case() if ctx.guild else "upper"
        
        if emergency_style is None:
            emergency_style = await self.config.guild(ctx.guild).default_emergency_style() if ctx.guild else "glow"
        
        # Generate and send
        async with ctx.typing():
            await self._generate_and_send(ctx, text, hex_color, emergency, emergency_style, case)
        
        variant = "emergency" if emergency else "normal"
        color_name = preset_name if preset_name else hex_color
        await ctx.send(f"✅ Generated **{variant}** icon for `{text}` with color `{color_name}`")
    
    @icon_group.command(name="preview")
    async def icon_preview(
        self,
        ctx: commands.Context,
        text: str,
        color: str,
        case: Optional[Literal["upper", "lower", "normal"]] = None
    ):
        """
        Preview an icon with interactive buttons to generate variants
        
        **Arguments:**
        - `text`: Text to display on the icon
        - `color`: Color preset (fire/police/ems/etc.) or hex code (#DC2626)
        - `case`: Text case - upper/lower/normal (default: upper)
        
        **Example:**
        - `[p]icon preview "ENGINE" fire`
        """
        # Parse color
        hex_color, preset_name = self._parse_color(color)
        if hex_color is None:
            await ctx.send(
                f"❌ Invalid color! Use a preset name (see `{ctx.prefix}icon presets`) "
                f"or a hex code (e.g., #DC2626)"
            )
            return
        
        # Get default case
        if case is None:
            case = await self.config.guild(ctx.guild).default_case() if ctx.guild else "upper"
        
        # Generate preview image
        async with ctx.typing():
            preview_buffer = self.generator.generate_icon(
                text=text,
                color=hex_color,
                emergency=False,
                case_style=case,
                preview=True
            )
        
        # Create view
        view = IconPreviewView(self, ctx, text, hex_color, case)
        
        # Send preview
        file = discord.File(preview_buffer, filename=f"{text}_preview.png")
        color_name = preset_name if preset_name else hex_color
        
        embed = discord.Embed(
            title="🎨 Icon Preview",
            description=f"**Text:** `{text}`\n**Color:** `{color_name}`\n**Case:** `{case}`",
            color=discord.Color.from_rgb(*hex_to_rgb(hex_color))
        )
        embed.set_image(url=f"attachment://{text}_preview.png")
        embed.set_footer(text="Select a variant to generate below")
        
        message = await ctx.send(embed=embed, file=file, view=view)
        view.message = message
    
    @icon_group.command(name="batch")
    async def icon_batch(self, ctx: commands.Context):
        """
        Generate multiple icons at once using an interactive form
        
        Upload a text file with one icon per line in format:
        `TEXT|COLOR|emergency|style`
        
        **Example file content:**
        ```
        ENGINE|fire|false|glow
        TRUCK|fire|true|glow
        CHIEF|fire|true|both
        PATROL|police|false|glow
        MEDIC|ems|true|border
        ```
        
        Or use the simplified format (normal icons only):
        ```
        ENGINE|fire
        TRUCK|#DC2626
        PATROL|police
        ```
        """
        await ctx.send(
            "📋 **Batch Icon Generation**\n\n"
            "Please upload a text file (`.txt`) with icon definitions.\n"
            "Format: `TEXT|COLOR|emergency|style` (one per line)\n\n"
            "**Example:**\n"
            "```\n"
            "ENGINE|fire|false|glow\n"
            "TRUCK|fire|true|glow\n"
            "PATROL|police|false|glow\n"
            "```\n"
            "Simplified format also works: `TEXT|COLOR`\n\n"
            "Upload your file now (you have 60 seconds)..."
        )
        
        # Wait for file upload
        def check(m):
            return (
                m.author == ctx.author
                and m.channel == ctx.channel
                and len(m.attachments) > 0
                and m.attachments[0].filename.endswith('.txt')
            )
        
        try:
            message = await self.bot.wait_for('message', check=check, timeout=60)
        except:
            await ctx.send("❌ Timeout! No file uploaded.")
            return
        
        # Download and parse file
        attachment = message.attachments[0]
        content = await attachment.read()
        
        try:
            lines = content.decode('utf-8').strip().split('\n')
        except:
            await ctx.send("❌ Could not read file. Make sure it's a text file with UTF-8 encoding.")
            return
        
        # Parse icon definitions
        icons = []
        errors = []
        
        for i, line in enumerate(lines, 1):
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.split('|')
            if len(parts) < 2:
                errors.append(f"Line {i}: Not enough parameters")
                continue
            
            text = parts[0].strip()
            color_input = parts[1].strip()
            emergency = parts[2].strip().lower() == 'true' if len(parts) > 2 else False
            emergency_style = parts[3].strip() if len(parts) > 3 else 'glow'
            
            # Parse color
            hex_color, _ = self._parse_color(color_input)
            if hex_color is None:
                errors.append(f"Line {i}: Invalid color '{color_input}'")
                continue
            
            icons.append({
                "text": text,
                "color": hex_color,
                "emergency": emergency,
                "emergency_style": emergency_style
            })
        
        if not icons:
            await ctx.send("❌ No valid icon definitions found!")
            if errors:
                error_msg = "\n".join(errors[:10])
                await ctx.send(f"**Errors:**\n```\n{error_msg}\n```")
            return
        
        # Show summary
        summary = f"Found **{len(icons)}** icons to generate"
        if errors:
            summary += f" ({len(errors)} errors)"
        
        await ctx.send(f"{summary}\n\nGenerating icons...")
        
        # Generate all icons
        async with ctx.typing():
            case_style = await self.config.guild(ctx.guild).default_case() if ctx.guild else "upper"
            results = self.generator.generate_batch(icons, case_style=case_style)
        
        # Send icons in batches (Discord limit: 10 files per message)
        files = [discord.File(buffer, filename=name) for name, buffer in results.items()]
        
        batch_size = 10
        for i in range(0, len(files), batch_size):
            batch = files[i:i + batch_size]
            await ctx.send(files=batch)
        
        await ctx.send(f"✅ Generated **{len(results)}** icons successfully!")
        
        if errors:
            error_msg = "\n".join(errors[:10])
            for page in pagify(error_msg, delims=["\n"]):
                await ctx.send(f"**Errors:**\n```\n{page}\n```")
    
    @icon_group.command(name="presets")
    async def icon_presets(self, ctx: commands.Context):
        """List all available color presets"""
        presets = get_all_presets()
        
        embed = discord.Embed(
            title="🎨 Available Color Presets",
            description="Use these preset names or any hex code (#RRGGBB)",
            color=discord.Color.blue()
        )
        
        # Group presets by category
        categories = {
            "🚒 Fire & Rescue": ["fire", "rescue"],
            "🚔 Police": ["police", "sheriff", "swat"],
            "🚑 Medical": ["ems", "medical"],
            "🌊 Coastal": ["coastal"],
            "🌲 Forestry": ["forestry"],
            "🏛️ Federal": ["federal", "fbi"],
            "🚧 Other": ["tow"]
        }
        
        for category, preset_names in categories.items():
            lines = []
            for name in preset_names:
                if name in presets:
                    preset = presets[name]
                    lines.append(f"`{name}` - {preset['name']} ({preset['color']})")
            
            if lines:
                embed.add_field(
                    name=category,
                    value="\n".join(lines),
                    inline=False
                )
        
        embed.set_footer(text=f"Use {ctx.prefix}icon gen <text> <preset> to generate an icon")
        await ctx.send(embed=embed)
    
    @icon_group.command(name="config")
    @commands.guild_only()
    @commands.admin_or_permissions(manage_guild=True)
    async def icon_config(
        self,
        ctx: commands.Context,
        setting: Literal["case", "emergency_style"],
        value: str
    ):
        """
        Configure default settings for this server
        
        **Settings:**
        - `case`: Default text case (upper/lower/normal)
        - `emergency_style`: Default emergency style (glow/border/both)
        
        **Examples:**
        - `[p]icon config case upper`
        - `[p]icon config emergency_style both`
        """
        if setting == "case":
            if value not in ["upper", "lower", "normal"]:
                await ctx.send("❌ Invalid value! Use: `upper`, `lower`, or `normal`")
                return
            
            await self.config.guild(ctx.guild).default_case.set(value)
            await ctx.send(f"✅ Default text case set to: `{value}`")
        
        elif setting == "emergency_style":
            if value not in ["glow", "border", "both"]:
                await ctx.send("❌ Invalid value! Use: `glow`, `border`, or `both`")
                return
            
            await self.config.guild(ctx.guild).default_emergency_style.set(value)
            await ctx.send(f"✅ Default emergency style set to: `{value}`")
    
    @icon_group.command(name="settings")
    @commands.guild_only()
    async def icon_settings(self, ctx: commands.Context):
        """View current server settings"""
        case = await self.config.guild(ctx.guild).default_case()
        emergency_style = await self.config.guild(ctx.guild).default_emergency_style()
        
        embed = discord.Embed(
            title="⚙️ IconGen Settings",
            color=discord.Color.blue()
        )
        embed.add_field(name="Default Text Case", value=f"`{case}`", inline=True)
        embed.add_field(name="Default Emergency Style", value=f"`{emergency_style}`", inline=True)
        
        await ctx.send(embed=embed)
    
    @icon_group.command(name="help", aliases=["guide"])
    async def icon_help(self, ctx: commands.Context):
        """Show detailed help and examples"""
        embed = discord.Embed(
            title="📚 IconGen Help",
            description="Generate custom vehicle icons for MissionChief",
            color=discord.Color.green()
        )
        
        embed.add_field(
            name="🎨 Quick Start",
            value=(
                f"`{ctx.prefix}icon gen \"ENGINE\" fire` - Generate normal icon\n"
                f"`{ctx.prefix}icon preview \"ENGINE\" fire` - Interactive preview\n"
                f"`{ctx.prefix}icon batch` - Generate multiple icons"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🎯 Commands",
            value=(
                f"`{ctx.prefix}icon gen <text> <color> [emergency] [style] [case]`\n"
                f"`{ctx.prefix}icon preview <text> <color> [case]`\n"
                f"`{ctx.prefix}icon batch` - Upload batch file\n"
                f"`{ctx.prefix}icon presets` - List color presets\n"
                f"`{ctx.prefix}icon config <setting> <value>` - Configure defaults\n"
                f"`{ctx.prefix}icon settings` - View settings"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🌈 Colors",
            value=(
                "Use preset names: `fire`, `police`, `ems`, `rescue`, etc.\n"
                f"Or hex codes: `#DC2626`, `#2563EB`, `#F59E0B`\n"
                f"See all presets: `{ctx.prefix}icon presets`"
            ),
            inline=False
        )
        
        embed.add_field(
            name="⚡ Emergency Styles",
            value=(
                "`glow` - Subtle glow effect\n"
                "`border` - Colored border\n"
                "`both` - Glow + border"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📝 Examples",
            value=(
                f"`{ctx.prefix}icon gen \"ENGINE\" fire true glow upper`\n"
                f"`{ctx.prefix}icon gen \"patrol\" police false glow lower`\n"
                f"`{ctx.prefix}icon gen \"MEDIC\" #F59E0B true both`"
            ),
            inline=False
        )
        
        embed.set_footer(text="Icon dimensions: 60x35 pixels (PNG format)")
        
        await ctx.send(embed=embed)
