"""
IconGen - Generate custom vehicle icons for MissionChief
"""

import discord
from redbot.core import commands, Config
from redbot.core.utils.chat_formatting import box, pagify
from typing import Literal, Optional
import re
import io
import zipfile

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
    
    @discord.ui.button(label="Normal", style=discord.ButtonStyle.secondary, emoji="📋", row=0)
    async def generate_normal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, False, "glow", self.case_style
        )
        await interaction.followup.send("✅ Generated normal icon!", ephemeral=True)
    
    @discord.ui.button(label="Static Glow", style=discord.ButtonStyle.secondary, emoji="🔆", row=0)
    async def generate_static_glow(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, "glow", self.case_style
        )
        await interaction.followup.send("✅ Generated static glow icon!", ephemeral=True)
    
    @discord.ui.button(label="Static Border", style=discord.ButtonStyle.secondary, emoji="⭕", row=0)
    async def generate_static_border(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, "border", self.case_style
        )
        await interaction.followup.send("✅ Generated static border icon!", ephemeral=True)
    
    @discord.ui.button(label="Static Both", style=discord.ButtonStyle.secondary, emoji="💫", row=0)
    async def generate_static_both(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, "both", self.case_style
        )
        await interaction.followup.send("✅ Generated static glow + border icon!", ephemeral=True)
    
    @discord.ui.select(
        placeholder="🚨 Choose Emergency Animation...",
        min_values=1,
        max_values=1,
        row=1,
        options=[
            discord.SelectOption(
                label="Classic Flash (L-R)",
                description="Alternating left-right police lights",
                value="classic_flash",
                emoji="🚔"
            ),
            discord.SelectOption(
                label="Quad Flash",
                description="Four-burst pattern (2L, 2R)",
                value="quad_flash",
                emoji="⚡"
            ),
            discord.SelectOption(
                label="Strobe Pulse",
                description="Rapid police strobe",
                value="strobe_pulse",
                emoji="⚠️"
            ),
            discord.SelectOption(
                label="Slow Fade",
                description="Smooth paramedic fade",
                value="slow_fade",
                emoji="🚑"
            ),
            discord.SelectOption(
                label="Dual Color",
                description="Red/blue split flash",
                value="dual_color",
                emoji="🔴"
            ),
            discord.SelectOption(
                label="Rotating Beacon",
                description="Classic rotating light",
                value="rotating_beacon",
                emoji="🔄"
            ),
            discord.SelectOption(
                label="Wig-Wag",
                description="Headlight alternating",
                value="wig_wag",
                emoji="💡"
            ),
            discord.SelectOption(
                label="Triple Flash",
                description="3 flashes + pause",
                value="triple_flash",
                emoji="⚡"
            ),
            discord.SelectOption(
                label="Arrow Flash",
                description="Directional traffic advisor",
                value="arrow_flash",
                emoji="➡️"
            ),
            discord.SelectOption(
                label="Halo Pulse",
                description="Expanding halo effect",
                value="halo_pulse",
                emoji="⭕"
            ),
        ]
    )
    async def select_animation(self, interaction: discord.Interaction, select: discord.ui.Select):
        await interaction.response.defer()
        animation_style = select.values[0]
        
        await self.cog._generate_and_send(
            self.ctx, self.text, self.color, True, animation_style, self.case_style
        )
        
        animation_names = {
            "classic_flash": "Classic Flash (L-R)",
            "quad_flash": "Quad Flash",
            "strobe_pulse": "Strobe Pulse",
            "slow_fade": "Slow Fade",
            "dual_color": "Dual Color",
            "rotating_beacon": "Rotating Beacon",
            "wig_wag": "Wig-Wag",
            "triple_flash": "Triple Flash",
            "arrow_flash": "Arrow Flash",
            "halo_pulse": "Halo Pulse"
        }
        
        await interaction.followup.send(
            f"✅ Generated **{animation_names[animation_style]}** animation (APNG in ZIP)!",
            ephemeral=True
        )
    
    @discord.ui.button(label="Generate All", style=discord.ButtonStyle.success, emoji="📦", row=2)
    async def generate_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        
        # Generate all variants (static + animated)
        generator = IconGenerator()
        
        static_variants = [
            ("normal", False, "glow"),
            ("static_glow", True, "glow"),
            ("static_border", True, "border"),
            ("static_both", True, "both"),
        ]
        
        animated_variants = [
            ("classic_flash", True, "classic_flash"),
            ("quad_flash", True, "quad_flash"),
            ("strobe_pulse", True, "strobe_pulse"),
            ("slow_fade", True, "slow_fade"),
            ("dual_color", True, "dual_color"),
            ("rotating_beacon", True, "rotating_beacon"),
            ("wig_wag", True, "wig_wag"),
            ("triple_flash", True, "triple_flash"),
            ("arrow_flash", True, "arrow_flash"),
            ("halo_pulse", True, "halo_pulse"),
        ]
        
        # Generate static files
        static_files = []
        for variant_name, emergency, style in static_variants:
            buffer = generator.generate_icon(
                text=self.text,
                color=self.color,
                emergency=emergency,
                emergency_style=style,
                case_style=self.case_style
            )
            filename = f"{self.text}_{variant_name}.png"
            static_files.append(discord.File(buffer, filename=filename))
        
        # Generate animated files and ZIP them
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for variant_name, emergency, style in animated_variants:
                buffer = generator.generate_icon(
                    text=self.text,
                    color=self.color,
                    emergency=emergency,
                    emergency_style=style,
                    case_style=self.case_style
                )
                filename = f"{self.text}_{variant_name}.png"
                zip_file.writestr(filename, buffer.read())
        
        zip_buffer.seek(0)
        animated_zip = discord.File(zip_buffer, filename=f"{self.text}_animated.zip")
        
        # Send static files
        await self.ctx.send(
            f"**Static icons for `{self.text}`:**",
            files=static_files
        )
        
        # Send animated files as ZIP
        await self.ctx.send(
            f"**Animated icons for `{self.text}` (10 realistic emergency patterns):**\n"
            f"⚠️ Download the ZIP and extract! Discord compresses PNGs.",
            file=animated_zip
        )
        
        await interaction.followup.send(
            "✅ Generated all 14 variants!\n"
            "- 4 static PNGs\n"
            "- 10 animated APNGs (in ZIP)",
            ephemeral=True
        )
    
    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌", row=2)
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
        
        # Check if it's an animated style
        animated_styles = [
            "classic_flash", "quad_flash", "strobe_pulse", "slow_fade",
            "dual_color", "rotating_beacon", "wig_wag", "triple_flash",
            "arrow_flash", "halo_pulse"
        ]
        is_animated = emergency and emergency_style in animated_styles
        
        # Create filename
        emergency_suffix = "emergency" if emergency else "normal"
        style_suffix = f"_{emergency_style}" if emergency else ""
        base_filename = f"{text}_{emergency_suffix}{style_suffix}"
        
        if is_animated:
            # ZIP animated files so Discord doesn't compress/convert them
            import zipfile
            import tempfile
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                zip_file.writestr(f"{base_filename}.png", buffer.read())
            
            zip_buffer.seek(0)
            file = discord.File(zip_buffer, filename=f"{base_filename}.zip")
            
            await ctx.send(
                f"⚠️ **APNG Animatie** - Download de ZIP en pak uit! "
                f"(Discord comprimeert PNGs en vernietigt de animatie)",
                file=file
            )
        else:
            # Static images can be sent directly
            filename = f"{base_filename}.png"
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
        emergency_style: Optional[Literal[
            "glow", "border", "both",
            "classic_flash", "quad_flash", "strobe_pulse", "slow_fade",
            "dual_color", "rotating_beacon", "wig_wag", "triple_flash",
            "arrow_flash", "halo_pulse"
        ]] = None,
        case: Optional[Literal["upper", "lower", "normal"]] = None
    ):
        """
        Generate a single icon (static PNG or animated APNG)
        
        **Arguments:**
        - `text`: Text to display on the icon
        - `color`: Color preset (fire/police/ems/etc.) or hex code (#DC2626)
        - `emergency`: Whether to generate emergency variant (default: False)
        - `emergency_style`: Effect style (default: glow)
          **Static:** glow, border, both
          **Animated (Realistic Emergency Patterns):**
          - classic_flash: Alternating left-right police lights
          - quad_flash: Four-burst pattern (2 left, 2 right)
          - strobe_pulse: Rapid police strobe
          - slow_fade: Smooth paramedic fade
          - dual_color: Red/blue split flash
          - rotating_beacon: Classic rotating light
          - wig_wag: Headlight alternating
          - triple_flash: 3 flashes + pause
          - arrow_flash: Directional traffic advisor
          - halo_pulse: Expanding halo effect
        - `case`: Text case - upper/lower/normal (default: upper)
        
        **Examples:**
        - `[p]icon gen "ENGINE" fire` - Normal icon
        - `[p]icon gen "TRUCK" fire true glow` - Static emergency
        - `[p]icon gen "PATROL" police true classic_flash` - Animated (APNG)
        - `[p]icon gen "MEDIC" ems true slow_fade` - Animated fade (APNG)
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
        
        # Determine output type
        animated_styles = ["flash", "pulse", "strobe", "rotate", "combo"]
        is_animated = emergency and emergency_style in animated_styles
        
        variant = "emergency (animated APNG)" if is_animated else ("emergency" if emergency else "normal")
        color_name = preset_name if preset_name else hex_color
        await ctx.send(f"✅ Generated **{variant}** icon for `{text}` with color `{color_name}` and style `{emergency_style}`")
    
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
        
        # Separate static and animated icons
        static_files = []
        animated_files = {}
        animated_styles = [
            "classic_flash", "quad_flash", "strobe_pulse", "slow_fade",
            "dual_color", "rotating_beacon", "wig_wag", "triple_flash",
            "arrow_flash", "halo_pulse"
        ]
        
        for name, buffer in results.items():
            # Check if filename contains animated style
            is_animated = any(style in name.lower() for style in animated_styles)
            
            if is_animated:
                animated_files[name] = buffer
            else:
                static_files.append(discord.File(buffer, filename=name))
        
        # Send static icons in batches (Discord limit: 10 files per message)
        if static_files:
            batch_size = 10
            for i in range(0, len(static_files), batch_size):
                batch = static_files[i:i + batch_size]
                await ctx.send(f"**Static icons (batch {i//batch_size + 1}):**", files=batch)
        
        # Send animated icons as ZIP
        if animated_files:
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for name, buffer in animated_files.items():
                    zip_file.writestr(name, buffer.read())
            
            zip_buffer.seek(0)
            zip_file_obj = discord.File(zip_buffer, filename="animated_icons.zip")
            
            await ctx.send(
                f"**Animated icons ({len(animated_files)} APNGs):**\n"
                f"⚠️ Download the ZIP and extract! Discord comprimeert PNGs en vernietigt animaties.",
                file=zip_file_obj
            )
        
        total_count = len(static_files) + len(animated_files)
        await ctx.send(
            f"✅ Generated **{total_count}** icons successfully!\n"
            f"- {len(static_files)} static PNGs\n"
            f"- {len(animated_files)} animated APNGs (in ZIP)"
        )
        
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
        - `emergency_style`: Default emergency style
          Static: glow/border/both
          Animated: flash/pulse/strobe/rotate/combo
        
        **Examples:**
        - `[p]icon config case upper`
        - `[p]icon config emergency_style flash`
        - `[p]icon config emergency_style pulse`
        """
        if setting == "case":
            if value not in ["upper", "lower", "normal"]:
                await ctx.send("❌ Invalid value! Use: `upper`, `lower`, or `normal`")
                return
            
            await self.config.guild(ctx.guild).default_case.set(value)
            await ctx.send(f"✅ Default text case set to: `{value}`")
        
        elif setting == "emergency_style":
            valid_styles = [
                "glow", "border", "both",
                "classic_flash", "quad_flash", "strobe_pulse", "slow_fade",
                "dual_color", "rotating_beacon", "wig_wag", "triple_flash",
                "arrow_flash", "halo_pulse"
            ]
            if value not in valid_styles:
                await ctx.send(f"❌ Invalid value! Use one of: {', '.join(f'`{s}`' for s in valid_styles[:5])}... (see `{ctx.prefix}icon help` for all)")
                return
            
            await self.config.guild(ctx.guild).default_emergency_style.set(value)
            
            animated = value in [
                "classic_flash", "quad_flash", "strobe_pulse", "slow_fade",
                "dual_color", "rotating_beacon", "wig_wag", "triple_flash",
                "arrow_flash", "halo_pulse"
            ]
            style_type = "animated (APNG)" if animated else "static"
            await ctx.send(f"✅ Default emergency style set to: `{value}` ({style_type})")
    
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
                "**Static (PNG):**\n"
                "`glow`, `border`, `both`\n\n"
                "**Animated (APNG) - Realistic Emergency Patterns:**\n"
                "🚔 `classic_flash` - L-R alternating police lights\n"
                "⚡ `quad_flash` - Four-burst pattern (2L, 2R)\n"
                "⚠️ `strobe_pulse` - Rapid police strobe\n"
                "🚑 `slow_fade` - Smooth paramedic fade\n"
                "🔴 `dual_color` - Red/blue split flash\n"
                "🔄 `rotating_beacon` - Classic rotating light\n"
                "💡 `wig_wag` - Headlight alternating\n"
                "⚡ `triple_flash` - 3 flashes + pause\n"
                "➡️ `arrow_flash` - Directional traffic advisor\n"
                "⭕ `halo_pulse` - Expanding halo effect"
            ),
            inline=False
        )
        
        embed.add_field(
            name="🎬 Animation Info",
            value=(
                "MissionChief supports APNG (animated PNG)!\n"
                "Emergency icons can now have **realistic animations**.\n"
                "Use preview dropdown to see all options!"
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
