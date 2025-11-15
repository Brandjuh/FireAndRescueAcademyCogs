"""
Image generation module for IconGen
"""

from PIL import Image, ImageDraw, ImageFont, ImageFilter
import io
from typing import Tuple, Literal, List
from .presets import hex_to_rgb
try:
    from apng import APNG, PNG
    APNG_AVAILABLE = True
except ImportError:
    APNG_AVAILABLE = False

class IconGenerator:
    """Generate vehicle icons with modern pill-shaped design"""
    
    # Standard output dimensions
    WIDTH = 60
    HEIGHT = 35
    
    # Preview dimensions (larger for visibility)
    PREVIEW_WIDTH = 200
    PREVIEW_HEIGHT = 120
    
    # Upscaling factor for anti-aliasing
    SCALE_FACTOR = 4
    
    def __init__(self):
        self.font_cache = {}
    
    def _get_font(self, size: int, bold: bool = True):
        """Get font with caching"""
        cache_key = (size, bold)
        if cache_key not in self.font_cache:
            try:
                # Try to load system fonts
                if bold:
                    font_paths = [
                        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
                        "/System/Library/Fonts/Helvetica.ttc",
                        "C:\\Windows\\Fonts\\arialbd.ttf",
                        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
                    ]
                else:
                    font_paths = [
                        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                        "/System/Library/Fonts/Helvetica.ttc",
                        "C:\\Windows\\Fonts\\arial.ttf",
                        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"
                    ]
                
                font = None
                for path in font_paths:
                    try:
                        font = ImageFont.truetype(path, size)
                        break
                    except:
                        continue
                
                if font is None:
                    # Fallback to default font
                    font = ImageFont.load_default()
                
                self.font_cache[cache_key] = font
            except Exception:
                self.font_cache[cache_key] = ImageFont.load_default()
        
        return self.font_cache[cache_key]
    
    def _apply_text_case(self, text: str, case_style: str) -> str:
        """Apply text case transformation"""
        if case_style == "upper":
            return text.upper()
        elif case_style == "lower":
            return text.lower()
        else:  # normal
            return text
    
    def generate_icon(
        self,
        text: str,
        color: str,
        emergency: bool = False,
        emergency_style: Literal["glow", "border", "both", "flash", "pulse", "strobe", "rotate", "combo"] = "glow",
        case_style: str = "upper",
        preview: bool = False
    ) -> io.BytesIO:
        """
        Generate a single icon (static PNG or animated APNG)
        
        Args:
            text: Text to display on icon
            color: Hex color code (e.g., '#DC2626')
            emergency: Whether to apply emergency styling
            emergency_style: Type of emergency effect:
                - Static: 'glow', 'border', 'both'
                - Animated: 'flash', 'pulse', 'strobe', 'rotate', 'combo'
            case_style: Text case ('upper', 'lower', or 'normal')
            preview: Generate larger preview version
        
        Returns:
            BytesIO buffer containing PNG or APNG image
        """
        # Animated styles
        animated_styles = ["flash", "pulse", "strobe", "rotate", "combo"]
        
        if emergency and emergency_style in animated_styles:
            if not APNG_AVAILABLE:
                # Fallback to static flash effect if APNG not available
                return self._generate_static_frame(text, color, True, "flash", case_style, preview)
            
            # Generate animated APNG
            return self._generate_animated_icon(text, color, emergency_style, case_style, preview)
        else:
            # Generate static PNG
            return self._generate_static_frame(text, color, emergency, emergency_style, case_style, preview)
    
    def _generate_static_frame(
        self,
        text: str,
        color: str,
        emergency: bool = False,
        emergency_style: Literal["glow", "border", "both", "flash"] = "glow",
        case_style: str = "upper",
        preview: bool = False
    ) -> io.BytesIO:
        """Internal method to generate a single static frame"""
        # Apply text case
        display_text = self._apply_text_case(text, case_style)
        
        # Choose dimensions
        if preview:
            width = self.PREVIEW_WIDTH
            height = self.PREVIEW_HEIGHT
        else:
            width = self.WIDTH
            height = self.HEIGHT
        
        # Create high-res image for anti-aliasing
        hr_width = width * self.SCALE_FACTOR
        hr_height = height * self.SCALE_FACTOR
        
        # Parse color
        bg_color = hex_to_rgb(color)
        
        # For flash effect, lighten the background color significantly
        if emergency and emergency_style == "flash":
            # Brighten the color by 60% and increase saturation
            bg_color = tuple(min(255, int(c * 1.6)) for c in bg_color)
        
        # Create image with transparency
        img = Image.new('RGBA', (hr_width, hr_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        
        # Calculate pill shape dimensions
        border_radius = hr_height // 2
        
        # Draw pill shape (rounded rectangle)
        pill_bbox = [0, 0, hr_width - 1, hr_height - 1]
        draw.rounded_rectangle(pill_bbox, radius=border_radius, fill=bg_color)
        
        # Add emergency effects if needed
        if emergency:
            if emergency_style in ["border", "both", "flash"]:
                # Draw MUCH thicker border
                border_width = 8 * self.SCALE_FACTOR if emergency_style == "flash" else 6 * self.SCALE_FACTOR
                
                # Use high-contrast emergency colors (red/blue alternating effect)
                emergency_border_color = (255, 50, 50, 255)  # Bright red
                
                for i in range(border_width):
                    opacity = 255 - (i * 15)
                    border_color = (*emergency_border_color[:3], max(100, opacity))
                    draw.rounded_rectangle(
                        [i, i, hr_width - 1 - i, hr_height - 1 - i],
                        radius=border_radius - i,
                        outline=border_color,
                        width=2
                    )
            
            if emergency_style in ["glow", "both", "flash"]:
                # Apply MUCH stronger glow effect
                glow_size = 120 if emergency_style == "flash" else 80
                glow_img = Image.new('RGBA', (hr_width + glow_size, hr_height + glow_size), (0, 0, 0, 0))
                glow_draw = ImageDraw.Draw(glow_img)
                
                # Multiple glow layers with different colors for flash effect
                if emergency_style == "flash":
                    # Alternating red and blue glow for emergency light effect
                    glow_colors = [
                        (255, 50, 50, 180),   # Bright red
                        (50, 100, 255, 160),  # Bright blue
                        (255, 100, 100, 140), # Light red
                        (100, 150, 255, 120), # Light blue
                    ]
                else:
                    # Single color glow (much stronger than before)
                    glow_colors = [
                        (*bg_color, 200),
                        (*bg_color, 160),
                        (*bg_color, 120),
                    ]
                
                # Draw multiple glow layers
                offset_center = glow_size // 2
                for idx, glow_color in enumerate(glow_colors):
                    offset = 10 + (idx * 15)
                    glow_draw.rounded_rectangle(
                        [offset_center - offset, offset_center - offset, 
                         hr_width + offset_center + offset, hr_height + offset_center + offset],
                        radius=border_radius + offset,
                        fill=glow_color
                    )
                
                # Apply strong blur
                blur_radius = 30 if emergency_style == "flash" else 20
                glow_img = glow_img.filter(ImageFilter.GaussianBlur(radius=blur_radius))
                
                # Composite glow with main image
                final_img = Image.new('RGBA', (hr_width, hr_height), (0, 0, 0, 0))
                final_img.paste(glow_img, (-glow_size // 2, -glow_size // 2), glow_img)
                final_img.paste(img, (0, 0), img)
                img = final_img
                draw = ImageDraw.Draw(img)
            
            # For flash variant, add text outline for extra visibility
            if emergency_style == "flash":
                # We'll add the outline when drawing text below
                pass
        
        # Calculate font size that fits
        font_size = hr_height // 2
        font = self._get_font(font_size, bold=True)
        
        # Get text bounding box
        bbox = draw.textbbox((0, 0), display_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        # Adjust font size if text is too wide
        while text_width > hr_width * 0.85 and font_size > 10:
            font_size -= 2
            font = self._get_font(font_size, bold=True)
            bbox = draw.textbbox((0, 0), display_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        
        # Center text
        text_x = (hr_width - text_width) // 2 - bbox[0]
        text_y = (hr_height - text_height) // 2 - bbox[1]
        
        # For flash variant, draw text outline for extra visibility
        if emergency and emergency_style == "flash":
            # Draw thick outline
            outline_width = 3
            for adj_x in range(-outline_width, outline_width + 1):
                for adj_y in range(-outline_width, outline_width + 1):
                    if adj_x != 0 or adj_y != 0:
                        draw.text((text_x + adj_x, text_y + adj_y), display_text, 
                                fill=(0, 0, 0, 255), font=font)
        
        # Draw main text (white)
        draw.text((text_x, text_y), display_text, fill=(255, 255, 255, 255), font=font)
        
        # Downscale for anti-aliasing
        img = img.resize((width, height), Image.Resampling.LANCZOS)
        
        # Save to BytesIO
        buffer = io.BytesIO()
        img.save(buffer, format='PNG')
        buffer.seek(0)
        
        return buffer
    
    def _generate_animated_icon(
        self,
        text: str,
        color: str,
        animation_type: Literal["flash", "pulse", "strobe", "rotate", "combo"],
        case_style: str = "upper",
        preview: bool = False
    ) -> io.BytesIO:
        """Generate an animated APNG icon"""
        # Generate frames based on animation type
        if animation_type == "flash":
            frames = self._generate_flash_frames(text, color, case_style, preview)
            delays = [500, 500]  # 500ms per frame
        elif animation_type == "pulse":
            frames = self._generate_pulse_frames(text, color, case_style, preview)
            delays = [150] * 6  # 150ms per frame (smooth)
        elif animation_type == "strobe":
            frames = self._generate_strobe_frames(text, color, case_style, preview)
            delays = [200, 200]  # 200ms per frame (fast)
        elif animation_type == "rotate":
            frames = self._generate_rotate_frames(text, color, case_style, preview)
            delays = [125] * 8  # 125ms per frame
        elif animation_type == "combo":
            frames = self._generate_combo_frames(text, color, case_style, preview)
            delays = [250] * 4  # 250ms per frame
        else:
            # Fallback
            frames = [self._generate_frame_with_params(text, color, case_style, preview)]
            delays = [1000]
        
        # Create APNG
        return self._create_apng(frames, delays)
    
    def _generate_frame_with_params(
        self,
        text: str,
        color: str,
        case_style: str,
        preview: bool,
        glow_intensity: float = 1.0,
        glow_color_override: tuple = None,
        border_thickness: int = 6,
        brightness_multiplier: float = 1.0
    ) -> Image.Image:
        """Generate a single frame with custom parameters for animations"""
        display_text = self._apply_text_case(text, case_style)
        
        if preview:
            width = self.PREVIEW_WIDTH
            height = self.PREVIEW_HEIGHT
        else:
            width = self.WIDTH
            height = self.HEIGHT
        
        hr_width = width * self.SCALE_FACTOR
        hr_height = height * self.SCALE_FACTOR
        
        # Parse and adjust color
        bg_color = hex_to_rgb(color)
        if brightness_multiplier != 1.0:
            bg_color = tuple(min(255, int(c * brightness_multiplier)) for c in bg_color)
        
        # Create image
        img = Image.new('RGBA', (hr_width, hr_height), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        border_radius = hr_height // 2
        
        # Draw pill shape
        pill_bbox = [0, 0, hr_width - 1, hr_height - 1]
        draw.rounded_rectangle(pill_bbox, radius=border_radius, fill=bg_color)
        
        # Add glow effect
        glow_size = int(80 * glow_intensity)
        glow_img = Image.new('RGBA', (hr_width + glow_size, hr_height + glow_size), (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(glow_img)
        
        # Use custom glow color if provided
        if glow_color_override:
            base_glow = glow_color_override
        else:
            base_glow = bg_color
        
        # Draw glow layers
        offset_center = glow_size // 2
        opacity_base = int(200 * glow_intensity)
        for idx in range(3):
            offset = 10 + (idx * 15)
            opacity = max(100, opacity_base - (idx * 40))
            glow_color = (*base_glow, opacity)
            glow_draw.rounded_rectangle(
                [offset_center - offset, offset_center - offset,
                 hr_width + offset_center + offset, hr_height + offset_center + offset],
                radius=border_radius + offset,
                fill=glow_color
            )
        
        # Blur the glow
        blur_radius = int(20 * glow_intensity)
        glow_img = glow_img.filter(ImageFilter.GaussianBlur(radius=blur_radius))
        
        # Composite
        final_img = Image.new('RGBA', (hr_width, hr_height), (0, 0, 0, 0))
        final_img.paste(glow_img, (-glow_size // 2, -glow_size // 2), glow_img)
        final_img.paste(img, (0, 0), img)
        
        # Add border
        draw = ImageDraw.Draw(final_img)
        border_width = border_thickness * self.SCALE_FACTOR
        emergency_border_color = (255, 50, 50)
        
        for i in range(border_width):
            opacity = 255 - (i * 15)
            border_color = (*emergency_border_color, max(100, opacity))
            draw.rounded_rectangle(
                [i, i, hr_width - 1 - i, hr_height - 1 - i],
                radius=border_radius - i,
                outline=border_color,
                width=2
            )
        
        # Add text
        font_size = hr_height // 2
        font = self._get_font(font_size, bold=True)
        bbox = draw.textbbox((0, 0), display_text, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        
        while text_width > hr_width * 0.85 and font_size > 10:
            font_size -= 2
            font = self._get_font(font_size, bold=True)
            bbox = draw.textbbox((0, 0), display_text, font=font)
            text_width = bbox[2] - bbox[0]
            text_height = bbox[3] - bbox[1]
        
        text_x = (hr_width - text_width) // 2 - bbox[0]
        text_y = (hr_height - text_height) // 2 - bbox[1]
        
        # Text outline
        outline_width = 3
        for adj_x in range(-outline_width, outline_width + 1):
            for adj_y in range(-outline_width, outline_width + 1):
                if adj_x != 0 or adj_y != 0:
                    draw.text((text_x + adj_x, text_y + adj_y), display_text,
                            fill=(0, 0, 0, 255), font=font)
        
        draw.text((text_x, text_y), display_text, fill=(255, 255, 255, 255), font=font)
        
        # Downscale
        final_img = final_img.resize((width, height), Image.Resampling.LANCZOS)
        return final_img
    
    def _generate_flash_frames(self, text: str, color: str, case_style: str, preview: bool) -> List[Image.Image]:
        """Generate alternating red-blue flash frames (2 frames)"""
        frames = []
        
        # Frame 1: Red glow
        frame1 = self._generate_frame_with_params(
            text, color, case_style, preview,
            glow_intensity=1.5,
            glow_color_override=(255, 50, 50),
            border_thickness=6,
            brightness_multiplier=1.2
        )
        frames.append(frame1)
        
        # Frame 2: Blue glow
        frame2 = self._generate_frame_with_params(
            text, color, case_style, preview,
            glow_intensity=1.5,
            glow_color_override=(50, 100, 255),
            border_thickness=6,
            brightness_multiplier=1.2
        )
        frames.append(frame2)
        
        return frames
    
    def _generate_pulse_frames(self, text: str, color: str, case_style: str, preview: bool) -> List[Image.Image]:
        """Generate pulsating glow frames (6 frames)"""
        frames = []
        intensities = [0.8, 1.0, 1.3, 1.6, 1.3, 1.0]  # Smooth pulse
        
        for intensity in intensities:
            frame = self._generate_frame_with_params(
                text, color, case_style, preview,
                glow_intensity=intensity,
                border_thickness=int(4 + intensity * 2),
                brightness_multiplier=1.0 + (intensity - 1.0) * 0.3
            )
            frames.append(frame)
        
        return frames
    
    def _generate_strobe_frames(self, text: str, color: str, case_style: str, preview: bool) -> List[Image.Image]:
        """Generate hard strobe frames (2 frames)"""
        frames = []
        
        # Frame 1: Normal emergency
        frame1 = self._generate_frame_with_params(
            text, color, case_style, preview,
            glow_intensity=1.2,
            border_thickness=6,
            brightness_multiplier=1.1
        )
        frames.append(frame1)
        
        # Frame 2: Super bright
        frame2 = self._generate_frame_with_params(
            text, color, case_style, preview,
            glow_intensity=2.0,
            border_thickness=8,
            brightness_multiplier=1.6
        )
        frames.append(frame2)
        
        return frames
    
    def _generate_rotate_frames(self, text: str, color: str, case_style: str, preview: bool) -> List[Image.Image]:
        """Generate rotating color frames (8 frames)"""
        frames = []
        colors = [
            (255, 50, 50),    # Red
            (255, 120, 50),   # Orange
            (255, 200, 50),   # Yellow
            (50, 255, 120),   # Green
            (50, 150, 255),   # Cyan
            (100, 50, 255),   # Blue
            (200, 50, 255),   # Purple
            (255, 50, 150),   # Pink
        ]
        
        for glow_color in colors:
            frame = self._generate_frame_with_params(
                text, color, case_style, preview,
                glow_intensity=1.4,
                glow_color_override=glow_color,
                border_thickness=6,
                brightness_multiplier=1.2
            )
            frames.append(frame)
        
        return frames
    
    def _generate_combo_frames(self, text: str, color: str, case_style: str, preview: bool) -> List[Image.Image]:
        """Generate combo strobe + pulse frames (4 frames)"""
        frames = []
        
        configs = [
            (1.2, (255, 50, 50), 6, 1.2),    # Red bright
            (1.8, (50, 100, 255), 8, 1.5),   # Blue super bright
            (1.2, (255, 50, 50), 6, 1.2),    # Red bright
            (1.8, (255, 150, 50), 8, 1.5),   # Orange super bright
        ]
        
        for intensity, glow_color, border, brightness in configs:
            frame = self._generate_frame_with_params(
                text, color, case_style, preview,
                glow_intensity=intensity,
                glow_color_override=glow_color,
                border_thickness=border,
                brightness_multiplier=brightness
            )
            frames.append(frame)
        
        return frames
    
    def _create_apng(self, frames: List[Image.Image], delays: List[int]) -> io.BytesIO:
        """Create an APNG from frames"""
        # Convert PIL Images to PNG objects
        png_frames = []
        for frame in frames:
            # Save frame to bytes
            buffer = io.BytesIO()
            frame.save(buffer, format='PNG')
            png_data = buffer.getvalue()
            
            # Create PNG object from bytes
            png = PNG.from_bytes(png_data)
            png_frames.append(png)
        
        # Create APNG with frames
        apng = APNG()
        for png, delay in zip(png_frames, delays):
            apng.append(png, delay=delay)
        
        # Save to BytesIO
        output = io.BytesIO()
        apng_bytes = apng.to_bytes()
        output.write(apng_bytes)
        output.seek(0)
        
        return output
    
    def generate_batch(
        self,
        icons: list,
        case_style: str = "upper"
    ) -> dict:
        """
        Generate multiple icons at once
        
        Args:
            icons: List of dicts with keys: text, color, emergency, emergency_style
            case_style: Default text case for all icons
        
        Returns:
            Dict mapping icon text to BytesIO buffers
        """
        results = {}
        
        for icon_data in icons:
            text = icon_data.get("text")
            color = icon_data.get("color")
            emergency = icon_data.get("emergency", False)
            emergency_style = icon_data.get("emergency_style", "glow")
            
            # Use icon-specific case or default
            icon_case = icon_data.get("case_style", case_style)
            
            buffer = self.generate_icon(
                text=text,
                color=color,
                emergency=emergency,
                emergency_style=emergency_style,
                case_style=icon_case
            )
            
            # Create unique filename
            emergency_suffix = f"_{'emergency' if emergency else 'normal'}"
            style_suffix = f"_{emergency_style}" if emergency else ""
            filename = f"{text}{emergency_suffix}{style_suffix}.png"
            
            results[filename] = buffer
        
        return results
