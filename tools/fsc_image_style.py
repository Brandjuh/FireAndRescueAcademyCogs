from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageOps


ROOT = Path(__file__).resolve().parents[1]
FSC_IMAGES = ROOT / "FireStationCommand" / "Images"
CANVAS_SIZE = (1024, 1024)
PIXEL_SOURCE_SIZE = (256, 256)
PALETTE_COLORS = 128


def background_for(image: Image.Image) -> tuple[int, int, int]:
    pixel = image.convert("RGB").getpixel((0, 0))
    return tuple(int(value) for value in pixel)


def normalize_canvas(image: Image.Image) -> Image.Image:
    rgb_image = image.convert("RGB")
    if rgb_image.size == CANVAS_SIZE:
        return rgb_image

    resized = ImageOps.contain(rgb_image, CANVAS_SIZE, Image.Resampling.LANCZOS)
    canvas = Image.new("RGB", CANVAS_SIZE, background_for(rgb_image))
    x = (CANVAS_SIZE[0] - resized.width) // 2
    y = (CANVAS_SIZE[1] - resized.height) // 2
    canvas.paste(resized, (x, y))
    return canvas


def apply_pixel_art_style(image: Image.Image) -> Image.Image:
    canvas = normalize_canvas(image)
    source = canvas.resize(PIXEL_SOURCE_SIZE, Image.Resampling.BOX)
    pixel_palette = source.quantize(colors=PALETTE_COLORS, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE)
    return pixel_palette.convert("RGB").resize(CANVAS_SIZE, Image.Resampling.NEAREST)


def prepare_fsc_image(image: Image.Image) -> Image.Image:
    return apply_pixel_art_style(image)
