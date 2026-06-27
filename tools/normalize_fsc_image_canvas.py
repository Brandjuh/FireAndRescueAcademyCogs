from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageOps


ROOT = Path(__file__).resolve().parents[1]
FSC_IMAGES = ROOT / "FireStationCommand" / "Images"
TARGET_SIZE = (1024, 1024)


def background_for(image: Image.Image) -> tuple[int, int, int]:
    pixel = image.convert("RGB").getpixel((0, 0))
    return tuple(int(value) for value in pixel)


def normalize_image(path: Path, dry_run: bool = False) -> bool:
    with Image.open(path) as original:
        image = original.convert("RGB")
        if image.size == TARGET_SIZE:
            return False

        background = background_for(image)
        resized = ImageOps.contain(image, TARGET_SIZE, Image.Resampling.LANCZOS)
        canvas = Image.new("RGB", TARGET_SIZE, background)
        x = (TARGET_SIZE[0] - resized.width) // 2
        y = (TARGET_SIZE[1] - resized.height) // 2
        canvas.paste(resized, (x, y))

    if not dry_run:
        canvas.save(path, "PNG", compress_level=6)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize FireStationCommand PNG assets to a shared 1024px canvas.")
    parser.add_argument("--dry-run", action="store_true", help="List files that would be normalized without writing them.")
    args = parser.parse_args()

    changed = []
    for path in sorted(FSC_IMAGES.rglob("*.png")):
        if normalize_image(path, dry_run=args.dry_run):
            changed.append(path.relative_to(ROOT).as_posix())

    action = "Would normalize" if args.dry_run else "Normalized"
    print(f"{action} {len(changed)} FireStationCommand image(s).")
    for path in changed:
        print(path)


if __name__ == "__main__":
    main()
