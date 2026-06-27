from __future__ import annotations

import argparse

from PIL import Image

from fsc_image_style import FSC_IMAGES, ROOT, prepare_fsc_image


def normalize_image(path, dry_run: bool = False) -> bool:
    with Image.open(path) as original:
        image = original.convert("RGB")
        prepared = prepare_fsc_image(image)
        if image.size == prepared.size and image.tobytes() == prepared.tobytes():
            return False

    if not dry_run:
        prepared.save(path, "PNG", compress_level=6)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Normalize FireStationCommand PNG assets to the shared game-asset canvas.")
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
