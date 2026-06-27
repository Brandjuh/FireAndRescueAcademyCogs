# FireStationCommand Asset Guide

FireStationCommand uses a consistent PixelArt game-asset style for all in-game images. This guide defines the required format so new generated, imported, or hand-edited assets stay compatible with the dashboard, mission, shop, station, and result embeds.

## Scope

These rules apply to every PNG under `FireStationCommand/Images`, including:

- Mission images in `Images/Missions`
- Vehicle images in `Images/Vehicles`
- Equipment images in `Images/Equipment`
- Station images in `Images/Stations`
- Outcome images in `Images/Outcomes`
- Legacy or shared FireStationCommand images in the top-level `Images` folder

## Required Format

Every FireStationCommand image must be:

- PNG format
- `1024x1024` pixels
- RGB or RGBA mode
- Built from a `256x256` visual grid and scaled to `1024x1024` with nearest-neighbor scaling
- Limited to no more than `128` colors

The result should read as a clean PixelArt game asset: simplified shapes, crisp hard edges, clear silhouettes, limited colors, and no photorealistic rendering.

## Style Rules

Use:

- Square front-facing or three-quarter game-asset composition
- Clear emergency-service subject matter
- Strong outlines and blocky shapes
- Simple readable backgrounds
- Limited color palettes with consistent red, dark gray, light blue, white, yellow, and safety-orange accents

Avoid:

- Photorealistic images
- Soft painterly gradients
- Blurred details
- High-resolution texture noise
- Tiny unreadable text
- Arbitrary canvas sizes
- High-color imported images without normalization

## Tooling

Regenerate mission, vehicle, and equipment catalog images:

```powershell
python tools/generate_fsc_mission_images.py
```

Normalize any existing FireStationCommand PNG assets to the shared PixelArt canvas:

```powershell
python tools/normalize_fsc_image_canvas.py
```

Check whether all assets are already normalized without writing changes:

```powershell
python tools/normalize_fsc_image_canvas.py --dry-run
```

Both tools use `tools/fsc_image_style.py`, which applies the shared canvas, palette, and PixelArt processing.

## Validation

Run the FireStationCommand data tests before committing asset changes:

```powershell
python -m pytest tests/test_firestationcommand_data.py
```

The test suite checks that configured mission, vehicle, and equipment image references exist and that every FireStationCommand image follows the PixelArt asset contract.

## Contributor Workflow

1. Add or update the `image` path in the relevant YAML catalog, or place a PNG in the correct `FireStationCommand/Images` folder.
2. Run the generator for catalog art, or run the normalizer for manually supplied images.
3. Run `python tools/normalize_fsc_image_canvas.py --dry-run`.
4. Run `python -m pytest tests/test_firestationcommand_data.py`.
5. Update `FireStationCommand/CHANGELOG.md` when player-visible assets or asset rules change.
