from __future__ import annotations

import hashlib
import random
import re
from pathlib import Path
from typing import Any

import yaml
from PIL import Image, ImageDraw


ROOT = Path(__file__).resolve().parents[1]
FSC_ROOT = ROOT / "FireStationCommand"
MISSIONS_CONFIG = FSC_ROOT / "data" / "config" / "missions.yaml"
SIZE = 1024


def slug_seed(value: str) -> int:
    return int(hashlib.sha256(value.encode("utf-8")).hexdigest()[:16], 16)


def normalize_words(*values: Any) -> str:
    return " ".join(str(value or "").casefold() for value in values)


def category_for(mission: dict[str, Any]) -> str:
    title_text = normalize_words(
        mission.get("id"),
        mission.get("name"),
    )
    type_text = normalize_words(mission.get("mission_type"))
    full_text = normalize_words(
        mission.get("id"),
        mission.get("name"),
        mission.get("description"),
        mission.get("mission_type"),
        mission.get("dispatch_narrative"),
    )
    checks = [
        ("aircraft", ("aircraft", "airplane", "airport", "runway", "helicopter", "aviation")),
        ("water", ("water", "lake", "river", "flood", "boat", "drowning", "shore", "canal")),
        ("wildfire", ("wildfire", "brush", "forest", "grass", "wildland", "tree fire")),
        ("hazmat", ("hazmat", "chemical", "gas", "fuel", "oil", "acid", "toxic", "spill", "contamin")),
        ("collapse", ("collapse", "collapsed", "rubble", "earthquake", "sinkhole", "structural")),
        ("traffic", ("traffic", "road", "vehicle", "car", "bus", "truck", "motorbike", "collision", "crash", "accident")),
        ("medical", ("medical", "ems", "patient", "injur", "pain", "cardiac", "asthma", "stroke", "bleeding")),
        ("police", ("police", "robbery", "suspect", "shoot", "theft", "assault", "burglary", "weapon", "riot")),
        ("animal", ("animal", "cat", "dog", "horse", "hunting", "fishing")),
        ("rescue", ("rescue", "stuck", "trapped", "scaffolding", "height", "ladder", "confined")),
        ("fire", ("fire", "burning", "smoke", "flame", "explosion")),
    ]
    for category, tokens in checks:
        if any(token in title_text for token in tokens):
            return category
    for category, tokens in checks:
        if any(token in type_text for token in tokens):
            return category
    for category, tokens in checks:
        if any(token in full_text for token in tokens):
            return category
    return "station"


def severity_for(mission: dict[str, Any]) -> int:
    level = int(mission.get("recommended_level", mission.get("tier", mission.get("min_tier", 1))) or 1)
    staff = int(mission.get("required_staff", 1) or 1)
    vehicles = len(mission.get("required_vehicles") or [])
    score = level + staff // 4 + vehicles
    if score >= 18:
        return 4
    if score >= 11:
        return 3
    if score >= 6:
        return 2
    return 1


def line(draw: ImageDraw.ImageDraw, points: list[tuple[int, int]], fill: str = "#1b2228", width: int = 6) -> None:
    draw.line(points, fill=fill, width=width, joint="curve")


def rect(draw: ImageDraw.ImageDraw, xy: tuple[int, int, int, int], fill: str, outline: str = "#1b2228", width: int = 6) -> None:
    draw.rectangle(xy, fill=fill, outline=outline, width=width)


def rounded(draw: ImageDraw.ImageDraw, xy: tuple[int, int, int, int], radius: int, fill: str, outline: str = "#1b2228", width: int = 6) -> None:
    draw.rounded_rectangle(xy, radius=radius, fill=fill, outline=outline, width=width)


def ellipse(draw: ImageDraw.ImageDraw, xy: tuple[int, int, int, int], fill: str, outline: str = "#1b2228", width: int = 5) -> None:
    draw.ellipse(xy, fill=fill, outline=outline, width=width)


def polygon(draw: ImageDraw.ImageDraw, points: list[tuple[int, int]], fill: str, outline: str = "#1b2228", width: int = 5) -> None:
    draw.polygon(points, fill=fill)
    draw.line(points + [points[0]], fill=outline, width=width, joint="curve")


def draw_sky(draw: ImageDraw.ImageDraw, rng: random.Random) -> None:
    for y in range(SIZE):
        t = y / SIZE
        r = int(118 + 22 * (1 - t))
        g = int(197 + 23 * (1 - t))
        b = int(224 + 18 * (1 - t))
        draw.line([(0, y), (SIZE, y)], fill=(r, g, b))
    for _ in range(4800):
        x = rng.randrange(SIZE)
        y = rng.randrange(SIZE)
        shade = rng.randrange(0, 18)
        draw.point((x, y), fill=(105 + shade, 185 + shade, 214 + shade))


def draw_cloud(draw: ImageDraw.ImageDraw, x: int, y: int, scale: float = 1.0) -> None:
    fill = "#f3ead7"
    outline = "#1b2228"
    parts = [
        (0, 25, 90, 55),
        (40, 0, 130, 60),
        (95, 20, 185, 65),
        (-25, 35, 205, 72),
    ]
    for box in parts:
        xy = tuple(int(v * scale) for v in box)
        ellipse(draw, (x + xy[0], y + xy[1], x + xy[2], y + xy[3]), fill, outline, 3)


def draw_background(draw: ImageDraw.ImageDraw, rng: random.Random, category: str) -> None:
    draw_sky(draw, rng)
    draw_cloud(draw, 90 + rng.randrange(40), 85 + rng.randrange(40), 1.0)
    draw_cloud(draw, 700 + rng.randrange(50), 100 + rng.randrange(45), 0.85)
    if category == "water":
        rect(draw, (0, 680, SIZE, SIZE), "#2e88ad", width=0)
        for y in range(710, 965, 55):
            line(draw, [(0, y), (SIZE, y + rng.randrange(-20, 20))], "#d9eef2", 3)
        rect(draw, (0, 620, SIZE, 705), "#5d8f4d", width=0)
    elif category in {"wildfire", "animal"}:
        rect(draw, (0, 620, SIZE, SIZE), "#5f914c", width=0)
        rect(draw, (0, 835, SIZE, SIZE), "#40464e", width=0)
        for x in range(-40, SIZE, 90):
            line(draw, [(x, 1024), (x + 70, 835)], "#d7d3c9", 3)
    else:
        rect(draw, (0, 635, SIZE, SIZE), "#5b8c4c", width=0)
        rect(draw, (0, 790, SIZE, SIZE), "#3f454c", width=0)
        for x in range(-60, SIZE, 100):
            line(draw, [(x, 1024), (x + 70, 790)], "#d7d3c9", 3)


def draw_bricks(draw: ImageDraw.ImageDraw, x1: int, y1: int, x2: int, y2: int, rng: random.Random) -> None:
    rect(draw, (x1, y1, x2, y2), "#b9352c", width=7)
    for y in range(y1 + 26, y2, 42):
        line(draw, [(x1 + 4, y), (x2 - 4, y)], "#842b28", 3)
    row = 0
    for y in range(y1 + 8, y2 - 12, 42):
        offset = 0 if row % 2 == 0 else 34
        for x in range(x1 + offset, x2, 68):
            line(draw, [(x, y), (x, min(y + 28, y2 - 5))], "#842b28", 2)
        row += 1
    for _ in range(65):
        x = rng.randrange(x1 + 8, max(x1 + 9, x2 - 8))
        y = rng.randrange(y1 + 8, max(y1 + 9, y2 - 8))
        draw.rectangle((x, y, x + rng.randrange(7, 18), y + 4), fill="#cf493d")


def draw_windows(draw: ImageDraw.ImageDraw, boxes: list[tuple[int, int, int, int]]) -> None:
    for box in boxes:
        rect(draw, box, "#27343d", width=5)
        x1, y1, x2, y2 = box
        polygon(draw, [(x1 + 8, y1 + 8), (x2 - 12, y1 + 8), (x1 + 8, y2 - 10)], "#405764", "#405764", 1)
        line(draw, [(x1, (y1 + y2) // 2), (x2, (y1 + y2) // 2)], "#1b2228", 3)


def draw_building(draw: ImageDraw.ImageDraw, rng: random.Random, damaged: bool = False, industrial: bool = False) -> None:
    x1 = 220 + rng.randrange(-50, 45)
    y1 = 370 + rng.randrange(-25, 25)
    x2 = 800 + rng.randrange(-35, 55)
    y2 = 795
    draw_bricks(draw, x1, y1, x2, y2, rng)
    rect(draw, (x1 - 18, y1 - 26, x2 + 18, y1 + 8), "#2e3338", width=6)
    if industrial:
        for cx in (x1 + 90, x2 - 105):
            rect(draw, (cx, y1 - 150, cx + 55, y1 + 12), "#4f5961", width=6)
            line(draw, [(cx + 10, y1 - 145), (cx + 45, y1 - 145)], "#8e969a", 4)
    draw_windows(draw, [(x1 + 50, y1 + 72, x1 + 155, y1 + 155), (x2 - 170, y1 + 72, x2 - 65, y1 + 155)])
    rect(draw, ((x1 + x2) // 2 - 70, y2 - 155, (x1 + x2) // 2 + 70, y2), "#424950", width=6)
    if damaged:
        crack = [(x1 + 265, y1 + 30), (x1 + 240, y1 + 90), (x1 + 280, y1 + 145), (x1 + 260, y1 + 210)]
        line(draw, crack, "#111519", 7)
        polygon(draw, [(x2 - 150, y1 + 10), (x2 - 70, y1 + 40), (x2 - 130, y1 + 95)], "#6b2d2b", "#1b2228", 4)


def draw_fire(draw: ImageDraw.ImageDraw, cx: int, cy: int, scale: float, rng: random.Random) -> None:
    for i, color in enumerate(["#e94225", "#ff8b24", "#ffc044"]):
        s = scale * (1 - i * 0.25)
        points = [
            (int(cx - 70 * s), int(cy + 85 * s)),
            (int(cx - 25 * s), int(cy - 80 * s)),
            (int(cx + 5 * s), int(cy - 5 * s)),
            (int(cx + 45 * s), int(cy - 120 * s)),
            (int(cx + 80 * s), int(cy + 85 * s)),
        ]
        polygon(draw, points, color, "#1b2228" if i == 0 else color, 4 if i == 0 else 1)
    for i in range(5):
        sx = cx + int((i - 2) * 35 * scale) + rng.randrange(-10, 10)
        sy = cy - int(155 * scale) - i * 16
        ellipse(draw, (sx - 38, sy - 20, sx + 42, sy + 38), "#52575b", "#1b2228", 3)


def draw_fire_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int, category: str = "fire") -> None:
    draw_building(draw, rng, damaged=severity >= 3, industrial=category == "hazmat")
    draw_fire(draw, 515 + rng.randrange(-80, 80), 395 + rng.randrange(-30, 30), 0.75 + severity * 0.12, rng)
    if severity >= 2:
        draw_fire_engine(draw, 120, 720, rng)


def draw_fire_engine(draw: ImageDraw.ImageDraw, x: int, y: int, rng: random.Random) -> None:
    rect(draw, (x, y, x + 255, y + 95), "#c7372f", width=6)
    rect(draw, (x + 162, y - 42, x + 245, y + 95), "#c7372f", width=6)
    draw_windows(draw, [(x + 175, y - 25, x + 232, y + 25)])
    rect(draw, (x + 25, y + 18, x + 145, y + 58), "#333b42", width=4)
    for wx in (x + 55, x + 202):
        ellipse(draw, (wx - 28, y + 72, wx + 28, y + 128), "#20252b", width=5)
        ellipse(draw, (wx - 12, y + 88, wx + 12, y + 112), "#8c949b", width=3)
    line(draw, [(x + 15, y - 18), (x + 140, y - 58), (x + 235, y - 58)], "#d9d3bf", 8)
    if rng.random() > 0.45:
        ellipse(draw, (x + 212, y - 62, x + 238, y - 38), "#e52e2e", width=4)


def draw_ambulance(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    rect(draw, (x, y, x + 245, y + 98), "#f3eee0", width=6)
    rect(draw, (x + 165, y - 38, x + 235, y + 98), "#f3eee0", width=6)
    draw_windows(draw, [(x + 178, y - 22, x + 222, y + 25)])
    line(draw, [(x + 35, y + 48), (x + 125, y + 48)], "#d4473f", 12)
    line(draw, [(x + 80, y + 5), (x + 80, y + 88)], "#d4473f", 12)
    for wx in (x + 52, x + 196):
        ellipse(draw, (wx - 27, y + 73, wx + 27, y + 127), "#20252b", width=5)


def draw_police_car(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    polygon(draw, [(x + 35, y + 60), (x + 90, y + 15), (x + 175, y + 15), (x + 230, y + 60)], "#f2f0e6", "#1b2228", 6)
    rect(draw, (x, y + 55, x + 265, y + 120), "#222933", width=6)
    rect(draw, (x + 22, y + 65, x + 115, y + 103), "#f2f0e6", width=3)
    rect(draw, (x + 130, y + 65, x + 240, y + 103), "#f2f0e6", width=3)
    ellipse(draw, (x + 38, y + 95, x + 92, y + 149), "#20252b", width=5)
    ellipse(draw, (x + 178, y + 95, x + 232, y + 149), "#20252b", width=5)
    rect(draw, (x + 116, y + 2, x + 150, y + 18), "#e43a36", width=3)


def draw_car(draw: ImageDraw.ImageDraw, x: int, y: int, color: str, damaged: bool = False) -> None:
    polygon(draw, [(x + 35, y + 70), (x + 90, y + 25), (x + 185, y + 25), (x + 245, y + 70)], color, "#1b2228", 6)
    rect(draw, (x, y + 65, x + 280, y + 132), color, width=6)
    draw_windows(draw, [(x + 88, y + 37, x + 136, y + 67), (x + 146, y + 37, x + 196, y + 67)])
    if damaged:
        line(draw, [(x + 215, y + 55), (x + 250, y + 78), (x + 220, y + 102)], "#111519", 6)
    for wx in (x + 55, x + 218):
        ellipse(draw, (wx - 28, y + 108, wx + 28, y + 164), "#20252b", width=5)


def draw_traffic_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_car(draw, 230, 610, "#2f79b8", damaged=True)
    draw_car(draw, 510, 645, "#d9bd46", damaged=severity >= 2)
    if severity >= 2:
        draw_fire(draw, 510, 610, 0.45, rng)
    draw_fire_engine(draw, 60, 755, rng)
    if severity >= 2:
        draw_ambulance(draw, 710, 745)
    for x in (420, 465, 510, 555):
        polygon(draw, [(x, 830), (x + 20, 760), (x + 42, 830)], "#f28b27", "#1b2228", 4)


def draw_medical_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_building(draw, rng, damaged=False)
    draw_ambulance(draw, 110, 735)
    ellipse(draw, (645, 675, 695, 725), "#f0c7a6", width=5)
    rect(draw, (620, 724, 735, 770), "#2d79b8", width=5)
    line(draw, [(605, 785), (750, 785)], "#d9d3bf", 10)
    if severity >= 3:
        draw_fire_engine(draw, 700, 755, rng)


def draw_police_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_building(draw, rng, damaged=False)
    draw_police_car(draw, 120, 735)
    if severity >= 2:
        draw_police_car(draw, 650, 745)
    for x in range(280, 740, 70):
        line(draw, [(x, 610), (x + 55, 650)], "#e5c33b", 8)


def draw_hazmat_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_fire_scene(draw, rng, severity, "hazmat")
    for x in (640, 700, 760):
        rect(draw, (x, 705, x + 48, 790), "#e2b542", width=5)
        line(draw, [(x, 730), (x + 48, 730)], "#1b2228", 4)
    ellipse(draw, (620, 792, 850, 855), "#7dbd57", "#2d5b35", 5)


def draw_water_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    polygon(draw, [(275, 690), (600, 690), (535, 790), (325, 790)], "#e34a34", "#1b2228", 7)
    rect(draw, (370, 615, 485, 692), "#f1ece0", width=6)
    draw_windows(draw, [(390, 635, 455, 680)])
    line(draw, [(275, 690), (210, 650)], "#1b2228", 7)
    ellipse(draw, (705, 700, 780, 770), "#f28b27", width=5)
    line(draw, [(705, 735), (780, 735)], "#f8e2b4", 7)
    if severity >= 2:
        draw_fire_engine(draw, 50, 760, rng)


def draw_tree(draw: ImageDraw.ImageDraw, x: int, y: int, scale: float = 1.0) -> None:
    rect(draw, (int(x - 18 * scale), int(y), int(x + 18 * scale), int(y + 210 * scale)), "#7b4e2d", width=4)
    for dx, dy, s in [(-70, -55, 1.0), (0, -90, 1.15), (70, -55, 1.0), (-25, -10, 1.0), (35, -5, 1.0)]:
        ellipse(
            draw,
            (
                int(x + dx * scale - 72 * s * scale),
                int(y + dy * scale - 55 * s * scale),
                int(x + dx * scale + 72 * s * scale),
                int(y + dy * scale + 55 * s * scale),
            ),
            "#3f7d3e",
            width=5,
        )


def draw_wildfire_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    for x in range(120, 930, 155):
        draw_tree(draw, x + rng.randrange(-25, 25), 460 + rng.randrange(-25, 25), 0.85)
    for x in range(300, 760, 110):
        draw_fire(draw, x + rng.randrange(-25, 25), 690 + rng.randrange(-15, 15), 0.38 + severity * 0.06, rng)
    draw_fire_engine(draw, 80, 780, rng)


def draw_aircraft_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    rect(draw, (0, 670, SIZE, SIZE), "#4a4f55", width=0)
    for x in range(-80, SIZE, 170):
        line(draw, [(x, 1000), (x + 90, 670)], "#d7d3c9", 6)
    polygon(draw, [(290, 560), (770, 615), (850, 665), (360, 640)], "#e9e5d7", "#1b2228", 7)
    polygon(draw, [(470, 590), (560, 455), (610, 610)], "#d84a3e", "#1b2228", 6)
    polygon(draw, [(610, 610), (735, 510), (690, 635)], "#d84a3e", "#1b2228", 6)
    draw_windows(draw, [(365, 575, 420, 607), (435, 582, 490, 614), (505, 590, 560, 622)])
    if severity >= 2:
        draw_fire(draw, 750, 630, 0.55, rng)
    draw_fire_engine(draw, 70, 775, rng)


def draw_collapse_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_building(draw, rng, damaged=True)
    for x in range(370, 700, 55):
        polygon(draw, [(x, 790), (x + 42, 735), (x + 88, 790)], "#73777a", "#1b2228", 4)
    draw_fire_engine(draw, 80, 765, rng)
    if severity >= 3:
        draw_ambulance(draw, 710, 760)


def draw_rescue_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_building(draw, rng, damaged=False)
    line(draw, [(240, 780), (580, 430)], "#d9d3bf", 12)
    for i in range(9):
        x = 260 + i * 35
        line(draw, [(x, 760 - i * 35), (x + 40, 720 - i * 35)], "#d9d3bf", 5)
    draw_fire_engine(draw, 100, 765, rng)
    if severity >= 2:
        draw_ambulance(draw, 690, 765)


def draw_animal_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree(draw, 515, 450, 1.25)
    ellipse(draw, (552, 430, 612, 492), "#d58d3b", width=5)
    polygon(draw, [(560, 435), (575, 400), (590, 437)], "#d58d3b", "#1b2228", 4)
    polygon(draw, [(585, 437), (605, 400), (610, 445)], "#d58d3b", "#1b2228", 4)
    draw_fire_engine(draw, 85, 765, rng)
    if severity >= 2:
        draw_police_car(draw, 680, 760)


def draw_station_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_building(draw, rng, damaged=False)
    draw_fire_engine(draw, 110, 760, rng)


SCENE_DRAWERS = {
    "fire": draw_fire_scene,
    "traffic": draw_traffic_scene,
    "medical": draw_medical_scene,
    "police": draw_police_scene,
    "hazmat": draw_hazmat_scene,
    "water": draw_water_scene,
    "wildfire": draw_wildfire_scene,
    "aircraft": draw_aircraft_scene,
    "collapse": draw_collapse_scene,
    "rescue": draw_rescue_scene,
    "animal": draw_animal_scene,
    "station": draw_station_scene,
}


def draw_footer_badge(draw: ImageDraw.ImageDraw, category: str, severity: int) -> None:
    rect(draw, (36, 36, 190, 92), "#2e3338", width=5)
    colors = {
        "fire": "#e94225",
        "traffic": "#f3bf3c",
        "medical": "#f3eee0",
        "police": "#2f5fb8",
        "hazmat": "#82bd54",
        "water": "#2e88ad",
        "wildfire": "#f06b25",
        "aircraft": "#d9d3bf",
        "collapse": "#73777a",
        "rescue": "#e94225",
        "animal": "#3f7d3e",
        "station": "#e94225",
    }
    for i in range(severity):
        ellipse(draw, (55 + i * 30, 52, 76 + i * 30, 73), colors.get(category, "#e94225"), width=3)


def render_mission(mission: dict[str, Any]) -> Image.Image:
    seed_text = str(mission.get("id") or mission.get("name") or mission.get("image"))
    rng = random.Random(slug_seed(seed_text))
    category = category_for(mission)
    severity = severity_for(mission)
    img = Image.new("RGB", (SIZE, SIZE), "#80cce8")
    draw = ImageDraw.Draw(img)
    draw_background(draw, rng, category)
    SCENE_DRAWERS.get(category, draw_station_scene)(draw, rng, severity)
    draw_footer_badge(draw, category, severity)
    return img


def main() -> None:
    data = yaml.safe_load(MISSIONS_CONFIG.read_text(encoding="utf-8"))
    missions = data.get("missions", [])
    if not isinstance(missions, list):
        raise SystemExit("missions.yaml does not contain a missions list")

    written = 0
    for mission in missions:
        if not isinstance(mission, dict):
            continue
        image_path = mission.get("image")
        if not isinstance(image_path, str) or not image_path:
            continue
        if not re.fullmatch(r"Images/Missions/[A-Za-z0-9_.() -]+\.png", image_path):
            raise SystemExit(f"Unexpected mission image path: {image_path}")
        output = FSC_ROOT / image_path
        output.parent.mkdir(parents=True, exist_ok=True)
        render_mission(mission).save(output, "PNG", compress_level=6)
        written += 1

    print(f"Wrote {written} mission images.")


if __name__ == "__main__":
    main()
