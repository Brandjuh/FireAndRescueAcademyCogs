from __future__ import annotations

import hashlib
import random
import re
from pathlib import Path
from typing import Any

import yaml
from PIL import Image, ImageDraw

from fsc_image_style import prepare_fsc_image


ROOT = Path(__file__).resolve().parents[1]
FSC_ROOT = ROOT / "FireStationCommand"
MISSIONS_CONFIG = FSC_ROOT / "data" / "config" / "missions.yaml"
VEHICLES_CONFIG = FSC_ROOT / "data" / "config" / "vehicles.yaml"
EQUIPMENT_CONFIG = FSC_ROOT / "data" / "config" / "equipment.yaml"
SIZE = 1024
LEGACY_MISSION_IMAGE_IDS = (
    "rescue_call",
    "small_bin_fire",
    "vehicle_collision",
)


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


def draw_tree_line(draw: ImageDraw.ImageDraw, rng: random.Random, y: int = 585) -> None:
    for x in range(-45, SIZE + 80, 86):
        height = rng.randrange(70, 145)
        ellipse(draw, (x, y - height, x + 130, y + 28), "#4e7f59", "#4e7f59", 1)
        ellipse(draw, (x + 35, y - height - 28, x + 105, y + 30), "#3d7050", "#3d7050", 1)


def draw_sidewalk(draw: ImageDraw.ImageDraw) -> None:
    rect(draw, (0, 742, SIZE, 832), "#d5d0c4", width=0)
    for x in range(-70, SIZE, 165):
        line(draw, [(x, 832), (x + 98, 742)], "#a9a79e", 4)
    line(draw, [(0, 742), (SIZE, 742)], "#1b2228", 5)


def draw_cone(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    polygon(draw, [(x, y + 78), (x + 28, y), (x + 58, y + 78)], "#ef7e24", "#1b2228", 4)
    line(draw, [(x + 12, y + 48), (x + 46, y + 48)], "#f6ead2", 7)
    rect(draw, (x - 10, y + 75, x + 68, y + 92), "#ef7e24", width=4)


def draw_hydrant(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    rect(draw, (x + 22, y + 30, x + 70, y + 115), "#c7372f", width=5)
    ellipse(draw, (x + 16, y + 2, x + 76, y + 58), "#d9483e", width=5)
    rect(draw, (x + 3, y + 53, x + 90, y + 82), "#c7372f", width=5)
    ellipse(draw, (x - 5, y + 48, x + 22, y + 85), "#c7372f", width=4)
    ellipse(draw, (x + 68, y + 48, x + 95, y + 85), "#c7372f", width=4)
    rect(draw, (x + 8, y + 112, x + 84, y + 132), "#9f2e2b", width=4)


def draw_hose(draw: ImageDraw.ImageDraw, points: list[tuple[int, int]], width: int = 14) -> None:
    line(draw, points, "#4f1615", width + 6)
    line(draw, points, "#b9352c", width)
    line(draw, points, "#d9584f", max(3, width // 4))


def draw_firefighter(draw: ImageDraw.ImageDraw, x: int, y: int, holding_hose: bool = True) -> None:
    ellipse(draw, (x + 30, y, x + 75, y + 44), "#f1caa8", width=4)
    rect(draw, (x + 21, y + 34, x + 85, y + 122), "#22282f", width=5)
    rect(draw, (x + 18, y + 118, x + 44, y + 207), "#22282f", width=5)
    rect(draw, (x + 62, y + 118, x + 88, y + 207), "#22282f", width=5)
    line(draw, [(x + 20, y + 78), (x + 84, y + 78)], "#e7c43c", 8)
    line(draw, [(x + 20, y + 152), (x + 44, y + 152)], "#e7c43c", 7)
    line(draw, [(x + 62, y + 152), (x + 88, y + 152)], "#e7c43c", 7)
    polygon(draw, [(x + 20, y + 16), (x + 82, y + 16), (x + 94, y + 34), (x + 8, y + 34)], "#242a30", "#1b2228", 4)
    rect(draw, (x + 41, y + 4, x + 64, y + 23), "#e7c43c", width=3)
    if holding_hose:
        line(draw, [(x + 82, y + 90), (x + 132, y + 128)], "#1b2228", 9)
        line(draw, [(x + 82, y + 90), (x + 132, y + 128)], "#c7372f", 5)


def draw_house(draw: ImageDraw.ImageDraw, rng: random.Random, damaged: bool = False, burning: bool = False) -> None:
    x1 = 280 + rng.randrange(-30, 30)
    x2 = 792 + rng.randrange(-20, 35)
    y1 = 315 + rng.randrange(-12, 18)
    y2 = 745
    polygon(draw, [(x1 - 35, y1 + 145), ((x1 + x2) // 2, y1 - 25), (x2 + 35, y1 + 145)], "#2f353a", "#1b2228", 7)
    rect(draw, (x1, y1 + 120, x2, y2), "#c7b99b", width=7)
    for y in range(y1 + 155, y2 - 20, 42):
        line(draw, [(x1 + 6, y), (x2 - 6, y)], "#8f876f", 3)
    rect(draw, (x2 - 100, y1 - 12, x2 - 45, y1 + 132), "#b9352c", width=6)
    rect(draw, (x2 - 108, y1 - 42, x2 - 37, y1 - 10), "#2f353a", width=5)
    draw_windows(draw, [(x1 + 58, y1 + 205, x1 + 150, y1 + 295), (x2 - 168, y1 + 205, x2 - 75, y1 + 295)])
    draw_windows(draw, [(x1 + 90, y1 + 340, x1 + 180, y1 + 430), (x2 - 198, y1 + 340, x2 - 108, y1 + 430)])
    rect(draw, ((x1 + x2) // 2 - 54, y2 - 130, (x1 + x2) // 2 + 54, y2), "#343b42", width=6)
    rect(draw, (x1 - 15, y2 - 12, x2 + 15, y2 + 18), "#2f353a", width=5)
    if damaged:
        for bx in (x1 + 80, x2 - 155, (x1 + x2) // 2 - 30):
            line(draw, [(bx, y1 + 205), (bx + 45, y1 + 285), (bx + 10, y1 + 330)], "#111519", 7)
        for sx in (x1 + 155, x2 - 215):
            for i in range(4):
                ellipse(draw, (sx + i * 35, y1 - 35 - i * 18, sx + 85 + i * 35, y1 + 25 - i * 18), "#555b60", width=3)
    if burning:
        draw_fire(draw, (x1 + x2) // 2 + rng.randrange(-50, 50), y1 + 210, 0.65, rng)


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
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_house(draw, rng, damaged=severity >= 2, burning=True)
    draw_fire_engine(draw, 55, 742, rng)
    if severity >= 3:
        draw_command_van(draw, 708, 758)
    else:
        draw_hydrant(draw, 850, 670)
    draw_firefighter(draw, 475, 650)
    if severity >= 2:
        draw_firefighter(draw, 610, 688)
    draw_hose(draw, [(178, 872), (345, 832), (520, 765), (625, 735)])
    draw_hose(draw, [(892, 775), (760, 802), (630, 820), (560, 775)], 11)
    for x in (35, 790, 925):
        draw_cone(draw, x, 796)


def draw_fire_engine(draw: ImageDraw.ImageDraw, x: int, y: int, rng: random.Random) -> None:
    rect(draw, (x, y, x + 310, y + 110), "#c7372f", width=7)
    polygon(draw, [(x + 175, y), (x + 212, y - 52), (x + 292, y - 52), (x + 310, y)], "#d94336", "#1b2228", 7)
    draw_windows(draw, [(x + 215, y - 35, x + 282, y + 16)])
    rect(draw, (x + 25, y + 20, x + 160, y + 68), "#333b42", width=5)
    rect(draw, (x + 178, y + 30, x + 246, y + 94), "#b82f2b", width=4)
    for px in range(x + 184, x + 238, 18):
        ellipse(draw, (px, y + 42, px + 10, y + 52), "#d9d3bf", width=2)
    line(draw, [(x + 24, y + 84), (x + 292, y + 84)], "#f1ead7", 9)
    rect(draw, (x + 205, y - 75, x + 285, y - 56), "#2f353a", width=4)
    rect(draw, (x + 218, y - 77, x + 246, y - 54), "#e43a36", width=3)
    rect(draw, (x + 248, y - 77, x + 276, y - 54), "#f1ead7", width=3)
    for wx in (x + 65, x + 245):
        ellipse(draw, (wx - 34, y + 78, wx + 34, y + 146), "#20252b", width=6)
        ellipse(draw, (wx - 15, y + 97, wx + 15, y + 127), "#8c949b", width=4)
    line(draw, [(x + 22, y - 18), (x + 118, y - 52), (x + 205, y - 52)], "#d9d3bf", 8)


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


def draw_command_van(draw: ImageDraw.ImageDraw, x: int, y: int) -> None:
    rect(draw, (x, y + 20, x + 255, y + 126), "#c7372f", width=6)
    rect(draw, (x + 160, y - 20, x + 238, y + 126), "#c7372f", width=6)
    draw_windows(draw, [(x + 175, y, x + 226, y + 45), (x + 28, y + 40, x + 105, y + 82)])
    rect(draw, (x + 108, y + 42, x + 154, y + 95), "#293541", width=4)
    line(draw, [(x + 18, y + 103), (x + 232, y + 103)], "#1b2228", 4)
    rect(draw, (x + 170, y - 45, x + 230, y - 26), "#2f353a", width=4)
    rect(draw, (x + 177, y - 47, x + 200, y - 24), "#e43a36", width=3)
    rect(draw, (x + 202, y - 47, x + 225, y - 24), "#2f65bf", width=3)
    for wx in (x + 58, x + 208):
        ellipse(draw, (wx - 28, y + 103, wx + 28, y + 159), "#20252b", width=5)


def draw_car(draw: ImageDraw.ImageDraw, x: int, y: int, color: str, damaged: bool = False) -> None:
    polygon(draw, [(x + 35, y + 70), (x + 90, y + 25), (x + 185, y + 25), (x + 245, y + 70)], color, "#1b2228", 6)
    rect(draw, (x, y + 65, x + 280, y + 132), color, width=6)
    draw_windows(draw, [(x + 88, y + 37, x + 136, y + 67), (x + 146, y + 37, x + 196, y + 67)])
    if damaged:
        line(draw, [(x + 215, y + 55), (x + 250, y + 78), (x + 220, y + 102)], "#111519", 6)
    for wx in (x + 55, x + 218):
        ellipse(draw, (wx - 28, y + 108, wx + 28, y + 164), "#20252b", width=5)


def draw_traffic_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_car(draw, 250, 610, "#2f79b8", damaged=True)
    draw_car(draw, 505, 650, "#d9bd46", damaged=severity >= 2)
    if severity >= 2:
        draw_fire(draw, 525, 614, 0.42, rng)
    draw_fire_engine(draw, 55, 762, rng)
    if severity >= 2:
        draw_ambulance(draw, 700, 765)
    draw_firefighter(draw, 470, 728)
    for x in (420, 465, 510, 555):
        polygon(draw, [(x, 830), (x + 20, 760), (x + 42, 830)], "#f28b27", "#1b2228", 4)


def draw_medical_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_house(draw, rng, damaged=False)
    draw_ambulance(draw, 110, 755)
    ellipse(draw, (645, 675, 695, 725), "#f0c7a6", width=5)
    rect(draw, (620, 724, 735, 770), "#2d79b8", width=5)
    line(draw, [(605, 785), (750, 785)], "#d9d3bf", 10)
    if severity >= 3:
        draw_fire_engine(draw, 700, 755, rng)


def draw_police_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_house(draw, rng, damaged=False)
    draw_police_car(draw, 110, 745)
    if severity >= 2:
        draw_police_car(draw, 650, 760)
    for x in range(280, 740, 70):
        line(draw, [(x, 610), (x + 55, 650)], "#e5c33b", 8)


def draw_hazmat_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_building(draw, rng, damaged=severity >= 2, industrial=True)
    draw_fire(draw, 520, 438, 0.62, rng)
    draw_fire_engine(draw, 55, 752, rng)
    for x in (640, 700, 760):
        rect(draw, (x, 684, x + 48, 786), "#e2b542", width=5)
        line(draw, [(x, 718), (x + 48, 718)], "#1b2228", 4)
    ellipse(draw, (620, 792, 850, 855), "#7dbd57", "#2d5b35", 5)
    draw_firefighter(draw, 480, 675)


def draw_water_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    polygon(draw, [(275, 690), (600, 690), (535, 790), (325, 790)], "#e34a34", "#1b2228", 7)
    rect(draw, (370, 615, 485, 692), "#f1ece0", width=6)
    draw_windows(draw, [(390, 635, 455, 680)])
    line(draw, [(275, 690), (210, 650)], "#1b2228", 7)
    ellipse(draw, (705, 700, 780, 770), "#f28b27", width=5)
    line(draw, [(705, 735), (780, 735)], "#f8e2b4", 7)
    draw_firefighter(draw, 625, 715)
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
    draw_firefighter(draw, 640, 710)


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
    draw_firefighter(draw, 620, 720)


def draw_collapse_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_house(draw, rng, damaged=True)
    for x in range(370, 700, 55):
        polygon(draw, [(x, 790), (x + 42, 735), (x + 88, 790)], "#73777a", "#1b2228", 4)
    draw_fire_engine(draw, 80, 765, rng)
    draw_firefighter(draw, 520, 668)
    if severity >= 3:
        draw_ambulance(draw, 710, 760)


def draw_rescue_scene(draw: ImageDraw.ImageDraw, rng: random.Random, severity: int) -> None:
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
    draw_house(draw, rng, damaged=False)
    line(draw, [(240, 780), (580, 430)], "#d9d3bf", 12)
    for i in range(9):
        x = 260 + i * 35
        line(draw, [(x, 760 - i * 35), (x + 40, 720 - i * 35)], "#d9d3bf", 5)
    draw_fire_engine(draw, 100, 765, rng)
    draw_firefighter(draw, 600, 670, holding_hose=False)
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
    draw_tree_line(draw, rng)
    draw_sidewalk(draw)
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
    return img


def vehicle_kind(vehicle: dict[str, Any]) -> str:
    text = normalize_words(vehicle.get("id"), vehicle.get("name"), vehicle.get("category"))
    checks = [
        ("helicopter", ("helicopter",)),
        ("plane", ("plane", "airborne", "lead plane", "smoke jumper")),
        ("boat", ("boat", "coastal", "docks", "patrol")),
        ("ambulance", ("ambulance", "ems", "mobile air")),
        ("police", ("police", "fbi", "atf", "dea", "swat", "sheriff", "riot", "warden", "k-9")),
        ("command", ("command", "mcv", "battalion", "icpc", "fbpc", "fwdc", "wtc")),
        ("hazmat", ("hazmat",)),
        ("rescue", ("rescue", "crane")),
        ("trailer", ("trailer", "tower")),
        ("fire", ("engine", "fire", "foam", "arff", "wildland")),
    ]
    for kind, tokens in checks:
        if any(token in text for token in tokens):
            return kind
    return "support"


def draw_asset_station_backdrop(draw: ImageDraw.ImageDraw, rng: random.Random, apron: bool = True) -> None:
    draw_sky(draw, rng)
    draw_cloud(draw, 75, 88, 0.9)
    draw_cloud(draw, 730, 122, 0.72)
    draw_tree_line(draw, rng, 625)
    if apron:
        rect(draw, (0, 646, SIZE, SIZE), "#d2ccc0", width=0)
        for x in range(-70, SIZE, 160):
            line(draw, [(x, 1024), (x + 118, 646)], "#aaa69d", 4)
    draw_bricks(draw, 430, 300, 985, 656, rng)
    rect(draw, (400, 274, 1005, 314), "#2f353a", width=6)
    rect(draw, (532, 496, 674, 656), "#414950", width=6)
    rect(draw, (724, 488, 890, 656), "#414950", width=6)
    draw_windows(draw, [(468, 360, 585, 445), (820, 360, 935, 445)])


def draw_large_fire_engine(draw: ImageDraw.ImageDraw, rng: random.Random, kind: str = "fire") -> None:
    color = "#c7372f"
    accent = "#f1ead7"
    if kind == "hazmat":
        color = "#e2b542"
        accent = "#1f2832"
    elif kind == "rescue":
        color = "#d64b35"
    elif kind == "support":
        color = "#d34a38"

    rect(draw, (128, 608, 794, 748), color, width=8)
    polygon(draw, [(610, 608), (655, 520), (790, 520), (842, 608)], color, "#1b2228", 8)
    rect(draw, (166, 638, 332, 716), "#333b42", width=6)
    rect(draw, (360, 633, 468, 724), "#4a5259", width=5)
    rect(draw, (496, 633, 604, 724), "#4a5259", width=5)
    draw_windows(draw, [(665, 545, 768, 615)])
    line(draw, [(150, 675), (818, 675)], accent, 11)
    rect(draw, (650, 480, 760, 510), "#2f353a", width=5)
    rect(draw, (660, 474, 695, 508), "#e43a36", width=4)
    rect(draw, (700, 474, 735, 508), "#f1ead7", width=4)
    for px in range(184, 318, 28):
        ellipse(draw, (px, 656, px + 15, 671), "#d9d3bf", width=3)
    line(draw, [(180, 585), (340, 525), (555, 525), (715, 570)], "#d9d3bf", 11)
    line(draw, [(218, 564), (378, 505), (585, 505), (744, 550)], "#1b2228", 4)
    for wx in (238, 514, 742):
        ellipse(draw, (wx - 52, 714, wx + 52, 818), "#20252b", width=8)
        ellipse(draw, (wx - 24, 742, wx + 24, 790), "#8c949b", width=5)
    rect(draw, (106, 736, 838, 762), "#2f353a", width=5)


def draw_large_ambulance(draw: ImageDraw.ImageDraw) -> None:
    rect(draw, (160, 585, 740, 735), "#f3eee0", width=7)
    polygon(draw, [(610, 585), (648, 520), (770, 520), (820, 585)], "#f3eee0", "#1b2228", 7)
    draw_windows(draw, [(662, 542, 760, 602)])
    line(draw, [(238, 662), (520, 662)], "#d4473f", 18)
    line(draw, [(380, 595), (380, 725)], "#d4473f", 18)
    rect(draw, (655, 492, 742, 516), "#2f353a", width=4)
    for wx in (260, 695):
        ellipse(draw, (wx - 48, 700, wx + 48, 796), "#20252b", width=7)
        ellipse(draw, (wx - 22, 726, wx + 22, 770), "#8c949b", width=5)


def draw_large_police(draw: ImageDraw.ImageDraw) -> None:
    polygon(draw, [(180, 662), (310, 550), (620, 550), (755, 662)], "#f1ead7", "#1b2228", 7)
    rect(draw, (120, 650, 820, 768), "#1f2832", width=7)
    draw_windows(draw, [(320, 575, 430, 640), (450, 575, 570, 640)])
    rect(draw, (438, 512, 520, 540), "#2f353a", width=4)
    rect(draw, (448, 514, 482, 538), "#e43a36", width=3)
    rect(draw, (486, 514, 518, 538), "#2f65bf", width=3)
    for wx in (260, 680):
        ellipse(draw, (wx - 52, 725, wx + 52, 829), "#20252b", width=7)
        ellipse(draw, (wx - 24, 753, wx + 24, 801), "#8c949b", width=5)


def draw_large_boat(draw: ImageDraw.ImageDraw, rng: random.Random) -> None:
    rect(draw, (0, 665, SIZE, SIZE), "#2e88ad", width=0)
    for y in range(705, 1000, 62):
        line(draw, [(0, y), (SIZE, y + rng.randrange(-15, 16))], "#d9eef2", 4)
    polygon(draw, [(190, 645), (790, 645), (700, 805), (275, 805)], "#c7372f", "#1b2228", 8)
    rect(draw, (360, 515, 590, 648), "#f1ead7", width=7)
    draw_windows(draw, [(392, 546, 472, 608), (494, 546, 565, 608)])
    rect(draw, (460, 470, 520, 514), "#2f353a", width=5)


def draw_large_aircraft(draw: ImageDraw.ImageDraw, kind: str) -> None:
    rect(draw, (0, 670, SIZE, SIZE), "#4a4f55", width=0)
    line(draw, [(512, 672), (512, 1024)], "#f1ead7", 10)
    if kind == "helicopter":
        rect(draw, (350, 590, 690, 690), "#d94336", width=7)
        ellipse(draw, (292, 552, 720, 720), "#d94336", width=7)
        rect(draw, (662, 610, 850, 652), "#d94336", width=6)
        line(draw, [(205, 520), (820, 520)], "#1b2228", 10)
        line(draw, [(500, 452), (500, 585)], "#1b2228", 8)
        draw_windows(draw, [(382, 585, 480, 650), (500, 585, 610, 650)])
    else:
        polygon(draw, [(210, 650), (790, 610), (890, 658), (270, 718)], "#f1ead7", "#1b2228", 8)
        polygon(draw, [(470, 642), (565, 470), (625, 650)], "#d94336", "#1b2228", 7)
        polygon(draw, [(610, 640), (790, 515), (720, 670)], "#d94336", "#1b2228", 7)
        draw_windows(draw, [(340, 642, 405, 675), (425, 635, 490, 668), (510, 628, 575, 660)])


def render_vehicle(vehicle: dict[str, Any]) -> Image.Image:
    seed_text = str(vehicle.get("id") or vehicle.get("name") or vehicle.get("image"))
    rng = random.Random(slug_seed(seed_text))
    kind = vehicle_kind(vehicle)
    img = Image.new("RGB", (SIZE, SIZE), "#80cce8")
    draw = ImageDraw.Draw(img)
    draw_asset_station_backdrop(draw, rng, apron=kind not in {"boat", "plane", "helicopter"})
    if kind == "boat":
        draw_large_boat(draw, rng)
    elif kind in {"plane", "helicopter"}:
        draw_large_aircraft(draw, kind)
    elif kind == "ambulance":
        draw_large_ambulance(draw)
    elif kind == "police":
        draw_large_police(draw)
    elif kind in {"command", "support", "trailer"}:
        draw_command_van(draw, 310, 600)
        if kind == "trailer":
            rect(draw, (590, 662, 812, 754), "#4d565e", width=6)
            ellipse(draw, (655, 735, 710, 790), "#20252b", width=5)
    else:
        draw_large_fire_engine(draw, rng, kind)
    return img


def draw_equipment_panel(draw: ImageDraw.ImageDraw, rng: random.Random) -> None:
    draw_sky(draw, rng)
    draw_cloud(draw, 95, 95, 0.78)
    draw_cloud(draw, 720, 120, 0.62)
    draw_bricks(draw, 100, 245, 924, 710, rng)
    rect(draw, (72, 218, 952, 268), "#2f353a", width=7)
    rect(draw, (170, 325, 854, 615), "#475057", width=7)
    for x in range(205, 830, 56):
        line(draw, [(x, 342), (x, 595)], "#353d43", 2)
    for y in range(362, 595, 56):
        line(draw, [(188, y), (836, y)], "#353d43", 2)
    rect(draw, (0, 710, SIZE, SIZE), "#d2ccc0", width=0)
    for x in range(-70, SIZE, 160):
        line(draw, [(x, 1024), (x + 118, 710)], "#aaa69d", 4)
    rect(draw, (190, 690, 834, 780), "#2f353a", width=7)
    rect(draw, (222, 650, 802, 714), "#59636b", width=6)


def draw_tool_icon(draw: ImageDraw.ImageDraw, item: dict[str, Any]) -> None:
    text = normalize_words(item.get("id"), item.get("name"), item.get("category"))
    if "hose" in text or "water_supply" in text:
        for r in range(155, 35, -36):
            ellipse(draw, (512 - r, 470 - r, 512 + r, 470 + r), "#b9352c", width=16)
        rect(draw, (595, 560, 745, 610), "#4a5259", width=6)
    elif "breathing" in text or "ba_" in text:
        rect(draw, (410, 330, 520, 650), "#4a5259", width=8)
        rect(draw, (548, 350, 625, 630), "#2f353a", width=8)
        line(draw, [(520, 420), (548, 420)], "#1b2228", 9)
        ellipse(draw, (430, 245, 600, 385), "#1f2832", width=7)
    elif "rescue" in text or "stabilization" in text or "sked" in text or "litter" in text:
        line(draw, [(330, 640), (715, 340)], "#d94336", 28)
        line(draw, [(385, 305), (720, 640)], "#4a5259", 28)
        ellipse(draw, (300, 600, 398, 700), "#2f353a", width=6)
        ellipse(draw, (660, 300, 758, 398), "#2f353a", width=6)
    elif "ems" in text or "patient" in text or "medical" in text:
        rounded(draw, (345, 330, 695, 645), 34, "#d94336", width=8)
        rect(draw, (465, 282, 575, 340), "#d94336", width=7)
        line(draw, [(430, 488), (610, 488)], "#f1ead7", 30)
        line(draw, [(520, 398), (520, 578)], "#f1ead7", 30)
    elif "law" in text or "police" in text or "swat" in text or "tactical" in text:
        polygon(draw, [(512, 275), (705, 360), (670, 625), (512, 720), (354, 625), (319, 360)], "#1f2832", "#1b2228", 9)
        line(draw, [(410, 455), (614, 455)], "#2f65bf", 18)
        rect(draw, (455, 515, 570, 585), "#f1ead7", width=5)
    elif "hazmat" in text or "decon" in text or "foam" in text:
        rect(draw, (382, 315, 642, 670), "#e2b542", width=8)
        polygon(draw, [(512, 382), (604, 560), (420, 560)], "#1f2832", "#1b2228", 6)
        ellipse(draw, (475, 452, 550, 528), "#e2b542", width=5)
    elif "drone" in text:
        rect(draw, (420, 455, 600, 545), "#2f353a", width=7)
        for x, y in ((315, 350), (705, 350), (315, 650), (705, 650)):
            line(draw, [(512, 500), (x, y)], "#1b2228", 9)
            ellipse(draw, (x - 65, y - 65, x + 65, y + 65), "#4a5259", width=6)
            ellipse(draw, (x - 28, y - 28, x + 28, y + 28), "#f1ead7", width=4)
    elif "light" in text:
        rect(draw, (492, 340, 532, 715), "#4a5259", width=6)
        polygon(draw, [(512, 340), (360, 520), (664, 520)], "#f8e2b4", "#f8e2b4", 1)
        rect(draw, (430, 285, 595, 365), "#2f353a", width=7)
        line(draw, [(390, 715), (512, 610), (635, 715)], "#1b2228", 8)
    elif "camera" in text or "investigation" in text or "tablet" in text:
        rounded(draw, (360, 325, 690, 655), 26, "#2f353a", width=8)
        rect(draw, (405, 370, 645, 560), "#4d7380", width=5)
        ellipse(draw, (475, 430, 575, 530), "#1f2832", width=6)
        line(draw, [(625, 600), (720, 695)], "#1b2228", 12)
    else:
        rounded(draw, (350, 330, 690, 660), 26, "#4a5259", width=8)
        rect(draw, (430, 270, 610, 340), "#2f353a", width=7)
        line(draw, [(430, 468), (610, 468)], "#e7c43c", 18)


def render_equipment(item: dict[str, Any]) -> Image.Image:
    seed_text = str(item.get("id") or item.get("name") or item.get("image"))
    rng = random.Random(slug_seed(seed_text))
    img = Image.new("RGB", (SIZE, SIZE), "#80cce8")
    draw = ImageDraw.Draw(img)
    draw_equipment_panel(draw, rng)
    draw_tool_icon(draw, item)
    return img


def load_config(path: Path, key: str) -> list[dict[str, Any]]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    values = data.get(key, []) if isinstance(data, dict) else []
    if not isinstance(values, list):
        raise SystemExit(f"{path.name} does not contain a {key} list")
    return [value for value in values if isinstance(value, dict)]


def write_catalog_images(items: list[dict[str, Any]], image_prefix: str, renderer: Any) -> int:
    written = 0
    pattern = rf"{re.escape(image_prefix)}/[A-Za-z0-9_.() -]+\.png"
    for item in items:
        image_path = item.get("image")
        if not isinstance(image_path, str) or not image_path:
            continue
        if not re.fullmatch(pattern, image_path):
            raise SystemExit(f"Unexpected image path: {image_path}")
        output = FSC_ROOT / image_path
        output.parent.mkdir(parents=True, exist_ok=True)
        prepare_fsc_image(renderer(item)).save(output, "PNG", compress_level=6)
        written += 1
    return written


def write_legacy_mission_images() -> int:
    written = 0
    for mission_id in LEGACY_MISSION_IMAGE_IDS:
        item = {
            "id": mission_id,
            "name": mission_id.replace("_", " ").title(),
            "image": f"Images/Missions/{mission_id}.png",
        }
        output = FSC_ROOT / item["image"]
        output.parent.mkdir(parents=True, exist_ok=True)
        prepare_fsc_image(render_mission(item)).save(output, "PNG", compress_level=6)
        written += 1
    return written


def main() -> None:
    mission_count = write_catalog_images(load_config(MISSIONS_CONFIG, "missions"), "Images/Missions", render_mission)
    legacy_mission_count = write_legacy_mission_images()
    vehicle_count = write_catalog_images(load_config(VEHICLES_CONFIG, "vehicles"), "Images/Vehicles", render_vehicle)
    equipment_count = write_catalog_images(load_config(EQUIPMENT_CONFIG, "equipment"), "Images/Equipment", render_equipment)
    print(
        "Wrote "
        f"{mission_count + legacy_mission_count} mission images, "
        f"{vehicle_count} vehicle images and "
        f"{equipment_count} equipment images."
    )


if __name__ == "__main__":
    main()
