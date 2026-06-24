from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from difflib import SequenceMatcher
import hashlib
from html.parser import HTMLParser
import logging
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import discord
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box

log = logging.getLogger("red.cog.trainings_manager")

AMS = ZoneInfo("Europe/Amsterdam")

# ---------- Utilities ----------

def ts(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp())

def fmt_dt(dt: datetime) -> str:
    unix = ts(dt)
    return f"<t:{unix}:F> (Amsterdam)"

async def safe_update(interaction: discord.Interaction, *, content=None, embed=None, view=None):
    """Robust message updater for component/modal callbacks."""
    try:
        if not interaction.response.is_done():
            await interaction.response.edit_message(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: response.edit_message failed: %r", e)
    try:
        if getattr(interaction, "message", None) is not None:
            await interaction.message.edit(content=content, embed=embed, view=view)
            return
    except Exception as e:
        log.debug("safe_update: message.edit failed: %r", e)
    try:
        await interaction.followup.send(content or "Updated.", embed=embed, view=view, ephemeral=True)
    except Exception as e:
        log.exception("safe_update completely failed: %r", e)
        try:
            if not interaction.response.is_done():
                await interaction.response.send_message(content or "Updated.", embed=embed, view=view, ephemeral=True)
        except Exception:
            pass

# ---------- Static data ----------

DISCIPLINES: Dict[str, List[Tuple[str, int]]] = {
    "Coastal": [
        ("Coastal Air Rescue Operations", 5),
        ("Lifeguard Supervisor", 5),
        ("Lifeguard Training", 5),
        ("Ocean Navigation", 5),
        ("Sharpshooter Training", 5),
        ("Swift water rescue", 4),
        ("TACLET", 3),
    ],
    "Police": [
        ("Drone Operator", 5),
        ("Environmental Game Warden", 4),
        ("FBI Bomb Technician", 5),
        ("FBI Mobile Center Commander", 7),
        ("K-9", 5),
        ("Ocean Navigation", 5),
        ("Police Aviation", 7),
        ("Police Motorcycle", 3),
        ("Police Operations Management", 5),
        ("Police Supervisor / Sheriff", 5),
        ("Riot Police Training", 3),
        ("SWAT", 5),
        ("Sharpshooter Training", 5),
        ("Swift water rescue", 4),
        ("Tactical Rescue Training", 5),
        ("Traffic Control Training", 3),
    ],
    "Fire": [
        ("ALS Medical Training for Fire Apparatus", 3),
        ("ARFF-Training", 3),
        ("Airborne firefighting", 5),
        ("Critical Care", 5),
        ("EMS Mobile Command", 7),
        ("HazMat", 3),
        ("Heavy Machinery Operating", 3),
        ("Hooklift Truck Driving", 4),
        ("Hotshot Crew Training", 3),
        ("Law Enforcement for Arson Investigation", 4),
        ("Lifeguard Supervisor", 5),
        ("Lifeguard Training", 5),
        ("Mobile command", 5),
        ("Ocean Navigation", 5),
        ("Search and Rescue Training", 4),
        ("Smoke Jumper Training", 3),
        ("Swift water rescue", 4),
        ("Tactical Medic Training", 4),
        ("Technical Rescue Training", 4),
        ("Traffic Control Training", 3),
        ("Truck Driver's License", 2),
        ("Wildland Lead Pilot Training", 7),
        ("Wildland Mobile Command Center Training", 5),
    ],
    "EMS": [
        ("ALS Medical Training for Fire Apparatus", 3),
        ("Critical Care", 5),
        ("EMS Mobile Command", 7),
        ("Hazmat Medic Training", 3),
        ("Tactical Medic Training", 4),
        ("Truck Driver's License", 2),
        ("Mountain Dog Training", 5),
        ("Mountain Rescue Certificate", 5)
    ],
    "OTHER": [
        ("Not specified (USE REFERENCE)", 0),
        ("TEST DEBUG (DO NOT USE)", 0),
    ],
}

FEE_CHOICES = [0, 100, 200, 300, 400, 500]
MEMBER_PANEL_CHANNEL_ID = 1421627971831070730
DEVELOPER_PANEL_CHANNEL_ID = 1421242306136113254
AUTO_BUILDING_LIST_PATH = "/verband/gebauede"
AUTO_ACADEMY_LIST_MAX_PAGES = 25
AUTO_ACADEMY_BUILDINGS = {
    "Fire": 4951748,
    "Police": 4951746,
}
AUTO_ALLIANCE_DURATION_SECONDS = 3600
AUTO_MIN_CONTRIBUTION_RATE = 5.0
AUTO_MAX_CLASSES = 4
AVAILABILITY_REFRESH_SECONDS = 60 * 60
BOARD_THREAD_ID = 5935
BOARD_POLL_SECONDS = 5 * 60
BOARD_DEFAULT_FEE = 0
BOARD_MATCH_THRESHOLD = 0.78
BOARD_GUIDE_MARKER_PREFIX = "TM-GUIDE"
BOARD_GUIDE_OVERVIEW_SECTION = "overview"
BOARD_GUIDE_MAX_SCAN_PAGES = 25
BOARD_GUIDE_SYNC_SECONDS = AVAILABILITY_REFRESH_SECONDS
AGENCY_ORDER = ("Fire", "Police", "EMS", "Coastal")
BOARD_GUIDE_SECTIONS = (BOARD_GUIDE_OVERVIEW_SECTION, *AGENCY_ORDER)


@dataclass
class AcademyCourse:
    label: str
    normalized_label: str
    value: str


@dataclass
class AcademyPage:
    action: Optional[str]
    authenticity_token: Optional[str]
    available_rooms: int
    costs: List[int]
    courses: List[AcademyCourse]


@dataclass
class AvailableAcademy:
    building_id: int
    name: str
    discipline: str
    has_start_button: bool = False


@dataclass
class AutoTrainingResult:
    success: bool
    reason: str
    academy_id: Optional[int] = None
    mc_user_id: Optional[str] = None
    mc_username: Optional[str] = None
    contribution_rate: Optional[float] = None
    course_value: Optional[str] = None
    classes_opened: int = 0
    status: Optional[int] = None


@dataclass
class DisciplineAvailability:
    discipline: str
    academies_checked: int = 0
    academies_available: int = 0
    available_classrooms: int = 0
    errors: int = 0


@dataclass
class BoardTrainingPost:
    post_id: int
    author_id: Optional[str]
    author_name: str
    created_at: str
    content: str


@dataclass
class BoardPage:
    posts: List[BoardTrainingPost]
    last_page: int = 1
    current_user_id: Optional[str] = None
    reply_action: Optional[str] = None
    reply_token: Optional[str] = None


@dataclass(frozen=True)
class TrainingCatalogEntry:
    discipline: str
    training: str
    days: int
    normalized: str


@dataclass(frozen=True)
class BoardTrainingMatch:
    discipline: str
    training: str
    days: int
    matched_text: str
    score: float


@dataclass
class MissionChiefForm:
    action: Optional[str]
    method: str
    fields: Dict[str, str]


def _normalize_training_name(name: str) -> str:
    cleaned = re.sub(r"\(\s*\d+\s+days?\s*\)", "", str(name), flags=re.IGNORECASE)
    cleaned = cleaned.replace("’", "'")
    return re.sub(r"\s+", " ", cleaned).strip().casefold()


def _normalize_training_search_text(value: str) -> str:
    text = _normalize_training_name(value)
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _training_catalog() -> List[TrainingCatalogEntry]:
    entries: List[TrainingCatalogEntry] = []
    seen: set[Tuple[str, str]] = set()
    for discipline, trainings in DISCIPLINES.items():
        if discipline == "OTHER":
            continue
        for training, days in trainings:
            key = (discipline, training)
            if key in seen:
                continue
            seen.add(key)
            entries.append(
                TrainingCatalogEntry(
                    discipline=discipline,
                    training=training,
                    days=int(days),
                    normalized=_normalize_training_search_text(training),
                )
            )
    return entries


def extract_board_training_matches(text: str) -> List[BoardTrainingMatch]:
    """Extract one or more training requests from free-form board text."""
    normalized_text = _normalize_training_search_text(text)
    if not normalized_text:
        return []

    matches: List[BoardTrainingMatch] = []
    seen_trainings: set[Tuple[str, str]] = set()
    catalog = _training_catalog()

    for entry in catalog:
        if entry.normalized and re.search(rf"\b{re.escape(entry.normalized)}\b", normalized_text):
            seen_trainings.add((entry.discipline, entry.training))
            matches.append(
                BoardTrainingMatch(
                    discipline=entry.discipline,
                    training=entry.training,
                    days=entry.days,
                    matched_text=entry.training,
                    score=1.0,
                )
            )

    chunks = [
        _normalize_training_search_text(chunk)
        for chunk in re.split(r"[\n;,/|]+|\band\b|&|\+", str(text), flags=re.IGNORECASE)
    ]
    chunks = [chunk for chunk in chunks if len(chunk) >= 3]
    for chunk in chunks:
        best: Tuple[float, Optional[TrainingCatalogEntry]] = (0.0, None)
        for entry in catalog:
            key = (entry.discipline, entry.training)
            if key in seen_trainings:
                continue
            if not entry.normalized:
                continue
            score = SequenceMatcher(None, chunk, entry.normalized).ratio()
            if entry.normalized in chunk:
                score = max(score, 0.88)
            if score > best[0]:
                best = (score, entry)

        score, entry = best
        if entry and score >= BOARD_MATCH_THRESHOLD:
            seen_trainings.add((entry.discipline, entry.training))
            matches.append(
                BoardTrainingMatch(
                    discipline=entry.discipline,
                    training=entry.training,
                    days=entry.days,
                    matched_text=chunk,
                    score=score,
                )
            )

    return matches


class TrainingBoardPageParser(HTMLParser):
    """Parse MissionChief alliance board pages for training requests and reply form data."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.posts: List[BoardTrainingPost] = []
        self.page_numbers: List[int] = []
        self.current_user_id: Optional[str] = None
        self.reply_action: Optional[str] = None
        self.reply_token: Optional[str] = None
        self._post: Optional[dict] = None
        self._post_depth = 0
        self._content_depth = 0
        self._capture_author = False
        self._capture_content = False
        self._capture_page_number = False
        self._capture_active_page = False

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}

        if tag == "div" and str(attr.get("id") or "").startswith("post-on-page-"):
            self._post = {
                "post_id": None,
                "author_id": None,
                "author_name": "",
                "created_at": "",
                "content": [],
            }
            self._post_depth = 1
            return

        if self._post is not None and tag == "div":
            self._post_depth += 1
            classes = str(attr.get("class") or "")
            if "col-md-11" in classes:
                self._content_depth = self._post_depth
                self._capture_content = True

        if self._post is not None and tag == "a":
            href = str(attr.get("href") or "")
            profile_match = re.search(r"/profile/(\d+)", href)
            if profile_match and not self._post.get("author_id"):
                self._post["author_id"] = profile_match.group(1)
                self._capture_author = True

            post_match = re.search(r"/alliance_posts/(\d+)", href)
            if post_match:
                self._post["post_id"] = int(post_match.group(1))

        if self._post is not None and tag == "span":
            title = attr.get("title")
            if title and not self._post.get("created_at"):
                self._post["created_at"] = str(title)

        if self._post is not None and self._capture_content and tag == "br":
            self._post["content"].append("\n")

        if tag == "a":
            href = str(attr.get("href") or "")
            page_match = re.search(r"[?&]page=(\d+)", href)
            if page_match:
                self.page_numbers.append(int(page_match.group(1)))
            self._capture_page_number = bool(page_match)

        if tag == "li" and "active" in str(attr.get("class") or ""):
            self._capture_active_page = True

        if tag == "form" and str(attr.get("id") or "") == "new_alliance_post":
            self.reply_action = attr.get("action")

        if tag == "input" and attr.get("name") == "authenticity_token":
            token = attr.get("value")
            if token:
                self.reply_token = token

    def handle_data(self, data: str):
        if "user_id =" in data:
            match = re.search(r"user_id\s*=\s*(\d+)", data)
            if match:
                self.current_user_id = match.group(1)

        if self._post is not None and self._capture_author:
            text = re.sub(r"\s+", " ", data).strip()
            if text:
                self._post["author_name"] = text

        if self._post is not None and self._capture_content:
            self._post["content"].append(data)

        if self._capture_page_number:
            try:
                self.page_numbers.append(int(data.strip()))
            except ValueError:
                pass

        if self._capture_active_page:
            try:
                self.page_numbers.append(int(data.strip()))
            except ValueError:
                pass

    def handle_endtag(self, tag: str):
        if self._capture_author and tag == "a":
            self._capture_author = False

        if self._capture_page_number and tag == "a":
            self._capture_page_number = False

        if self._capture_active_page and tag == "li":
            self._capture_active_page = False

        if self._post is not None and tag == "div":
            if self._capture_content and self._post_depth == self._content_depth:
                self._capture_content = False
                self._content_depth = 0

            self._post_depth -= 1
            if self._post_depth <= 0:
                self._finish_post()

    def _finish_post(self) -> None:
        if self._post is None:
            return

        post_id = self._post.get("post_id")
        if post_id is None:
            self._post = None
            return

        content = "".join(self._post.get("content") or [])
        content = re.sub(r"\n\s*\n+", "\n", content)
        content = re.sub(r"[ \t]+", " ", content).strip()
        self.posts.append(
            BoardTrainingPost(
                post_id=int(post_id),
                author_id=self._post.get("author_id"),
                author_name=str(self._post.get("author_name") or "Unknown"),
                created_at=str(self._post.get("created_at") or ""),
                content=content,
            )
        )
        self._post = None

    def page(self) -> BoardPage:
        return BoardPage(
            posts=self.posts,
            last_page=max(self.page_numbers or [1]),
            current_user_id=self.current_user_id,
            reply_action=self.reply_action,
            reply_token=self.reply_token,
        )


def parse_training_board_page(html: str) -> BoardPage:
    parser = TrainingBoardPageParser()
    parser.feed(html or "")
    return parser.page()


class MissionChiefFormParser(HTMLParser):
    """Small generic parser for MissionChief Rails forms."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.forms: List[MissionChiefForm] = []
        self._form: Optional[dict] = None
        self._textarea_name: Optional[str] = None
        self._textarea_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}
        if tag == "form":
            self._form = {
                "action": attr.get("action"),
                "method": str(attr.get("method") or "get").lower(),
                "fields": {},
            }
            return

        if self._form is None:
            return

        if tag == "input":
            name = attr.get("name")
            if name:
                self._form["fields"][name] = attr.get("value") or ""
        elif tag == "textarea":
            name = attr.get("name")
            if name:
                self._textarea_name = name
                self._textarea_text = []
        elif tag == "select":
            name = attr.get("name")
            if name and name not in self._form["fields"]:
                self._form["fields"][name] = ""

    def handle_data(self, data: str):
        if self._form is not None and self._textarea_name:
            self._textarea_text.append(data)

    def handle_endtag(self, tag: str):
        if self._form is None:
            return
        if tag == "textarea" and self._textarea_name:
            self._form["fields"][self._textarea_name] = "".join(self._textarea_text)
            self._textarea_name = None
            self._textarea_text = []
        elif tag == "form":
            self.forms.append(
                MissionChiefForm(
                    action=self._form.get("action"),
                    method=str(self._form.get("method") or "get").lower(),
                    fields=dict(self._form.get("fields") or {}),
                )
            )
            self._form = None


def parse_missionchief_forms(html: str) -> List[MissionChiefForm]:
    parser = MissionChiefFormParser()
    parser.feed(html or "")
    return parser.forms


class AcademyPageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.action: Optional[str] = None
        self.authenticity_token: Optional[str] = None
        self.room_options: List[int] = []
        self.cost_options: List[int] = []
        self.courses: List[AcademyCourse] = []
        self._select_name: Optional[str] = None
        self._current_option_value: Optional[str] = None
        self._current_option_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}
        if tag == "form":
            action = attr.get("action")
            if action and "/education" in action:
                self.action = action
        elif tag == "input" and attr.get("name") == "authenticity_token":
            self.authenticity_token = attr.get("value")
        elif tag == "select":
            self._select_name = attr.get("name")
        elif tag == "option" and self._select_name:
            self._current_option_value = attr.get("value") or ""
            self._current_option_text = []

    def handle_data(self, data: str):
        if self._current_option_value is not None:
            self._current_option_text.append(data)

    def handle_endtag(self, tag: str):
        if tag == "option" and self._select_name and self._current_option_value is not None:
            text = re.sub(r"\s+", " ", "".join(self._current_option_text)).strip()
            value = self._current_option_value
            if self._select_name == "building_rooms_use":
                try:
                    self.room_options.append(int(value))
                except ValueError:
                    pass
            elif self._select_name == "alliance[cost]":
                try:
                    self.cost_options.append(int(value))
                except ValueError:
                    pass
            elif self._select_name == "education_select" and value and text:
                self.courses.append(
                    AcademyCourse(
                        label=text,
                        normalized_label=_normalize_training_name(text),
                        value=value,
                    )
                )
            self._current_option_value = None
            self._current_option_text = []
        elif tag == "select":
            self._select_name = None

    def page(self) -> AcademyPage:
        return AcademyPage(
            action=self.action,
            authenticity_token=self.authenticity_token,
            available_rooms=max(self.room_options) if self.room_options else 0,
            costs=sorted(set(self.cost_options)),
            courses=self.courses,
        )


def parse_academy_page(html: str) -> AcademyPage:
    parser = AcademyPageParser()
    parser.feed(html)
    return parser.page()


class MissionChiefProfileParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.title_text = ""
        self.heading_texts: List[str] = []
        self._in_title = False
        self._heading_depth = 0
        self._current_heading: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        del attrs
        if tag == "title":
            self._in_title = True
        elif tag in {"h1", "h2"}:
            self._heading_depth += 1
            self._current_heading = []

    def handle_data(self, data: str):
        if self._in_title:
            self.title_text += data
        if self._heading_depth:
            self._current_heading.append(data)

    def handle_endtag(self, tag: str):
        if tag == "title":
            self._in_title = False
        elif tag in {"h1", "h2"} and self._heading_depth:
            text = re.sub(r"\s+", " ", "".join(self._current_heading)).strip()
            if text:
                self.heading_texts.append(text)
            self._heading_depth = 0
            self._current_heading = []


def parse_profile_username(html: str) -> Optional[str]:
    parser = MissionChiefProfileParser()
    parser.feed(html)
    candidates = parser.heading_texts
    if parser.title_text:
        candidates.append(parser.title_text)
    for candidate in candidates:
        cleaned = re.sub(r"\s+", " ", candidate).strip()
        cleaned = re.sub(r"\s*-\s*MISSIONCHIEF\.COM.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"^Profile\s*[:\-]\s*", "", cleaned, flags=re.IGNORECASE)
        if cleaned and "missionchief" not in cleaned.casefold():
            return cleaned
    return None


def _is_known_mc_username(value: Optional[str]) -> bool:
    if not value:
        return False
    cleaned = str(value).strip()
    return cleaned.casefold() not in {"unknown", "unknown user", "n/a", "none", "-"}


def infer_academy_discipline(text: str) -> Optional[str]:
    haystack = (text or "").casefold()
    if any(token in haystack for token in ("police", "polizeischule", "politie")):
        return "Police"
    if any(token in haystack for token in ("coastal", "water rescue", "water_rescue_school", "coast")):
        return "Coastal"
    if any(
        token in haystack
        for token in (
            "ems",
            "ems_school",
            "ems-school",
            "ambulance",
            "ambulance_school",
            "ambulance-school",
            "medical",
            "rescue_school",
            "rescue-school",
            "rescueschool",
            "rescue academy",
            "rettungsschule",
        )
    ):
        return "EMS"
    if any(token in haystack for token in ("fire", "fireschool", "brandweer")):
        return "Fire"
    return None


class AllianceAcademyListParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.academies: List[AvailableAcademy] = []
        self.next_page_path: Optional[str] = None
        self._in_row = False
        self._row_name = ""
        self._row_text = ""
        self._row_discipline: Optional[str] = None
        self._row_building_id: Optional[int] = None
        self._row_start_building_id: Optional[int] = None
        self._row_has_start_button = False
        self._link_href: Optional[str] = None
        self._link_class_text = ""
        self._link_text = ""

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]):
        attr = {key: value for key, value in attrs}
        if tag == "a":
            self._link_href = attr.get("href")
            classes = attr.get("class") or ""
            self._link_class_text = " ".join(classes) if isinstance(classes, list) else str(classes)
            self._link_text = ""
            rel = attr.get("rel") or ""
            rel_text = " ".join(rel) if isinstance(rel, list) else str(rel)
            if self._link_href and "next" in rel_text.casefold():
                self.next_page_path = self._link_href

        if tag == "tr":
            self._in_row = True
            self._row_name = attr.get("search_attribute") or ""
            self._row_text = self._row_name
            self._row_discipline = None
            self._row_building_id = None
            self._row_start_building_id = None
            self._row_has_start_button = False
            return

        if not self._in_row:
            return

        if tag == "img":
            image_text = " ".join(
                str(attr.get(key) or "")
                for key in ("src", "alt", "title")
            )
            self._row_text += f" {image_text}"
            self._row_discipline = self._row_discipline or infer_academy_discipline(image_text)
            building_id = attr.get("building_id")
            if building_id and str(building_id).isdigit() and self._row_building_id is None:
                self._row_building_id = int(building_id)
        elif tag == "a":
            href = re.sub(r"\s+", "", attr.get("href") or "")
            match = re.search(r"/buildings/(\d+)", href)
            if match and self._row_building_id is None:
                self._row_building_id = int(match.group(1))
            if "btn-success" in self._link_class_text:
                if match:
                    self._row_start_building_id = int(match.group(1))
                    self._row_has_start_button = True

    def handle_data(self, data: str):
        if self._in_row:
            self._row_text += f" {data}"
        if self._link_href is not None:
            self._link_text += data

    def handle_endtag(self, tag: str):
        if tag == "a" and self._link_href is not None:
            if "next" in self._link_text.casefold():
                self.next_page_path = self._link_href
            if self._in_row:
                if "start a new training course" in self._link_text.casefold():
                    href = re.sub(r"\s+", "", self._link_href)
                    match = re.search(r"/buildings/(\d+)", href)
                    if match:
                        self._row_start_building_id = int(match.group(1))
                        self._row_has_start_button = True
            self._link_href = None
            self._link_class_text = ""
            self._link_text = ""
            return

        if tag != "tr" or not self._in_row:
            return

        building_id = self._row_start_building_id or self._row_building_id
        if building_id and self._row_discipline:
            self.academies.append(
                AvailableAcademy(
                    building_id=building_id,
                    name=self._row_name.strip() or f"Building {building_id}",
                    discipline=self._row_discipline,
                    has_start_button=self._row_has_start_button,
                )
            )

        self._in_row = False
        self._row_name = ""
        self._row_text = ""
        self._row_discipline = None
        self._row_building_id = None
        self._row_start_building_id = None
        self._row_has_start_button = False


def parse_available_academies(html: str) -> List[AvailableAcademy]:
    parser = AllianceAcademyListParser()
    parser.feed(html)
    return parser.academies


def parse_available_academies_page(html: str) -> Tuple[List[AvailableAcademy], Optional[str]]:
    parser = AllianceAcademyListParser()
    parser.feed(html)
    return parser.academies, parser.next_page_path

# ---------- Model ----------

class TrainingRequest:
    def __init__(
        self,
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        fee_per_day: int,
        num_classes: int,
        references: Optional[List[str]],
        want_reminder: bool,
        request_channel_id: int,
        summary_message_link: Optional[str] = None,
        reminder_only: bool = False,
    ):
        self.user_id = user_id
        self.discipline = discipline
        self.training = training
        self.days = days
        self.fee_per_day = fee_per_day
        self.num_classes = num_classes
        self.references = references or []
        self.want_reminder = want_reminder
        self.request_channel_id = request_channel_id
        self.summary_message_link = summary_message_link
        self.reminder_only = reminder_only

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "discipline": self.discipline,
            "training": self.training,
            "days": self.days,
            "fee_per_day": self.fee_per_day,
            "num_classes": self.num_classes,
            "references": self.references,
            "want_reminder": self.want_reminder,
            "request_channel_id": self.request_channel_id,
            "summary_message_link": self.summary_message_link,
            "reminder_only": self.reminder_only,
        }

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            user_id=data["user_id"],
            discipline=data["discipline"],
            training=data["training"],
            days=int(data["days"]),
            fee_per_day=int(data["fee_per_day"]),
            num_classes=int(data.get("num_classes", 1)),
            references=data.get("references", []),
            want_reminder=bool(data.get("want_reminder", False)),
            request_channel_id=int(data["request_channel_id"]),
            summary_message_link=data.get("summary_message_link"),
            reminder_only=bool(data.get("reminder_only", False)),
        )

# ---------- Views ----------

class StartView(discord.ui.View):
    def __init__(self, cog: "TrainingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Start Request", style=discord.ButtonStyle.primary, custom_id="tm:start_request")
    async def start_request(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Let's set up your training request. First, pick a discipline.",
            view=DisciplineView(self.cog, reminder_only=False),
            ephemeral=True,
        )

    @discord.ui.button(label="Reminder Only", style=discord.ButtonStyle.secondary, custom_id="tm:reminder_only")
    async def reminder_only(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Let's set up a reminder for your training. First, pick a discipline.",
            view=DisciplineView(self.cog, reminder_only=True),
            ephemeral=True,
        )

class DisciplineView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", reminder_only: bool = False, auto_open: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.reminder_only = reminder_only
        self.auto_open = auto_open
        self.add_item(DisciplineSelect(self.cog, reminder_only, auto_open))

class DisciplineSelect(discord.ui.Select):
    def __init__(self, cog: "TrainingManager", reminder_only: bool = False, auto_open: bool = False):
        self.cog = cog
        self.reminder_only = reminder_only
        self.auto_open = auto_open
        options = [discord.SelectOption(label=k, description=f"{len(v)} trainings") for k, v in DISCIPLINES.items()]
        super().__init__(placeholder="Choose a discipline", min_values=1, max_values=1, options=options, custom_id="tm:discipline")

    async def callback(self, interaction: discord.Interaction):
        discipline = self.values[0]
        await safe_update(
            interaction,
            content=f"Discipline selected: **{discipline}**. Now choose a training.",
            view=TrainingView(self.cog, discipline, self.reminder_only, self.auto_open),
        )

class TrainingView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, reminder_only: bool = False, auto_open: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.discipline = discipline
        self.reminder_only = reminder_only
        self.auto_open = auto_open
        self.add_item(TrainingSelect(self.cog, discipline, reminder_only, auto_open))

class TrainingSelect(discord.ui.Select):
    def __init__(self, cog: "TrainingManager", discipline: str, reminder_only: bool = False, auto_open: bool = False):
        self.cog = cog
        self.discipline = discipline
        self.reminder_only = reminder_only
        self.auto_open = auto_open
        options = []
        for name, days in DISCIPLINES[discipline]:
            label = name
            desc = f"Duration: {days} day" + ("" if days == 1 else "s")
            options.append(discord.SelectOption(label=label, description=desc))
        super().__init__(placeholder="Choose a training", min_values=1, max_values=1, options=options, custom_id="tm:training")

    async def callback(self, interaction: discord.Interaction):
        training = self.values[0]
        days = next(days for name, days in DISCIPLINES[self.discipline] if name == training)
        
        if self.reminder_only:
            # Skip fee and class count for reminder only
            await safe_update(
                interaction,
                content=f"Selected: **{self.discipline} → {training}** ({days} days). Do you want to add a reference?",
                view=ReferenceAskView(self.cog, self.discipline, training, days, 0, 1, reminder_only=True, auto_open=self.auto_open),
            )
        else:
            await safe_update(
                interaction,
                content=f"Selected: **{self.discipline} → {training}**. Now pick the fee per day, per trainee.",
                view=FeeView(self.cog, self.discipline, training, days, self.auto_open),
            )

class FeeView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, auto_open: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.discipline = discipline
        self.training = training
        self.days = days
        self.auto_open = auto_open
        for fee in FEE_CHOICES:
            label = "Free" if fee == 0 else f"{fee} credits/day"
            self.add_item(FeeButton(label, fee))

class FeeButton(discord.ui.Button):
    def __init__(self, label: str, fee: int):
        super().__init__(label=label, style=discord.ButtonStyle.secondary, custom_id=f"tm:fee:{label}")
        self.fee = fee

    async def callback(self, interaction: discord.Interaction):
        view: FeeView = self.view  # type: ignore
        await safe_update(
            interaction,
            content=(
                f"Selected: **{view.discipline} → {view.training}** for "
                f"**{'Free' if self.fee == 0 else str(self.fee) + ' credits/day'}**.\n"
                "How many classes do you want to request?"
            ),
            view=ClassCountView(view.cog, view.discipline, view.training, view.days, self.fee, view.auto_open),
        )

class ClassCountView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, auto_open: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee)
        self.auto_open = auto_open

    @discord.ui.button(label="No, just one", style=discord.ButtonStyle.primary, custom_id="tm:class_1")
    async def one_class(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="1 class selected. Do you want to add a reference?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 1, reminder_only=False, auto_open=self.auto_open),
        )

    @discord.ui.button(label="2", style=discord.ButtonStyle.secondary, custom_id="tm:class_2")
    async def two_classes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="2 classes selected. Do you want to add references?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 2, reminder_only=False, auto_open=self.auto_open),
        )

    @discord.ui.button(label="3", style=discord.ButtonStyle.secondary, custom_id="tm:class_3")
    async def three_classes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="3 classes selected. Do you want to add references?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 3, reminder_only=False, auto_open=self.auto_open),
        )

    @discord.ui.button(label="4", style=discord.ButtonStyle.secondary, custom_id="tm:class_4")
    async def four_classes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee = self.state
        await safe_update(
            interaction,
            content="4 classes selected. Do you want to add references?",
            view=ReferenceAskView(self.cog, discipline, training, days, fee, 4, reminder_only=False, auto_open=self.auto_open),
        )

class ReferenceAskView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, num_classes: int, reminder_only: bool = False, auto_open: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee, num_classes, reminder_only)
        self.auto_open = auto_open

    @discord.ui.button(label="Yes, add references", style=discord.ButtonStyle.primary, custom_id="tm:ref_yes")
    async def yes(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        
        if num_classes > 1 and not reminder_only:
            # Ask if same or individual
            await safe_update(
                interaction,
                content="Do you want the same reference for all classes, or individual references?",
                view=ReferenceModeView(self.cog, discipline, training, days, fee, num_classes, reminder_only, self.auto_open),
            )
        else:
            # Single reference
            await interaction.response.send_modal(ReferenceModal(self.cog, discipline, training, days, fee, num_classes, reminder_only, mode="single", auto_open=self.auto_open))

    @discord.ui.button(label="No, continue", style=discord.ButtonStyle.secondary, custom_id="tm:ref_no")
    async def no(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        if reminder_only:
            view = ReminderOnlySummaryView(self.cog, interaction.user.id, discipline, training, days, [])
            await view.send_summary(interaction)
        else:
            summary_cls = DeveloperAutoOpenSummaryView if self.auto_open else SummaryView
            await safe_update(
                interaction,
                content="References skipped.",
                view=summary_cls(self.cog, interaction.user.id, discipline, training, days, fee, num_classes, []),
            )

class ReferenceModeView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, num_classes: int, reminder_only: bool, auto_open: bool = False):
        super().__init__(timeout=600)
        self.cog = cog
        self.state = (discipline, training, days, fee, num_classes, reminder_only)
        self.auto_open = auto_open

    @discord.ui.button(label="Same for all", style=discord.ButtonStyle.primary, custom_id="tm:ref_same")
    async def same_ref(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        await interaction.response.send_modal(ReferenceModal(self.cog, discipline, training, days, fee, num_classes, reminder_only, mode="same", auto_open=self.auto_open))

    @discord.ui.button(label="Individual", style=discord.ButtonStyle.secondary, custom_id="tm:ref_individual")
    async def individual_ref(self, interaction: discord.Interaction, button: discord.ui.Button):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        await interaction.response.send_modal(ReferenceModal(self.cog, discipline, training, days, fee, num_classes, reminder_only, mode="individual", auto_open=self.auto_open))

class ReferenceModal(discord.ui.Modal):
    def __init__(self, cog: "TrainingManager", discipline: str, training: str, days: int, fee: int, num_classes: int, reminder_only: bool, mode: str = "single", auto_open: bool = False):
        self.cog = cog
        self.state = (discipline, training, days, fee, num_classes, reminder_only)
        self.mode = mode
        self.auto_open = auto_open
        
        if mode == "individual":
            super().__init__(title="Add references for each class")
            for i in range(1, num_classes + 1):
                self.add_item(discord.ui.TextInput(
                    label=f"Class {i} reference (max 100 chars)",
                    style=discord.TextStyle.short,
                    max_length=100,
                    required=False,
                    placeholder=f"e.g., SWAT Team {i}",
                    custom_id=f"ref_{i}"
                ))
        else:
            super().__init__(title="Add reference")
            self.ref = discord.ui.TextInput(
                label="Reference (max 100 characters)",
                style=discord.TextStyle.short,
                max_length=100,
                required=True,
                placeholder="e.g., SWAT Team East, batch 3",
            )
            # FIX: Add the TextInput to the modal
            self.add_item(self.ref)

    async def on_submit(self, interaction: discord.Interaction):
        discipline, training, days, fee, num_classes, reminder_only = self.state
        
        if self.mode == "individual":
            references = []
            for child in self.children:
                if isinstance(child, discord.ui.TextInput):
                    val = str(child.value).strip()
                    references.append(val if val else None)
        elif self.mode == "same":
            ref_text = str(self.ref).strip()
            references = [ref_text] * num_classes if ref_text else []
        else:  # single
            ref_text = str(self.ref).strip()
            references = [ref_text] if ref_text else []
        
        if reminder_only:
            view = ReminderOnlySummaryView(self.cog, interaction.user.id, discipline, training, days, references)
            await view.send_summary(interaction)
        else:
            summary_cls = DeveloperAutoOpenSummaryView if self.auto_open else SummaryView
            await safe_update(
                interaction,
                content="References added.",
                view=summary_cls(self.cog, interaction.user.id, discipline, training, days, fee, num_classes, references),
            )

# ---------- Reminder Only Summary View ----------

class ReminderOnlySummaryView(discord.ui.View):
    def __init__(
        self,
        cog: "TrainingManager",
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        references: List[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.user_id = user_id
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=0,
            num_classes=1,
            references=references,
            want_reminder=True,
            request_channel_id=0,
            reminder_only=True,
        )

    async def send_summary(self, interaction: discord.Interaction):
        """Display the summary embed with action buttons."""
        user = interaction.user
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        embed = discord.Embed(
            title="Reminder - Summary",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Discipline", value=self.req.discipline, inline=True)
        embed.add_field(name="Training", value=self.req.training, inline=True)
        embed.add_field(name="Duration", value=f"{self.req.days} days", inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        ref_text = self.req.references[0] if self.req.references else "—"
        embed.add_field(name="Reference", value=ref_text, inline=False)
        embed.set_footer(text="Click 'Start Reminder' to confirm or 'Cancel' to abort.")

        await safe_update(interaction, content="Review your reminder:", embed=embed, view=self)

    @discord.ui.button(label="Start Reminder", style=discord.ButtonStyle.success, custom_id="tm:start_reminder")
    async def start_reminder(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        conf = await self.cog.config.guild(guild).all()
        log_channel_id = conf.get("log_channel_id")
        request_channel_id = conf.get("request_channel_id")

        if not log_channel_id or not request_channel_id:
            await interaction.response.send_message(
                "Log/Request channels are not configured yet. Ask an admin to use [p]tmset.",
                ephemeral=True,
            )
            return

        log_channel = guild.get_channel(log_channel_id)
        request_channel = guild.get_channel(request_channel_id)

        if not log_channel or not request_channel:
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        self.req.request_channel_id = request_channel.id
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)

        ref_text = self.req.references[0] if self.req.references else ""
        await self.cog._add_reminder(
            guild_id=guild.id,
            user_id=interaction.user.id,
            text=f"Your **{self.req.training}** class has finished." + (f" Reference: {ref_text}" if ref_text else ""),
            when=end_at.astimezone(timezone.utc),
            fallback_channel_id=self.req.request_channel_id,
        )

        user = interaction.user
        dm_text = (
            f"✅ Your reminder has been **STARTED**.\n"
            f"**{self.req.discipline} → {self.req.training}** ({self.req.days} days)\n"
            f"End time: {fmt_dt(end_at)}"
        )
        if ref_text:
            dm_text += f"\nReference: {ref_text}"
        dm_text += "\n\nYou will be notified when the training finishes."
        
        dm_sent = False
        try:
            await user.send(dm_text)
            dm_sent = True
        except discord.Forbidden:
            pass

        emb = discord.Embed(
            title="Reminder created",
            color=discord.Color.blue(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="User", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Training", value=f"{self.req.discipline} → {self.req.training} ({self.req.days}d)", inline=False)
        if ref_text:
            emb.add_field(name="Reference", value=ref_text, inline=False)
        emb.add_field(name="End time", value=fmt_dt(end_at), inline=False)
        await log_channel.send(embed=emb)

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True

        confirm_msg = "✅ Reminder started!" + ("" if dm_sent else " (Check your DMs)")
        await safe_update(interaction, content=confirm_msg, embed=None, view=self)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, custom_id="tm:cancel_reminder")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        await safe_update(interaction, content="❌ Reminder cancelled.", embed=None, view=self)

# Reminder buttons
class ReminderOn(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Notify at end: On", style=discord.ButtonStyle.success, custom_id="tm:rem_on")

    async def callback(self, interaction: discord.Interaction):
        parent: SummaryView = self.view  # type: ignore
        parent.req.want_reminder = True
        await parent.send_or_update(interaction)

class ReminderOff(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Notify at end: Off", style=discord.ButtonStyle.secondary, custom_id="tm:rem_off")

    async def callback(self, interaction: discord.Interaction):
        parent: SummaryView = self.view  # type: ignore
        parent.req.want_reminder = False
        await parent.send_or_update(interaction)

class SubmitButton(discord.ui.Button):
    def __init__(self, parent_view: "SummaryView"):
        super().__init__(label="Submit Request", style=discord.ButtonStyle.success, custom_id="tm:submit")
        self.parent_view = parent_view

    async def _mark_processing(self, interaction: discord.Interaction) -> None:
        for child in self.parent_view.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        self.label = "Processing..."
        self.style = discord.ButtonStyle.secondary

        embed = discord.Embed(
            title="Training request is being processed",
            description=(
                "The bot is checking MissionChief and trying to open the training automatically.\n"
                "Do not submit this request again."
            ),
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        req = self.parent_view.req
        embed.add_field(name="Training", value=f"{req.discipline} → {req.training}", inline=False)
        embed.add_field(name="Classes", value=str(req.num_classes), inline=True)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day/trainee"
        embed.add_field(name="Fee", value=fee_txt, inline=True)

        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(
                    content="Processing your training request...",
                    embed=embed,
                    view=self.parent_view,
                )
                return
        except Exception as exc:
            log.debug("Could not mark training request as processing via response edit: %s", exc)

        try:
            if getattr(interaction, "message", None) is not None:
                await interaction.message.edit(
                    content="Processing your training request...",
                    embed=embed,
                    view=self.parent_view,
                )
        except Exception as exc:
            log.debug("Could not mark training request as processing via message edit: %s", exc)

    async def callback(self, interaction: discord.Interaction):
        cog: TrainingManager = self.parent_view.cog  # type: ignore
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        if getattr(self.parent_view, "processing", False):
            await interaction.response.send_message(
                "This training request is already being processed. Do not submit it again.",
                ephemeral=True,
            )
            return
        self.parent_view.processing = True

        conf = await cog.config.guild(guild).all()
        admin_channel_id = conf.get("admin_channel_id")
        log_channel_id = conf.get("log_channel_id")
        request_channel_id = conf.get("request_channel_id")

        if not admin_channel_id or not log_channel_id or not request_channel_id:
            self.parent_view.processing = False
            await interaction.response.send_message(
                "Admin/Log/Request channels are not configured yet. Ask an admin to use [p]tmset.",
                ephemeral=True,
            )
            return

        admin_channel = guild.get_channel(admin_channel_id)
        log_channel = guild.get_channel(log_channel_id)
        request_channel = guild.get_channel(request_channel_id)

        if not admin_channel or not log_channel or not request_channel:
            self.parent_view.processing = False
            await interaction.response.send_message("One or more configured channels could not be found.", ephemeral=True)
            return

        req = self.parent_view.req
        req.request_channel_id = request_channel.id
        user = interaction.user

        await self._mark_processing(interaction)

        auto_result = await cog._try_auto_open_training(guild, user, req)
        if auto_result.success:
            await cog._send_auto_open_success(
                guild=guild,
                user=user,
                req=req,
                result=auto_result,
                log_channel=log_channel,
            )
            await cog._notify_auto_open_requester(user, req, auto_result, request_channel)
            await interaction.followup.send(
                "Training opened automatically. You'll be notified by DM.",
                ephemeral=True,
            )
            return

        fallback_reason = auto_result.reason or "Unknown automatic opening failure"
        end_at = datetime.now(AMS) + timedelta(days=req.days)
        emb = discord.Embed(
            title="New training request" + (" - MULTIPLE CLASSES" if req.num_classes > 1 else ""),
            color=discord.Color.orange() if req.num_classes > 1 else discord.Color.yellow(),
            timestamp=datetime.now(timezone.utc),
        )
        emb.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        emb.add_field(name="Discipline", value=req.discipline, inline=True)
        emb.add_field(name="Training", value=req.training, inline=True)
        emb.add_field(name="Duration", value=f"{req.days} days", inline=True)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day/trainee"
        emb.add_field(name="Fee", value=fee_txt, inline=True)
        
        # Classes field with emphasis if multiple
        class_txt = f"**{req.num_classes}**" if req.num_classes > 1 else "1"
        emb.add_field(name="Classes", value=class_txt, inline=True)
        
        # References handling
        if req.references and any(req.references):
            if len(req.references) == 1 or all(r == req.references[0] for r in req.references if r):
                ref_display = req.references[0] if req.references[0] else "—"
            else:
                ref_lines = []
                for i, ref in enumerate(req.references, 1):
                    ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                ref_display = "\n".join(ref_lines)
            emb.add_field(name="Reference(s)", value=ref_display, inline=False)
        else:
            emb.add_field(name="Reference", value="—", inline=False)
        
        emb.add_field(
            name="Automatic opening",
            value=f"Manual admin start required.\nReason: {fallback_reason}",
            inline=False,
        )
        emb.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        
        if req.num_classes > 1:
            emb.set_footer(text=f"⚠️ This request is for {req.num_classes} classes! Use the buttons below to approve or reject ALL classes.")
        else:
            emb.set_footer(text="Use the buttons below to approve or reject.")

        view = AdminDecisionView(cog, requester_id=user.id, req=req)
        await admin_channel.send(embed=emb, view=view)

        queue_emb = discord.Embed(
            title="Request submitted",
            description=f"By {user.mention} in {request_channel.mention}.",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        training_text = f"{req.discipline} → {req.training} ({req.days}d)"
        if req.num_classes > 1:
            training_text += f" × **{req.num_classes} classes**"
        queue_emb.add_field(name="Training", value=training_text, inline=False)
        
        if req.references and any(req.references):
            if len(req.references) == 1 or all(r == req.references[0] for r in req.references if r):
                queue_emb.add_field(name="Reference", value=req.references[0] if req.references[0] else "—", inline=False)
            else:
                ref_lines = []
                for i, ref in enumerate(req.references, 1):
                    ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                queue_emb.add_field(name="References", value="\n".join(ref_lines), inline=False)
        queue_emb.add_field(name="Automatic opening", value=f"Fallback to admin: {fallback_reason}", inline=False)
        await log_channel.send(embed=queue_emb)
        await cog._notify_auto_open_fallback_requester(user, req, fallback_reason, request_channel)

        if interaction.response.is_done():
            await interaction.followup.send(
                f"Automatic opening was not possible, so this was sent to Admin.\nReason: {fallback_reason}",
                ephemeral=True,
            )
        else:
            await safe_update(
                interaction,
                content=f"Automatic opening was not possible, so this was sent to Admin.\nReason: {fallback_reason}",
                embed=None,
                view=None,
            )

class SummaryView(discord.ui.View):
    def __init__(
        self,
        cog: "TrainingManager",
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        fee: int,
        num_classes: int,
        references: List[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=fee,
            num_classes=num_classes,
            references=references,
            want_reminder=False,
            request_channel_id=0,
        )
        self.processing = False
        self.add_item(ReminderOff())
        self.add_item(ReminderOn())
        self.add_item(SubmitButton(self))

    async def send_or_update(self, interaction: discord.Interaction):
        user = interaction.user
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        embed = discord.Embed(
            title="Training request - Summary",
            color=discord.Color.blurple(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Discipline", value=self.req.discipline, inline=True)
        embed.add_field(name="Training", value=self.req.training, inline=True)
        embed.add_field(name="Duration", value=f"{self.req.days} days", inline=True)
        fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} credits/day/trainee"
        embed.add_field(name="Fee", value=fee_txt, inline=True)
        embed.add_field(name="Classes", value=str(self.req.num_classes), inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        
        # References handling
        if self.req.references and any(self.req.references):
            if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                ref_display = self.req.references[0] if self.req.references[0] else "—"
            else:
                ref_lines = []
                for i, ref in enumerate(self.req.references, 1):
                    ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                ref_display = "\n".join(ref_lines)
            embed.add_field(name="Reference(s)", value=ref_display, inline=False)
        else:
            embed.add_field(name="Reference", value="—", inline=False)
        
        embed.add_field(name="Notify when class finishes", value="Yes" if self.req.want_reminder else "No", inline=True)

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "tm:rem_on":
                    child.style = discord.ButtonStyle.success if self.req.want_reminder else discord.ButtonStyle.secondary
                if child.custom_id == "tm:rem_off":
                    child.style = discord.ButtonStyle.success if not self.req.want_reminder else discord.ButtonStyle.secondary

        await safe_update(interaction, content="Review and submit to Admin.", embed=embed, view=self)


class DeveloperAutoOpenSubmitButton(discord.ui.Button):
    def __init__(self, parent_view: "DeveloperAutoOpenSummaryView"):
        super().__init__(
            label="Open Automatically",
            style=discord.ButtonStyle.danger,
            custom_id="tmdev:auto_open_submit",
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        cog: TrainingManager = self.parent_view.cog  # type: ignore
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("This only works inside a server.", ephemeral=True)
            return

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        req = self.parent_view.req
        req.request_channel_id = interaction.channel.id if interaction.channel else 0
        result = await cog._try_auto_open_training(guild, interaction.user, req)
        conf = await cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf.get("log_channel_id")) if conf.get("log_channel_id") else None
        request_channel = guild.get_channel(req.request_channel_id) if req.request_channel_id else None

        if result.success:
            if log_channel:
                await cog._send_auto_open_success(
                    guild=guild,
                    user=interaction.user,
                    req=req,
                    result=result,
                    log_channel=log_channel,
                )
            await cog._notify_auto_open_requester(interaction.user, req, result, request_channel)
            await interaction.followup.send(
                f"Developer test succeeded: opened **{req.training}** in academy `{result.academy_id}`.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Developer test did not open a class: {result.reason}",
            ephemeral=True,
        )


class DeveloperAutoOpenSummaryView(discord.ui.View):
    def __init__(
        self,
        cog: "TrainingManager",
        user_id: int,
        discipline: str,
        training: str,
        days: int,
        fee: int,
        num_classes: int,
        references: List[str],
    ):
        super().__init__(timeout=600)
        self.cog = cog
        self.req = TrainingRequest(
            user_id=user_id,
            discipline=discipline,
            training=training,
            days=days,
            fee_per_day=fee,
            num_classes=num_classes,
            references=references,
            want_reminder=False,
            request_channel_id=0,
        )
        self.add_item(ReminderOff())
        self.add_item(ReminderOn())
        self.add_item(DeveloperAutoOpenSubmitButton(self))

    async def send_or_update(self, interaction: discord.Interaction):
        user = interaction.user
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        embed = discord.Embed(
            title="Developer training auto-open - Summary",
            color=discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Discipline", value=self.req.discipline, inline=True)
        embed.add_field(name="Training", value=self.req.training, inline=True)
        embed.add_field(name="Duration", value=f"{self.req.days} days", inline=True)
        fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} credits/day/trainee"
        embed.add_field(name="Fee", value=fee_txt, inline=True)
        embed.add_field(name="Classes", value=str(self.req.num_classes), inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        if self.req.references and any(self.req.references):
            embed.add_field(name="Reference(s)", value="\n".join(ref or "-" for ref in self.req.references), inline=False)
        else:
            embed.add_field(name="Reference", value="-", inline=False)
        embed.add_field(name="Notify when class finishes", value="Yes" if self.req.want_reminder else "No", inline=True)
        embed.set_footer(text="Developer test only. Normal member requests still use admin approval.")

        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.custom_id == "tm:rem_on":
                    child.style = discord.ButtonStyle.success if self.req.want_reminder else discord.ButtonStyle.secondary
                if child.custom_id == "tm:rem_off":
                    child.style = discord.ButtonStyle.success if not self.req.want_reminder else discord.ButtonStyle.secondary

        await safe_update(interaction, content="Review and open automatically.", embed=embed, view=self)

# ---------- Admin decision ----------


class DeveloperTrainingPanelView(discord.ui.View):
    def __init__(self, cog: "TrainingManager"):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Open Test Training",
        style=discord.ButtonStyle.danger,
        custom_id="tmdev:auto_open_training",
    )
    async def open_test_training(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "Developer auto-open test. First, pick a discipline.",
            view=DisciplineView(self.cog, reminder_only=False, auto_open=True),
            ephemeral=True,
        )


class AdminDecisionView(discord.ui.View):
    def __init__(self, cog: "TrainingManager", requester_id: int, req: TrainingRequest):
        super().__init__(timeout=None)
        self.cog = cog
        self.requester_id = requester_id
        self.req = req

    async def _is_admin(self, interaction: discord.Interaction) -> bool:
        guild = interaction.guild
        if guild is None or not isinstance(interaction.user, discord.Member):
            return False
        role_id = await self.cog.config.guild(guild).admin_role_id()
        if role_id is None:
            return False
        role = guild.get_role(role_id)
        return role in interaction.user.roles if role else False

    @discord.ui.button(label="Start Education", style=discord.ButtonStyle.success, custom_id="tm:approve")
    async def approve(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        
        # Show modal for optional admin message
        await interaction.response.send_modal(ApproveModal(self.cog, self.requester_id, self.req, admin_msg=interaction.message, admin_user=interaction.user))

    @discord.ui.button(label="Reject", style=discord.ButtonStyle.danger, custom_id="tm:reject")
    async def reject(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._is_admin(interaction):
            await interaction.response.send_message("You don't have permission to do this.", ephemeral=True)
            return
        await interaction.response.send_modal(RejectModal(self.cog, self.requester_id, self.req, admin_msg=interaction.message, admin_user=interaction.user))

class ApproveModal(discord.ui.Modal, title="Approve training request"):
    message = discord.ui.TextInput(
        label="Optional message to requester",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=False,
        placeholder="Add a message that will be sent via DM (optional)",
    )

    def __init__(self, cog: "TrainingManager", requester_id: int, req: TrainingRequest, admin_msg: discord.Message, admin_user: discord.User):
        super().__init__()
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Internal error: no guild.", ephemeral=True)
            return
            
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        user = guild.get_member(self.requester_id) if guild else None
        end_at = datetime.now(AMS) + timedelta(days=self.req.days)
        
        class_text = f"{self.req.num_classes} classes" if self.req.num_classes > 1 else "class"
        ok_text = (
            f"Your training request has been **APPROVED**.\n"
            f"**{self.req.discipline} → {self.req.training}** ({class_text}) for "
            f"{'Free' if self.req.fee_per_day == 0 else str(self.req.fee_per_day) + ' credits/day/trainee'}.\n"
            f"End time: {fmt_dt(end_at)}."
        )
        
        if self.req.references and any(self.req.references):
            if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                ok_text += f"\nReference: {self.req.references[0]}"
            else:
                ok_text += "\nReferences:\n"
                for i, ref in enumerate(self.req.references, 1):
                    ok_text += f"  Class {i}: {ref if ref else '—'}\n"
        
        admin_message = str(self.message).strip()
        if admin_message:
            ok_text += f"\n\n**Message from admin:**\n{admin_message}"

        if user:
            try:
                await user.send(ok_text)
            except discord.Forbidden:
                pass

        if self.req.want_reminder:
            ref_text = ""
            if self.req.references and any(self.req.references):
                if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                    ref_text = f" Reference: {self.req.references[0]}"
                else:
                    ref_text = " (Multiple references)"
            
            await self.cog._add_reminder(
                guild_id=guild.id,
                user_id=self.requester_id,
                text=f"Your **{self.req.training}** class has finished.{ref_text}",
                when=end_at.astimezone(timezone.utc),
                fallback_channel_id=self.req.request_channel_id,
            )

        if log_channel:
            emb = discord.Embed(
                title="Training approved" + (f" - {self.req.num_classes} CLASSES" if self.req.num_classes > 1 else ""),
                color=discord.Color.green(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            
            training_text = f"{self.req.discipline} → {self.req.training} ({self.req.days}d)"
            if self.req.num_classes > 1:
                training_text += f" × **{self.req.num_classes} classes**"
            emb.add_field(name="Training", value=training_text, inline=False)
            
            fee_txt = "Free" if self.req.fee_per_day == 0 else f"{self.req.fee_per_day} c/day"
            emb.add_field(name="Fee", value=fee_txt, inline=True)
            emb.add_field(name="Classes", value=str(self.req.num_classes), inline=True)
            
            if self.req.references and any(self.req.references):
                if len(self.req.references) == 1 or all(r == self.req.references[0] for r in self.req.references if r):
                    emb.add_field(name="Reference", value=self.req.references[0] if self.req.references[0] else "—", inline=False)
                else:
                    ref_lines = []
                    for i, ref in enumerate(self.req.references, 1):
                        ref_lines.append(f"Class {i}: {ref if ref else '—'}")
                    emb.add_field(name="References", value="\n".join(ref_lines), inline=False)
            
            emb.add_field(name="Reminder", value="Yes" if self.req.want_reminder else "No", inline=True)
            emb.add_field(name="End time", value=fmt_dt(end_at), inline=False)
            emb.add_field(name="Approved by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            
            if admin_message:
                emb.add_field(name="Admin message", value=admin_message, inline=False)
            
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Request approved and processed.", ephemeral=True)

class RejectModal(discord.ui.Modal, title="Rejection reason"):
    reason = discord.ui.TextInput(
        label="Reason",
        style=discord.TextStyle.paragraph,
        max_length=400,
        required=True,
        placeholder="Briefly explain why this request is rejected.",
    )

    def __init__(self, cog: "TrainingManager", requester_id: int, req: TrainingRequest, admin_msg: discord.Message, admin_user: discord.User):
        super().__init__()
        self.cog = cog
        self.requester_id = requester_id
        self.req = req
        self.admin_msg = admin_msg
        self.admin_user = admin_user

    async def on_submit(self, interaction: discord.Interaction):
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message("Internal error: no guild.", ephemeral=True)
            return
        conf = await self.cog.config.guild(guild).all()
        log_channel = guild.get_channel(conf["log_channel_id"]) if conf.get("log_channel_id") else None

        user = guild.get_member(self.requester_id)
        
        class_text = f"{self.req.num_classes} classes" if self.req.num_classes > 1 else "class"
        text = (
            f"Your training request for **{self.req.discipline} → {self.req.training}** ({class_text}) has been **REJECTED**.\n"
            f"Reason: {self.reason}"
        )
        if user:
            try:
                await user.send(text)
            except discord.Forbidden:
                ch = guild.get_channel(self.req.request_channel_id)
                if ch:
                    await ch.send(f"{user.mention} {text}")

        if log_channel:
            emb = discord.Embed(
                title="Training rejected" + (f" - {self.req.num_classes} CLASSES" if self.req.num_classes > 1 else ""),
                color=discord.Color.red(),
                timestamp=datetime.now(timezone.utc),
            )
            requester = f"<@{self.requester_id}>"
            emb.add_field(name="Requester", value=requester, inline=False)
            
            training_text = f"{self.req.discipline} → {self.req.training}"
            if self.req.num_classes > 1:
                training_text += f" × **{self.req.num_classes} classes**"
            emb.add_field(name="Training", value=training_text, inline=False)
            
            emb.add_field(name="Reason", value=str(self.reason), inline=False)
            emb.add_field(name="Rejected by", value=f"{self.admin_user.mention} ({self.admin_user.id})", inline=False)
            await log_channel.send(embed=emb)

        try:
            await self.admin_msg.delete()
        except Exception:
            pass

        await interaction.response.send_message("Rejection forwarded and logged.", ephemeral=True)

# ---------- Cog ----------

class TrainingManager(commands.Cog):
    """Training requests with approvals and end-of-class reminders."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xBEEFCAFE, force_registration=True)
        default_guild = {
            "request_channel_id": MEMBER_PANEL_CHANNEL_ID,
            "admin_channel_id": None,
            "log_channel_id": None,
            "admin_role_id": None,
            "reminders": [],
            "button_message": None,
            "panel_message_id": None,
            "panel_last_auto_post_at": None,
            "developer_panel_channel_id": DEVELOPER_PANEL_CHANNEL_ID,
            "developer_panel_message_id": None,
            "availability_channel_id": MEMBER_PANEL_CHANNEL_ID,
            "availability_message_id": None,
            "availability_enabled": False,
            "board_poll_enabled": True,
            "board_thread_id": BOARD_THREAD_ID,
            "board_last_seen_post_id": None,
            "board_guide_enabled": True,
            "board_guide_thread_id": None,
            "board_guide_post_id": None,
            "board_guide_post_ids": {},
            "board_guide_content_hash": None,
            "board_guide_content_hashes": {},
        }
        self.config.register_guild(**default_guild)

        self.bot.add_view(StartView(self))
        self.bot.add_view(DeveloperTrainingPanelView(self))

        self._reminder_task = self.bot.loop.create_task(self._reminder_loop())
        self._panel_task = self.bot.loop.create_task(self._ensure_member_panels())
        self._developer_panel_task = self.bot.loop.create_task(self._ensure_developer_panels())
        self._availability_task = self.bot.loop.create_task(self._availability_loop())
        self._board_poll_task = self.bot.loop.create_task(self._board_poll_loop())
        self._board_guide_task = self.bot.loop.create_task(self._board_guide_loop())

    def cog_unload(self):
        if self._reminder_task:
            self._reminder_task.cancel()
        if self._panel_task:
            self._panel_task.cancel()
        if self._developer_panel_task:
            self._developer_panel_task.cancel()
        if self._availability_task:
            self._availability_task.cancel()
        if self._board_poll_task:
            self._board_poll_task.cancel()
        if self._board_guide_task:
            self._board_guide_task.cancel()

    @asynccontextmanager
    async def _bot_status(self, detail: str, *, priority: int = 70):
        bot = getattr(self, "bot", None)
        botstatus = bot.get_cog("BotStatus") if bot else None
        if botstatus and hasattr(botstatus, "track_activity"):
            async with botstatus.track_activity("TrainingManager", detail, priority=priority):
                yield
        else:
            yield

    # --------------- Reminder machinery ---------------

    async def _add_reminder(self, guild_id: int, user_id: int, text: str, when, fallback_channel_id: int):
        """Persist a reminder and let the loop pick it up."""
        if isinstance(when, datetime):
            when_ts = int(when.replace(tzinfo=timezone.utc).timestamp())
        else:
            when_ts = int(when)
        async with self.config.guild_from_id(guild_id).reminders() as rems:
            rems.append({
                "user_id": int(user_id),
                "text": str(text),
                "when_ts": int(when_ts),
                "fallback_channel_id": int(fallback_channel_id),
            })

    async def _reminder_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    conf = await self.config.guild(guild).all()
                    rems = conf.get("reminders", [])
                    now_ts = int(datetime.now(timezone.utc).timestamp())
                    due = [r for r in rems if r.get("when_ts", 10**18) <= now_ts]
                    if not due:
                        continue
                    keep: List[dict] = []
                    async with self._bot_status(
                        f"sending {len(due)} training reminders in {guild.name}"
                    ):
                        for r in rems:
                            if r in due:
                                await self._deliver_reminder(guild, r)
                            else:
                                keep.append(r)
                    await self.config.guild(guild).reminders.set(keep)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Reminder loop error: %r", e)
            await asyncio.sleep(30)

    async def _deliver_reminder(self, guild: discord.Guild, r: dict):
        user = guild.get_member(int(r["user_id"]))
        text = str(r["text"])
        sent = False
        if user:
            try:
                await user.send(text)
                sent = True
            except discord.Forbidden:
                sent = False
        if not sent:
            ch = guild.get_channel(int(r["fallback_channel_id"]))
            if ch:
                try:
                    await ch.send(f"<@{user.id}> {text}" if user else text)
                except Exception:
                    pass

    # --------------- Automatic MissionChief opening ---------------

    async def _resolve_verified_requester(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
    ) -> Tuple[bool, Optional[str], Optional[str], str]:
        membersync = self.bot.get_cog("MemberSync")
        if not membersync:
            return False, None, None, "MemberSync is not loaded"

        link = None
        get_link = getattr(membersync, "get_link_for_discord", None)
        if get_link:
            link = await get_link(user.id)
        if link:
            mc_user_id = link.get("mc_user_id")
            mc_username = link.get("mc_username") or link.get("mc_name") or link.get("name")
            return True, str(mc_user_id) if mc_user_id else None, mc_username, "approved MemberSync link"

        role_id = None
        try:
            role_id = await membersync.config.verified_role_id()
        except Exception as exc:
            log.debug("Could not read MemberSync verified role: %s", exc)

        member = guild.get_member(user.id)
        if role_id and member:
            role = guild.get_role(int(role_id))
            if role and role in getattr(member, "roles", []):
                return True, None, None, "verified Discord role"

        return False, None, None, "requester is not verified"

    async def _get_member_snapshot(self, mc_user_id: Optional[str]) -> Optional[dict]:
        if not mc_user_id:
            return None
        members_scraper = self.bot.get_cog("MembersScraper")
        if not members_scraper:
            return None
        get_snapshot = getattr(members_scraper, "get_member_snapshot", None)
        if not get_snapshot:
            return None
        try:
            return await get_snapshot(str(mc_user_id))
        except Exception as exc:
            log.warning("Could not fetch member snapshot for %s: %s", mc_user_id, exc)
            return None

    async def _resolve_mc_username(self, mc_user_id: Optional[str], current_name: Optional[str]) -> Optional[str]:
        if _is_known_mc_username(current_name):
            return current_name
        snapshot = await self._get_member_snapshot(mc_user_id)
        if not snapshot:
            return await self._fetch_mc_username_from_profile(mc_user_id)
        for key in ("name", "username", "mc_username", "mc_name"):
            value = snapshot.get(key)
            if _is_known_mc_username(str(value) if value is not None else None):
                return str(value)
        return await self._fetch_mc_username_from_profile(mc_user_id)

    async def _fetch_mc_username_from_profile(self, mc_user_id: Optional[str]) -> Optional[str]:
        if not mc_user_id:
            return None
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return None
        try:
            session = await cookie_manager.get_session()
        except Exception as exc:
            log.info("Could not get MissionChief session to resolve profile %s: %s", mc_user_id, exc)
            return None

        for path in (f"/profile/{mc_user_id}", f"/users/{mc_user_id}"):
            url = f"https://www.missionchief.com{path}"
            try:
                async with session.get(url, allow_redirects=True) as response:
                    status = getattr(response, "status", None)
                    html = await response.text()
            except Exception as exc:
                log.info("Could not fetch MissionChief profile %s: %s", url, exc)
                continue
            if status is not None and int(status) >= 400:
                continue
            username = parse_profile_username(html)
            if username:
                return username
        return None

    async def _get_latest_contribution_rate(self, mc_user_id: Optional[str]) -> Optional[float]:
        snapshot = await self._get_member_snapshot(mc_user_id)
        if not snapshot:
            return None
        value = snapshot.get("contribution_rate")
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _find_academy_course(self, page: AcademyPage, training_name: str) -> Optional[AcademyCourse]:
        wanted = _normalize_training_name(training_name)
        for course in page.courses:
            if course.normalized_label == wanted:
                return course
        return None

    async def _fetch_available_academies(self, session, discipline: str) -> Tuple[List[AvailableAcademy], Optional[int]]:
        academies, status = await self._fetch_all_available_academies(session)
        filtered = [
            academy
            for academy in academies
            if academy.discipline == discipline and academy.has_start_button
        ]
        preferred_id = AUTO_ACADEMY_BUILDINGS.get(discipline)
        if preferred_id:
            filtered.sort(key=lambda academy: academy.building_id != preferred_id)
        return filtered, status

    async def _fetch_all_available_academies(self, session) -> Tuple[List[AvailableAcademy], Optional[int]]:
        next_url = f"https://www.missionchief.com{AUTO_BUILDING_LIST_PATH}"
        academies: List[AvailableAcademy] = []
        seen_urls = set()
        last_status: Optional[int] = None

        for _page_number in range(AUTO_ACADEMY_LIST_MAX_PAGES):
            if next_url in seen_urls:
                break
            seen_urls.add(next_url)

            async with session.get(next_url, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
            last_status = status

            if status is not None and int(status) >= 400:
                return academies, status

            page_academies, next_page_path = parse_available_academies_page(html)
            academies.extend(page_academies)
            if not next_page_path:
                break
            next_url = urljoin(next_url, next_page_path)

        return academies, last_status

    async def _collect_training_availability(self) -> Tuple[Dict[str, DisciplineAvailability], Optional[str]]:
        availability = {
            discipline: DisciplineAvailability(discipline=discipline)
            for discipline in ("Fire", "Police", "EMS", "Coastal")
        }
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return availability, "CookieManager is not loaded"

        try:
            session = await cookie_manager.get_session()
            academies, status = await self._fetch_all_available_academies(session)
        except Exception as exc:
            log.warning("Could not fetch training academy list: %s", exc)
            return availability, f"Could not fetch academy list: {exc}"

        if status is not None and int(status) >= 400:
            return availability, f"Academy list returned HTTP {status}"

        for academy in academies:
            if academy.discipline not in availability:
                continue
            stats = availability[academy.discipline]
            stats.academies_checked += 1
            if not academy.has_start_button:
                continue
            try:
                async with session.get(f"https://www.missionchief.com/buildings/{academy.building_id}", allow_redirects=True) as response:
                    academy_status = getattr(response, "status", None)
                    html = await response.text()
                if academy_status is not None and int(academy_status) >= 400:
                    stats.errors += 1
                    continue
                page = parse_academy_page(html)
            except Exception as exc:
                log.info("Could not inspect academy %s: %s", academy.building_id, exc)
                stats.errors += 1
                continue

            if page.available_rooms > 0:
                stats.academies_available += 1
                stats.available_classrooms += page.available_rooms

        return availability, None

    async def _try_auto_open_training(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
        req: TrainingRequest,
    ) -> AutoTrainingResult:
        fallback_academy_id = AUTO_ACADEMY_BUILDINGS.get(req.discipline)

        if req.num_classes < 1 or req.num_classes > AUTO_MAX_CLASSES:
            return AutoTrainingResult(False, f"Requested class count must be between 1 and {AUTO_MAX_CLASSES}")

        verified, mc_user_id, mc_username, verified_source = await self._resolve_verified_requester(guild, user)
        if not verified:
            return AutoTrainingResult(False, verified_source, academy_id=fallback_academy_id)
        mc_username = await self._resolve_mc_username(mc_user_id, mc_username)

        contribution_rate = await self._get_latest_contribution_rate(mc_user_id)
        if contribution_rate is not None and contribution_rate < AUTO_MIN_CONTRIBUTION_RATE:
            return AutoTrainingResult(
                False,
                f"Latest contribution rate is {contribution_rate:.1f}%, below {AUTO_MIN_CONTRIBUTION_RATE:.1f}%",
                academy_id=fallback_academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
            )

        return await self._open_training_course(
            req,
            mc_user_id=mc_user_id,
            mc_username=mc_username,
            contribution_rate=contribution_rate,
        )

    async def _try_auto_open_board_training(
        self,
        post: BoardTrainingPost,
        req: TrainingRequest,
    ) -> AutoTrainingResult:
        fallback_academy_id = AUTO_ACADEMY_BUILDINGS.get(req.discipline)

        if req.num_classes < 1 or req.num_classes > AUTO_MAX_CLASSES:
            return AutoTrainingResult(False, f"Requested class count must be between 1 and {AUTO_MAX_CLASSES}")

        mc_user_id = str(post.author_id) if post.author_id else None
        mc_username = post.author_name
        contribution_rate = await self._get_latest_contribution_rate(mc_user_id)
        if contribution_rate is not None and contribution_rate < AUTO_MIN_CONTRIBUTION_RATE:
            return AutoTrainingResult(
                False,
                f"Latest contribution rate is {contribution_rate:.1f}%, below {AUTO_MIN_CONTRIBUTION_RATE:.1f}%",
                academy_id=fallback_academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
            )

        return await self._open_training_course(
            req,
            mc_user_id=mc_user_id,
            mc_username=mc_username,
            contribution_rate=contribution_rate,
        )

    async def _open_training_course(
        self,
        req: TrainingRequest,
        *,
        mc_user_id: Optional[str],
        mc_username: Optional[str],
        contribution_rate: Optional[float],
    ) -> AutoTrainingResult:
        fallback_academy_id = AUTO_ACADEMY_BUILDINGS.get(req.discipline)
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            return AutoTrainingResult(
                False,
                "CookieManager is not loaded",
                academy_id=fallback_academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
            )

        session = await cookie_manager.get_session()
        academies, list_status = await self._fetch_available_academies(session, req.discipline)
        if not academies and fallback_academy_id and list_status is not None and int(list_status) >= 400:
            academies = [
                AvailableAcademy(
                    building_id=fallback_academy_id,
                    name=f"Configured {req.discipline} academy",
                    discipline=req.discipline,
                    has_start_button=True,
                )
            ]
        if not academies:
            return AutoTrainingResult(
                False,
                f"No available {req.discipline} academies found on the alliance building list",
                academy_id=fallback_academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                status=list_status,
            )

        last_reason = "No suitable academy found"
        page = None
        course = None
        academy_id = None
        status = None
        for academy in academies:
            academy_id = academy.building_id
            building_url = f"https://www.missionchief.com/buildings/{academy_id}"
            async with session.get(building_url, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
            if status is not None and int(status) >= 400:
                last_reason = f"Academy {academy_id} returned HTTP {status}"
                continue

            candidate_page = parse_academy_page(html)
            if not candidate_page.action or not candidate_page.authenticity_token:
                last_reason = f"Academy {academy_id} has no education form"
                continue
            if candidate_page.available_rooms < req.num_classes:
                last_reason = (
                    f"Academy {academy_id} has only {candidate_page.available_rooms} classroom(s), "
                    f"request needs {req.num_classes}"
                )
                continue
            if req.fee_per_day not in candidate_page.costs:
                last_reason = f"Fee {req.fee_per_day} is not available in academy {academy_id}"
                continue
            candidate_course = self._find_academy_course(candidate_page, req.training)
            if not candidate_course:
                last_reason = f"Training `{req.training}` was not found in academy {academy_id}"
                continue

            page = candidate_page
            course = candidate_course
            break

        if not page or not course or not academy_id:
            return AutoTrainingResult(
                False,
                last_reason,
                academy_id=academy_id or fallback_academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                status=status,
            )

        post_url = f"https://www.missionchief.com{page.action}" if page.action.startswith("/") else page.action
        payload = {
            "utf8": "\u2713",
            "authenticity_token": page.authenticity_token,
            "building_rooms_use": str(req.num_classes),
            "education_select": course.value,
            "alliance[duration]": str(AUTO_ALLIANCE_DURATION_SECONDS),
            "alliance[cost]": str(req.fee_per_day),
            "commit": "Educate",
        }
        async with session.post(post_url, data=payload, allow_redirects=True) as response:
            post_status = getattr(response, "status", None)
            await response.text()

        if post_status is None or int(post_status) >= 400:
            return AutoTrainingResult(
                False,
                f"MissionChief education POST failed with HTTP {post_status}",
                academy_id=academy_id,
                mc_user_id=mc_user_id,
                mc_username=mc_username,
                contribution_rate=contribution_rate,
                course_value=course.value,
                status=post_status,
            )

        return AutoTrainingResult(
            True,
            "Training opened automatically",
            academy_id=academy_id,
            mc_user_id=mc_user_id,
            mc_username=mc_username,
            contribution_rate=contribution_rate,
            course_value=course.value,
            classes_opened=req.num_classes,
            status=post_status,
        )

    async def _board_poll_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    conf = await self.config.guild(guild).all()
                    if not conf.get("board_poll_enabled"):
                        continue
                    async with self._bot_status(f"checking training board in {guild.name}", priority=55):
                        await self._poll_training_board_for_guild(guild, conf)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("Training board poll loop error: %s", exc)
            await asyncio.sleep(BOARD_POLL_SECONDS)

    async def _poll_training_board_for_guild(self, guild: discord.Guild, conf: dict) -> None:
        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            log.info("Training board poll skipped: CookieManager is not loaded")
            return

        thread_id = int(conf.get("board_thread_id") or BOARD_THREAD_ID)
        session = await cookie_manager.get_session()
        page, status = await self._fetch_training_board_latest_page(session, thread_id)
        if status is not None and int(status) >= 400:
            log.warning("Training board poll failed: thread %s returned HTTP %s", thread_id, status)
            return

        if not page.posts:
            return

        latest_post_id = max(post.post_id for post in page.posts)
        last_seen_raw = conf.get("board_last_seen_post_id")
        if not last_seen_raw:
            await self.config.guild(guild).board_last_seen_post_id.set(latest_post_id)
            log.info("Training board baseline set to post %s for guild %s", latest_post_id, guild.id)
            return

        try:
            last_seen = int(last_seen_raw)
        except (TypeError, ValueError):
            last_seen = latest_post_id
            await self.config.guild(guild).board_last_seen_post_id.set(latest_post_id)
            return

        new_posts = [
            post
            for post in sorted(page.posts, key=lambda item: item.post_id)
            if post.post_id > last_seen
            and post.author_id != page.current_user_id
            and not self._is_board_guide_post(post)
        ]
        for post in new_posts:
            try:
                await self._handle_training_board_post(guild, session, thread_id, page, post)
            except Exception as exc:
                log.exception("Training board post %s processing failed: %s", post.post_id, exc)

        if latest_post_id > last_seen:
            await self.config.guild(guild).board_last_seen_post_id.set(latest_post_id)

    async def _fetch_training_board_latest_page(self, session, thread_id: int) -> Tuple[BoardPage, Optional[int]]:
        base_url = f"https://www.missionchief.com/alliance_threads/{thread_id}"
        async with session.get(base_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        page = parse_training_board_page(html)
        if status is not None and int(status) >= 400:
            return page, status

        if page.last_page > 1:
            last_url = f"{base_url}?page={page.last_page}"
            async with session.get(last_url, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
            page = parse_training_board_page(html)
        return page, status

    async def _handle_training_board_post(
        self,
        guild: discord.Guild,
        session,
        thread_id: int,
        page: BoardPage,
        post: BoardTrainingPost,
    ) -> None:
        if self._is_board_guide_post(post):
            return

        matches = extract_board_training_matches(post.content)
        conf = await self.config.guild(guild).all()
        log_channel = await self._get_training_log_channel(guild, conf)

        if not matches:
            if log_channel:
                await self._send_board_training_log(
                    log_channel,
                    post,
                    [],
                    f"No known training name found in board post `{post.post_id}`.",
                )
            return

        results: List[Tuple[BoardTrainingMatch, AutoTrainingResult]] = []
        for match in matches:
            req = TrainingRequest(
                user_id=0,
                discipline=match.discipline,
                training=match.training,
                days=match.days,
                fee_per_day=BOARD_DEFAULT_FEE,
                num_classes=1,
                references=[f"MissionChief board post #{post.post_id}"],
                want_reminder=False,
                request_channel_id=0,
            )
            result = await self._try_auto_open_board_training(post, req)
            results.append((match, result))

        if log_channel:
            await self._send_board_training_log(log_channel, post, results, None)
        else:
            log.warning(
                "Training board post %s processed but no Discord log channel is configured or reachable",
                post.post_id,
            )

        if any(result.success for _, result in results):
            reply = self._build_training_board_reply(post, results)
            await self._post_training_board_reply(session, thread_id, page, reply)

    async def _post_training_board_reply(
        self,
        session,
        thread_id: int,
        page: BoardPage,
        content: str,
    ) -> Optional[int]:
        action = page.reply_action or f"/alliance_posts?alliance_thread_id={thread_id}"
        post_url = urljoin("https://www.missionchief.com", action)
        payload = {
            "utf8": "\u2713",
            "alliance_post[content]": content,
            "commit": "Save",
        }
        if page.reply_token:
            payload["authenticity_token"] = page.reply_token

        async with session.post(post_url, data=payload, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            await response.text()
        return status

    def _build_training_board_reply(
        self,
        post: BoardTrainingPost,
        results: List[Tuple[BoardTrainingMatch, AutoTrainingResult]],
    ) -> str:
        opened = [
            f"- {match.training}: opened {result.classes_opened or 1} class(es)"
            + (f" in academy {result.academy_id}" if result.academy_id else "")
            for match, result in results
            if result.success
        ]
        failed = [
            f"- {match.training}: {result.reason}"
            for match, result in results
            if not result.success
        ]

        lines = [f"Training request processed for {post.author_name}."]
        if opened:
            lines.append("")
            lines.append("Opened:")
            lines.extend(opened)
        if failed:
            lines.append("")
            lines.append("Could not open automatically:")
            lines.extend(failed)
        return "\n".join(lines)

    async def _send_board_training_log(
        self,
        log_channel: discord.abc.Messageable,
        post: BoardTrainingPost,
        results: List[Tuple[BoardTrainingMatch, AutoTrainingResult]],
        note: Optional[str],
    ) -> None:
        embed = discord.Embed(
            title="MissionChief board training request",
            color=discord.Color.green() if results and any(result.success for _, result in results) else discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(
            name="Requester",
            value=f"{post.author_name} ({post.author_id or 'unknown ID'})",
            inline=False,
        )
        embed.add_field(name="Board post", value=f"#{post.post_id}", inline=True)
        if post.created_at:
            embed.add_field(name="Posted at", value=post.created_at, inline=True)
        if note:
            embed.add_field(name="Status", value=note, inline=False)
        if results:
            lines = []
            for match, result in results:
                status = "opened" if result.success else f"failed: {result.reason}"
                academy = f" | academy {result.academy_id}" if result.academy_id else ""
                lines.append(f"- {match.training}: {status}{academy}")
            embed.add_field(name="Detected trainings", value="\n".join(lines)[:1024], inline=False)
        embed.add_field(name="Original text", value=(post.content or "-")[:1024], inline=False)
        try:
            await log_channel.send(embed=embed)
        except Exception as exc:
            log.exception("Could not send TrainingManager board log for post %s: %s", post.post_id, exc)

    def _build_board_guide_content(
        self,
        availability: Dict[str, DisciplineAvailability],
        error: Optional[str],
        request_thread_id: int,
    ) -> str:
        return "\n\n".join(
            self._build_board_guide_contents(availability, error, request_thread_id).values()
        )

    def _build_board_guide_contents(
        self,
        availability: Dict[str, DisciplineAvailability],
        error: Optional[str],
        request_thread_id: int,
    ) -> Dict[str, str]:
        updated_at = datetime.now(AMS).strftime("%Y-%m-%d %H:%M %Z")
        overview_lines = [
            self._board_guide_marker(BOARD_GUIDE_OVERVIEW_SECTION),
            "[b]Training Request Guide[/b]",
            "",
            "This post is maintained automatically by the Fire & Rescue Academy bot.",
            f"Last updated: {updated_at}",
            "",
            "[b]Request in this topic[/b]",
            f"Thread: https://www.missionchief.com/alliance_threads/{request_thread_id}",
            "",
            "[b]How to request[/b]",
            "- Type one or more training names from the list below.",
            "- You can request multiple classes in one post, one per line or separated by commas.",
            "- Small typos are supported, but the exact names below work best.",
            "- Board requests are opened as free alliance classes.",
            "- The bot opens 1 class for each recognized training.",
            "- If current data shows your alliance donation is below 5%, the class will not be opened automatically.",
            "",
            "[b]Current academy availability[/b]",
        ]
        if error:
            overview_lines.append(f"Availability could not be refreshed: {error}")
        for discipline in AGENCY_ORDER:
            stats = availability.get(discipline, DisciplineAvailability(discipline=discipline))
            overview_lines.append(f"- {discipline}: {stats.available_classrooms} classes")

        overview_lines.extend(
            [
                "",
                "[b]Guide posts[/b]",
                "The bot keeps one extra post per agency below. Use the training name exactly as shown there when possible.",
                "",
                "[b]Examples[/b]",
                "HazMat",
                "Hotshot Crew Training, K-9",
                "Coastal Air Rescue Operations",
            ]
        )

        contents: Dict[str, str] = {
            BOARD_GUIDE_OVERVIEW_SECTION: "\n".join(overview_lines),
        }
        for discipline in AGENCY_ORDER:
            trainings = DISCIPLINES.get(discipline, [])
            if not trainings:
                continue
            lines = [
                self._board_guide_marker(discipline),
                f"[b]{discipline} training request text[/b]",
                "",
                "Use one of these names in this topic to request a class:",
            ]
            for training, days in trainings:
                day_label = "day" if int(days) == 1 else "days"
                lines.append(f"- {training} ({int(days)} {day_label})")
            contents[discipline] = "\n".join(lines)
        return contents

    def _board_guide_marker(self, section: str) -> str:
        return f"[{BOARD_GUIDE_MARKER_PREFIX}:{section}]"

    def _board_guide_section_from_text(self, text: str) -> Optional[str]:
        match = re.search(rf"\[{re.escape(BOARD_GUIDE_MARKER_PREFIX)}:([^\]]+)\]", str(text or ""))
        return match.group(1) if match else None

    def _is_board_guide_post(self, post: BoardTrainingPost) -> bool:
        return self._board_guide_section_from_text(post.content) is not None

    async def _resolve_channel(self, guild: discord.Guild, channel_id: Optional[int]) -> Optional[discord.abc.Messageable]:
        if not channel_id:
            return None
        try:
            numeric_id = int(channel_id)
        except (TypeError, ValueError):
            return None

        channel = guild.get_channel(numeric_id)
        if channel is not None:
            return channel

        get_channel = getattr(self.bot, "get_channel", None)
        if get_channel:
            channel = get_channel(numeric_id)
            if channel is not None:
                return channel

        fetch_channel = getattr(self.bot, "fetch_channel", None)
        if fetch_channel:
            try:
                return await fetch_channel(numeric_id)
            except Exception as exc:
                log.info("Could not fetch configured channel %s: %s", numeric_id, exc)
        return None

    async def _get_training_log_channel(self, guild: discord.Guild, conf: dict) -> Optional[discord.abc.Messageable]:
        return await self._resolve_channel(guild, conf.get("log_channel_id"))

    def _board_guide_hash(self, content: str) -> str:
        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _set_form_value(
        self,
        payload: Dict[str, str],
        candidates: Tuple[str, ...],
        value: str,
        fallback_name: str,
    ) -> None:
        for name in list(payload.keys()):
            lowered = name.casefold()
            if any(candidate in lowered for candidate in candidates):
                payload[name] = value
                return
        payload[fallback_name] = value

    def _select_post_edit_form(self, forms: List[MissionChiefForm], post_id: int) -> Optional[MissionChiefForm]:
        wanted = f"/alliance_posts/{post_id}"
        for form in forms:
            if wanted in str(form.action or ""):
                return form
        for form in forms:
            if "/alliance_posts" in str(form.action or ""):
                return form
        return forms[0] if forms else None

    async def _submit_missionchief_form(self, session, form: MissionChiefForm, payload: Dict[str, str]) -> Tuple[Optional[int], str, str]:
        action = form.action or ""
        url = urljoin("https://www.missionchief.com", action)
        if form.method == "get":
            async with session.get(url, params=payload, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
                final_url = str(getattr(response, "url", url))
        else:
            async with session.post(url, data=payload, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                html = await response.text()
                final_url = str(getattr(response, "url", url))
        return status, html, final_url

    async def _find_existing_board_guide_posts(self, session, thread_id: int) -> Dict[str, int]:
        found: Dict[str, int] = {}
        base_url = f"https://www.missionchief.com/alliance_threads/{thread_id}"
        async with session.get(base_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            return found

        first_page = parse_training_board_page(html)
        max_page = min(first_page.last_page, BOARD_GUIDE_MAX_SCAN_PAGES)
        for page_number in range(1, max_page + 1):
            page_url = f"{base_url}?page={page_number}"
            async with session.get(page_url, allow_redirects=True) as response:
                status = getattr(response, "status", None)
                page_html = await response.text()
            if status is not None and int(status) >= 400:
                continue
            page = parse_training_board_page(page_html)

            for post in page.posts:
                section = self._board_guide_section_from_text(post.content)
                if section and section in BOARD_GUIDE_SECTIONS:
                    found[section] = post.post_id
        return found

    async def _create_board_guide_post(self, session, thread_id: int, section: str, content: str) -> Tuple[Optional[int], str]:
        page, status = await self._fetch_training_board_latest_page(session, thread_id)
        if status is not None and int(status) >= 400:
            return None, f"request thread returned HTTP {status}"

        post_status = await self._post_training_board_reply(session, thread_id, page, content)
        if post_status is None or int(post_status) >= 400:
            return None, f"create guide post returned HTTP {post_status}"

        page, status = await self._fetch_training_board_latest_page(session, thread_id)
        if status is not None and int(status) >= 400:
            return None, f"could not refetch request thread after create: HTTP {status}"

        for post in sorted(page.posts, key=lambda item: item.post_id, reverse=True):
            if self._board_guide_section_from_text(post.content) == section:
                return post.post_id, "created"
        return None, "created guide post but could not resolve new post ID"

    async def _edit_board_guide_post(self, session, post_id: int, content: str) -> Tuple[bool, str]:
        edit_url = f"https://www.missionchief.com/alliance_posts/{post_id}/edit"
        async with session.get(edit_url, allow_redirects=True) as response:
            status = getattr(response, "status", None)
            html = await response.text()
        if status is not None and int(status) >= 400:
            return False, f"edit form returned HTTP {status}"

        form = self._select_post_edit_form(parse_missionchief_forms(html), post_id)
        if not form:
            return False, "edit form not found"

        payload = dict(form.fields)
        payload.setdefault("utf8", "\u2713")
        payload.setdefault("commit", "Save")
        if "_method" not in payload and f"/alliance_posts/{post_id}" in str(form.action or ""):
            payload["_method"] = "patch"
        self._set_form_value(
            payload,
            ("[content]", "[text]", "[body]", "content", "text", "body"),
            content,
            "alliance_post[content]",
        )
        status, _html, _final_url = await self._submit_missionchief_form(session, form, payload)
        if status is None or int(status) >= 400:
            return False, f"edit post returned HTTP {status}"
        return True, "updated"

    async def _sync_training_board_guide_for_guild(self, guild: discord.Guild, *, force: bool = False) -> bool:
        conf = await self.config.guild(guild).all()
        if not conf.get("board_guide_enabled"):
            return False

        cookie_manager = self.bot.get_cog("CookieManager")
        if not cookie_manager or not hasattr(cookie_manager, "get_session"):
            log.info("Training board guide skipped: CookieManager is not loaded")
            return False

        request_thread_id = int(conf.get("board_thread_id") or BOARD_THREAD_ID)
        availability, error = await self._collect_training_availability()
        contents = self._build_board_guide_contents(availability, error, request_thread_id)
        content_hashes = {
            section: self._board_guide_hash(content)
            for section, content in contents.items()
        }

        post_ids = {
            str(section): int(post_id)
            for section, post_id in dict(conf.get("board_guide_post_ids") or {}).items()
            if str(post_id).isdigit()
        }
        legacy_thread_id = int(conf["board_guide_thread_id"]) if conf.get("board_guide_thread_id") else None
        legacy_post_id = int(conf["board_guide_post_id"]) if conf.get("board_guide_post_id") else None
        if legacy_thread_id == request_thread_id and legacy_post_id and BOARD_GUIDE_OVERVIEW_SECTION not in post_ids:
            post_ids[BOARD_GUIDE_OVERVIEW_SECTION] = legacy_post_id

        stored_hashes = dict(conf.get("board_guide_content_hashes") or {})
        if (
            not force
            and post_ids
            and all(section in post_ids for section in contents)
            and all(stored_hashes.get(section) == content_hashes.get(section) for section in contents)
        ):
            return False

        session = await cookie_manager.get_session()
        discovered_post_ids = await self._find_existing_board_guide_posts(session, request_thread_id)
        post_ids.update(discovered_post_ids)

        changed = False
        for section in BOARD_GUIDE_SECTIONS:
            content = contents.get(section)
            if content is None:
                continue
            post_id = post_ids.get(section)
            section_hash = content_hashes[section]
            if post_id and not force and stored_hashes.get(section) == section_hash:
                continue

            if post_id:
                updated, reason = await self._edit_board_guide_post(session, int(post_id), content)
                if updated:
                    changed = True
                    await asyncio.sleep(1)
                    continue
                log.warning("Training board guide post %s update failed: %s", post_id, reason)

            created_post_id, reason = await self._create_board_guide_post(session, request_thread_id, section, content)
            if not created_post_id:
                log.warning("Training board guide section %s could not be created: %s", section, reason)
                continue
            post_ids[section] = int(created_post_id)
            changed = True
            await asyncio.sleep(1)

        if not post_ids:
            return False

        overview_id = post_ids.get(BOARD_GUIDE_OVERVIEW_SECTION)
        await self.config.guild(guild).board_guide_thread_id.set(int(request_thread_id))
        await self.config.guild(guild).board_guide_post_ids.set({section: int(post_id) for section, post_id in post_ids.items()})
        await self.config.guild(guild).board_guide_content_hashes.set(content_hashes)
        await self.config.guild(guild).board_guide_post_id.set(int(overview_id) if overview_id else None)
        await self.config.guild(guild).board_guide_content_hash.set(
            self._board_guide_hash("\n\n".join(contents.values()))
        )
        return changed

    async def _board_guide_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    async with self._bot_status(f"syncing training board guide in {guild.name}", priority=45):
                        await self._sync_training_board_guide_for_guild(guild)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("TrainingManager board guide loop error: %s", exc)
            await asyncio.sleep(BOARD_GUIDE_SYNC_SECONDS)

    async def _send_auto_open_success(
        self,
        guild: discord.Guild,
        user: discord.abc.User,
        req: TrainingRequest,
        result: AutoTrainingResult,
        log_channel: discord.abc.Messageable,
    ) -> None:
        end_at = datetime.now(AMS) + timedelta(days=req.days)
        fee_txt = "Free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day/trainee"
        training_text = f"{req.discipline} → {req.training} ({req.days}d)"
        if req.num_classes > 1:
            training_text += f" × **{req.num_classes} classes**"

        embed = discord.Embed(
            title="Training automatically opened",
            color=discord.Color.green(),
            timestamp=datetime.now(timezone.utc),
        )
        embed.add_field(name="Requester", value=f"{user.mention} ({user.id})", inline=False)
        embed.add_field(name="Training", value=training_text, inline=False)
        embed.add_field(name="Academy", value=f"[Building {result.academy_id}](https://www.missionchief.com/buildings/{result.academy_id})", inline=True)
        embed.add_field(name="Fee", value=fee_txt, inline=True)
        embed.add_field(name="Alliance share", value="1 hour", inline=True)
        if result.mc_user_id or result.mc_username:
            embed.add_field(
                name="MissionChief member",
                value=f"{result.mc_username or 'Unknown'} ({result.mc_user_id or 'unknown ID'})",
                inline=False,
            )
        if result.contribution_rate is not None:
            embed.add_field(name="Contribution", value=f"{result.contribution_rate:.1f}%", inline=True)
        embed.add_field(name="Expected end time", value=fmt_dt(end_at), inline=False)
        embed.set_footer(text=f"Course value: {result.course_value} • HTTP {result.status}")

        await log_channel.send(embed=embed)

        if req.want_reminder:
            ref_text = ""
            if req.references and any(req.references):
                ref_text = " (Multiple references)" if len(set(req.references)) > 1 else f" Reference: {req.references[0]}"
            await self._add_reminder(
                guild_id=guild.id,
                user_id=user.id,
                text=f"Your **{req.training}** class has finished.{ref_text}",
                when=end_at.astimezone(timezone.utc),
                fallback_channel_id=req.request_channel_id,
            )

    async def _notify_auto_open_requester(
        self,
        user: discord.abc.User,
        req: TrainingRequest,
        result: AutoTrainingResult,
        request_channel: Optional[discord.abc.Messageable],
    ) -> None:
        fee_txt = "free" if req.fee_per_day == 0 else f"{req.fee_per_day} credits/day"
        class_txt = "class" if req.num_classes == 1 else "classes"
        message = (
            f"Your training has been started automatically: **{req.training}** "
            f"({req.num_classes} {class_txt}, {fee_txt})."
        )
        if result.academy_id:
            message += f"\nAcademy: https://www.missionchief.com/buildings/{result.academy_id}"

        try:
            await user.send(message)
            return
        except Exception as exc:
            log.info("Could not DM automatic training success to %s: %s", getattr(user, "id", "unknown"), exc)

        if request_channel is None:
            return

        try:
            fallback_message = await request_channel.send(f"{getattr(user, 'mention', '')} {message}")
            asyncio.create_task(self._delete_later(fallback_message, 15 * 60))
        except Exception as exc:
            log.info("Could not send/delete automatic training fallback notification: %s", exc)

    async def _notify_auto_open_fallback_requester(
        self,
        user: discord.abc.User,
        req: TrainingRequest,
        reason: str,
        request_channel: Optional[discord.abc.Messageable],
    ) -> None:
        message = (
            f"Your training request for **{req.training}** could not be opened automatically "
            f"and has been sent to admins for manual start.\nReason: {reason}"
        )
        try:
            await user.send(message)
            return
        except Exception as exc:
            log.info("Could not DM automatic training fallback to %s: %s", getattr(user, "id", "unknown"), exc)

        if request_channel is None:
            return

        try:
            fallback_message = await request_channel.send(f"{getattr(user, 'mention', '')} {message}")
            asyncio.create_task(self._delete_later(fallback_message, 15 * 60))
        except Exception as exc:
            log.info("Could not send/delete automatic training fallback explanation: %s", exc)

    async def _delete_later(self, message: discord.Message, delay_seconds: int) -> None:
        await asyncio.sleep(delay_seconds)
        try:
            await message.delete()
        except Exception as exc:
            log.info("Could not delete temporary automatic training notification: %s", exc)

    def _build_availability_embed(
        self,
        availability: Dict[str, DisciplineAvailability],
        error: Optional[str] = None,
    ) -> discord.Embed:
        description = "\n".join(
            f"**{discipline}:** {availability.get(discipline, DisciplineAvailability(discipline=discipline)).available_classrooms} classes"
            for discipline in AGENCY_ORDER
        )
        embed = discord.Embed(
            title="Academy Availability",
            description=description,
            color=discord.Color.blurple() if not error else discord.Color.orange(),
            timestamp=datetime.now(timezone.utc),
        )
        if error:
            embed.add_field(name="Status", value=f"Could not refresh automatically: {error}", inline=False)
        embed.set_footer(text="TrainingManager - refreshes every hour")
        return embed

    async def _refresh_availability_panel_for_guild(self, guild: discord.Guild, *, create_if_missing: bool = False) -> Optional[discord.Message]:
        conf = await self.config.guild(guild).all()
        if not conf.get("availability_enabled") and not create_if_missing:
            return None

        channel_id = int(conf.get("availability_channel_id") or MEMBER_PANEL_CHANNEL_ID)
        channel = guild.get_channel(channel_id)
        if channel is None:
            log.warning("TrainingManager availability channel not found: %s", channel_id)
            return None

        availability, error = await self._collect_training_availability()
        embed = self._build_availability_embed(availability, error)
        message_id = conf.get("availability_message_id")
        if message_id:
            try:
                message = await channel.fetch_message(int(message_id))
                await message.edit(embed=embed)
                return message
            except Exception as exc:
                log.info("TrainingManager availability message missing; reposting: %s", exc)

        message = await channel.send(embed=embed)
        await self.config.guild(guild).availability_message_id.set(message.id)
        await self.config.guild(guild).availability_channel_id.set(channel.id)
        await self.config.guild(guild).availability_enabled.set(True)
        return message

    async def _availability_loop(self) -> None:
        await self.bot.wait_until_red_ready()
        while True:
            try:
                for guild in self.bot.guilds:
                    await self._refresh_availability_panel_for_guild(guild)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("TrainingManager availability loop error: %s", exc)
            await asyncio.sleep(AVAILABILITY_REFRESH_SECONDS)

    async def _build_member_panel_embed(self, guild: discord.Guild) -> discord.Embed:
        custom_msg = await self.config.guild(guild).button_message()
        if custom_msg:
            description = custom_msg
        else:
            description = (
                "**Start Request**: Submit a full training request. The bot will try to open it automatically. "
                "If that is not possible, it will be sent to admins for review.\n"
                "**Reminder Only**: Set a reminder for your training without requesting a new class.\n\n"
                "Choose an option below to get started."
            )

        return discord.Embed(
            title="Training Request System",
            description=description,
            color=discord.Color.blurple(),
        )

    async def _send_member_panel(self, guild: discord.Guild, channel: discord.TextChannel) -> discord.Message:
        embed = await self._build_member_panel_embed(guild)
        message = await channel.send(embed=embed, view=StartView(self))
        await self.config.guild(guild).panel_message_id.set(message.id)
        return message

    async def _ensure_member_panel_for_guild(self, guild: discord.Guild) -> None:
        conf = await self.config.guild(guild).all()
        configured_channel_id = int(conf.get("request_channel_id") or MEMBER_PANEL_CHANNEL_ID)
        channel_id = MEMBER_PANEL_CHANNEL_ID
        channel = guild.get_channel(channel_id)
        if channel is None:
            log.warning("TrainingManager member panel channel not found: %s", channel_id)
            return

        if configured_channel_id != MEMBER_PANEL_CHANNEL_ID:
            await self.config.guild(guild).request_channel_id.set(MEMBER_PANEL_CHANNEL_ID)

        now = datetime.now(timezone.utc)
        last_post = conf.get("panel_last_auto_post_at")
        if last_post:
            try:
                last_dt = datetime.fromisoformat(str(last_post))
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                if (now - last_dt).total_seconds() < 600:
                    return
            except ValueError:
                pass

        await self._send_member_panel(guild, channel)
        await self.config.guild(guild).panel_last_auto_post_at.set(now.isoformat())

    async def _ensure_member_panels(self) -> None:
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            try:
                await self._ensure_member_panel_for_guild(guild)
            except Exception as exc:
                log.exception("Could not ensure TrainingManager member panel in %s: %s", guild, exc)

    async def _send_developer_panel(self, guild: discord.Guild, channel: discord.TextChannel) -> discord.Message:
        embed = discord.Embed(
            title="Developer Training Auto-Open",
            description=(
                "Use this private test panel to open MissionChief training classes automatically "
                "without changing the normal member request flow.\n\n"
                "This is for controlled testing only."
            ),
            color=discord.Color.orange(),
        )
        message = await channel.send(embed=embed, view=DeveloperTrainingPanelView(self))
        await self.config.guild(guild).developer_panel_message_id.set(message.id)
        return message

    async def _ensure_developer_panel_for_guild(self, guild: discord.Guild) -> None:
        conf = await self.config.guild(guild).all()
        channel_id = int(conf.get("developer_panel_channel_id") or DEVELOPER_PANEL_CHANNEL_ID)
        channel = guild.get_channel(channel_id)
        if channel is None:
            log.warning("TrainingManager developer panel channel not found: %s", channel_id)
            return

        message_id = conf.get("developer_panel_message_id")
        if message_id:
            try:
                await channel.fetch_message(int(message_id))
                return
            except Exception as exc:
                log.info("TrainingManager developer panel message missing; reposting: %s", exc)

        await self._send_developer_panel(guild, channel)

    async def _ensure_developer_panels(self) -> None:
        await self.bot.wait_until_red_ready()
        for guild in self.bot.guilds:
            try:
                await self._ensure_developer_panel_for_guild(guild)
            except Exception as exc:
                log.exception("Could not ensure TrainingManager developer panel in %s: %s", guild, exc)

    # --------------- Commands ---------------

    @commands.group(name="tmset", invoke_without_command=True)
    @commands.admin()
    async def tmset(self, ctx: commands.Context):
        """Configure Training Manager. Subcommands: requestchannel, adminchannel, logchannel, adminrole, buttonmessage, post."""
        conf = await self.config.guild(ctx.guild).all()
        txt = (
            f"Request channel: {ctx.guild.get_channel(conf['request_channel_id']).mention if conf.get('request_channel_id') else '—'}\n"
            f"Admin channel: {ctx.guild.get_channel(conf['admin_channel_id']).mention if conf.get('admin_channel_id') else '—'}\n"
            f"Log channel: {ctx.guild.get_channel(conf['log_channel_id']).mention if conf.get('log_channel_id') else '—'}\n"
            f"Admin role: {ctx.guild.get_role(conf['admin_role_id']).mention if conf.get('admin_role_id') else '—'}\n"
            f"Custom button message: {'Set' if conf.get('button_message') else 'Not set (using default)'}\n"
        )
        await ctx.send(box(txt, lang="ini"))

    @tmset.command()
    @commands.admin()
    async def requestchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where users can request trainings."""
        await self.config.guild(ctx.guild).request_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def adminchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where admin approval requests are sent."""
        await self.config.guild(ctx.guild).admin_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def logchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where all actions are logged."""
        await self.config.guild(ctx.guild).log_channel_id.set(channel.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def adminrole(self, ctx: commands.Context, role: discord.Role):
        """Set the role that can approve/reject training requests."""
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.tick()

    @tmset.command()
    @commands.admin()
    async def buttonmessage(self, ctx: commands.Context, *, message: str = None):
        """Set a custom message above the Start Request and Reminder Only buttons.
        
        Use this to explain the difference between the two options.
        Leave empty to reset to default message.
        
        Example: [p]tmset buttonmessage Use **Start Request** to submit a full training request that requires admin approval. Use **Reminder Only** to simply set a reminder for yourself without admin approval.
        """
        if message:
            await self.config.guild(ctx.guild).button_message.set(message)
            await ctx.send(f"Custom button message set. Use `{ctx.prefix}tmset post` to update the message in the request channel.")
        else:
            await self.config.guild(ctx.guild).button_message.set(None)
            await ctx.send(f"Button message reset to default. Use `{ctx.prefix}tmset post` to update the message in the request channel.")

    @tmset.command()
    @commands.admin()
    async def post(self, ctx: commands.Context):
        """Post or update the 'Start Request' and 'Reminder Only' buttons in the request channel."""
        request_channel_id = MEMBER_PANEL_CHANNEL_ID
        await self.config.guild(ctx.guild).request_channel_id.set(request_channel_id)
        ch = ctx.guild.get_channel(request_channel_id)
        if not ch:
            await ctx.send("The configured request channel was not found.")
            return
        
        await self._send_member_panel(ctx.guild, ch)
        await ctx.send(f"TrainingManager panel posted in {ch.mention}.")

    @tmset.command(name="devpost")
    @commands.admin()
    async def developer_post(self, ctx: commands.Context):
        """Repost the developer-only TrainingManager auto-open test panel."""
        channel_id = await self.config.guild(ctx.guild).developer_panel_channel_id()
        channel = ctx.guild.get_channel(int(channel_id or DEVELOPER_PANEL_CHANNEL_ID))
        if not channel:
            await ctx.send("The configured developer panel channel was not found.")
            return
        await self._send_developer_panel(ctx.guild, channel)
        await ctx.send(f"Developer TrainingManager panel posted in {channel.mention}.")

    @tmset.command(name="availabilitypost")
    @commands.admin()
    async def availability_post(self, ctx: commands.Context, channel: discord.TextChannel = None):
        """Post or refresh the training classroom availability overview."""
        target_channel = channel or ctx.channel
        await self.config.guild(ctx.guild).availability_channel_id.set(target_channel.id)
        await self.config.guild(ctx.guild).availability_enabled.set(True)
        message = await self._refresh_availability_panel_for_guild(ctx.guild, create_if_missing=True)
        if message:
            await ctx.send(f"Training availability overview posted in {target_channel.mention}.")
        else:
            await ctx.send("Could not post the training availability overview.")

    @tmset.command(name="availabilitystop")
    @commands.admin()
    async def availability_stop(self, ctx: commands.Context):
        """Stop automatic refreshes for the training classroom availability overview."""
        await self.config.guild(ctx.guild).availability_enabled.set(False)
        await ctx.send("Training availability overview refresh stopped.")

    @tmset.command(name="board")
    @commands.admin()
    async def board_polling(self, ctx: commands.Context, state: str = "status"):
        """Manage MissionChief board training request polling. Use on/off/status/reset."""
        normalized = str(state or "status").casefold().strip()
        if normalized in {"on", "enable", "enabled"}:
            await self.config.guild(ctx.guild).board_poll_enabled.set(True)
            await ctx.send("Training board polling enabled.")
            return
        if normalized in {"off", "disable", "disabled"}:
            await self.config.guild(ctx.guild).board_poll_enabled.set(False)
            await ctx.send("Training board polling disabled.")
            return
        if normalized == "reset":
            await self.config.guild(ctx.guild).board_last_seen_post_id.set(None)
            await ctx.send("Training board baseline reset. The next poll will baseline the latest post without processing older posts.")
            return

        conf = await self.config.guild(ctx.guild).all()
        await ctx.send(
            "Training board polling status\n"
            f"Enabled: {bool(conf.get('board_poll_enabled'))}\n"
            f"Thread ID: {conf.get('board_thread_id') or BOARD_THREAD_ID}\n"
            f"Last seen post ID: {conf.get('board_last_seen_post_id') or 'not set'}\n"
            f"Interval: {BOARD_POLL_SECONDS // 60} minutes"
        )

    @tmset.command(name="boardthread")
    @commands.admin()
    async def board_thread(self, ctx: commands.Context, thread_id: int = BOARD_THREAD_ID):
        """Set the MissionChief alliance thread ID used for board training requests."""
        await self.config.guild(ctx.guild).board_thread_id.set(int(thread_id))
        await self.config.guild(ctx.guild).board_last_seen_post_id.set(None)
        await ctx.send(
            f"Training board thread set to `{int(thread_id)}`. "
            "The next poll will baseline the latest post without processing older posts."
        )

    @tmset.command(name="boardguide")
    @commands.admin()
    async def board_guide(self, ctx: commands.Context, state: str = "status"):
        """Manage the MissionChief board training guide topic. Use on/off/status/reset/sync."""
        normalized = str(state or "status").casefold().strip()
        if normalized in {"on", "enable", "enabled"}:
            await self.config.guild(ctx.guild).board_guide_enabled.set(True)
            await ctx.send("Training board guide sync enabled.")
            return
        if normalized in {"off", "disable", "disabled"}:
            await self.config.guild(ctx.guild).board_guide_enabled.set(False)
            await ctx.send("Training board guide sync disabled.")
            return
        if normalized == "reset":
            await self.config.guild(ctx.guild).board_guide_thread_id.set(None)
            await self.config.guild(ctx.guild).board_guide_post_id.set(None)
            await self.config.guild(ctx.guild).board_guide_post_ids.set({})
            await self.config.guild(ctx.guild).board_guide_content_hash.set(None)
            await self.config.guild(ctx.guild).board_guide_content_hashes.set({})
            await ctx.send("Training board guide tracking reset. The next sync will find or create managed guide posts in the request topic.")
            return
        if normalized in {"sync", "refresh", "now"}:
            updated = await self._sync_training_board_guide_for_guild(ctx.guild, force=True)
            await ctx.send("Training board guide synced." if updated else "Training board guide sync did not change anything.")
            return

        conf = await self.config.guild(ctx.guild).all()
        thread_id = conf.get("board_guide_thread_id")
        post_ids = dict(conf.get("board_guide_post_ids") or {})
        request_thread_id = conf.get("board_thread_id") or BOARD_THREAD_ID
        await ctx.send(
            "Training board guide status\n"
            f"Enabled: {bool(conf.get('board_guide_enabled'))}\n"
            f"Request thread ID: {request_thread_id}\n"
            f"Managed thread ID: {thread_id or 'not set'}\n"
            f"Managed guide posts: {len(post_ids)} / {len(BOARD_GUIDE_SECTIONS)}\n"
            f"Sync interval: {BOARD_GUIDE_SYNC_SECONDS // 60} minutes"
        )
