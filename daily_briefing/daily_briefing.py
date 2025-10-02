
from __future__ import annotations

import asyncio
import aiosqlite
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import re
import discord
from redbot.core import commands, checks, Config
from redbot.core.data_manager import cog_data_path

__version__ = "0.1.0"

log = logging.getLogger("red.FARA.DailyBriefing")

NY_TZ = "America/New_York"
NL = "\n"

DEFAULTS = {
    "channels": {
        "daily": None,
        "monthly": None,
        "overview": None,
        "adminlog": None
    },
    "times": {
        "daily": "23:50",
        "monthly": "23:50"
    },
    "timezone": NY_TZ,
    "hot_refresh": True,
    "top_n": 10,
    "extras": {
        "streaks": True,
        "rising": True,
        "admin": True,
        "training": True
    },
    "last_published": {
        "daily": None,     # "YYYY-MM-DD" in NY time
        "monthly": None    # "YYYY-MM"
    }
}

# --- Small helpers ---

def _tz_now(tzname: str) -> datetime:
    return datetime.now(ZoneInfo(tzname))

def _ny_day_bounds(target_date: datetime) -> Tuple[datetime, datetime]:
    # target_date is NY-aware; compute [00:00, 23:50] window in NY, then return UTC datetimes
    ny = target_date.astimezone(ZoneInfo(NY_TZ))
    start_ny = ny.replace(hour=0, minute=0, second=0, microsecond=0)
    end_ny = ny.replace(hour=23, minute=50, second=0, microsecond=0)
    return start_ny.astimezone(timezone.utc), end_ny.astimezone(timezone.utc)

def _ny_month_bounds(year: int, month: int) -> Tuple[datetime, datetime]:
    ny = ZoneInfo(NY_TZ)
    start_ny = datetime(year, month, 1, 0, 0, 0, tzinfo=ny)
    if month == 12:
        next_ny = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=ny)
    else:
        next_ny = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=ny)
    last_ny = next_ny - timedelta(days=1)
    end_ny = last_ny.replace(hour=23, minute=50, second=0, microsecond=0)
    return start_ny.astimezone(timezone.utc), end_ny.astimezone(timezone.utc)

def _fmt_mc_profile(mc_id: Optional[str], name: str) -> str:
    name = name or "Unknown"
    if mc_id:
        return f"[{name}](https://www.missionchief.com/users/{mc_id})"
    return name

def _discord_profile_url(discord_id: int) -> str:
    return f"https://discord.com/users/{discord_id}"

def _clean_int(x) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return 0

class DailyBriefing(commands.Cog):
    """Daily and Monthly briefing + alliance overview."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xDA117B12, force_registration=True)
        self.config.register_global(**DEFAULTS)
        self.data_path = cog_data_path(self)
        self.state_db = self.data_path / "briefing_state.db"
        self._bg_task: Optional[asyncio.Task] = None

    async def cog_load(self):
        await self._init_state_db()
        await self._maybe_start_background()

    async def _init_state_db(self):
        self.data_path.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.state_db) as db:
            await db.execute("CREATE TABLE IF NOT EXISTS top_history(day TEXT, mc_user_id TEXT, PRIMARY KEY(day, mc_user_id))")
            await db.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
            await db.commit()

    # ---- Scraper DB access (read-only) ----
    def _scraper_db_path(self) -> Optional[str]:
        # We expect AllianceScraper to expose db_path or method. Try to discover it.
        sc = self.bot.get_cog("AllianceScraper")
        if sc and hasattr(sc, "db_path"):
            try:
                return str(sc.db_path)  # type: ignore
            except Exception:
                pass
        # fallback: known location under scraper cog data
        guess = self.bot._data_path / "cogs" / "AllianceScraper" / "alliance.db"  # type: ignore
        try:
            if guess.exists():
                return str(guess)
        except Exception:
            pass
        return None

    async def _hot_refresh_members(self) -> None:
        try:
            sc = self.bot.get_cog("AllianceScraper")
            if not sc:
                return
            # Support multiple method names; do nothing if missing.
            for m in ("force_refresh_members", "refresh_members", "run_members_once"):
                if hasattr(sc, m):
                    fn = getattr(sc, m)
                    if asyncio.iscoroutinefunction(fn):
                        await fn()  # type: ignore
                    else:
                        await self.bot.loop.run_in_executor(None, fn)  # type: ignore
                    break
        except Exception:
            log.exception("Hot refresh failed")

    # ---- MemberSync link ----
    async def _discord_id_for_mc(self, mc_user_id: str) -> Optional[int]:
        ms = self.bot.get_cog("MemberSync")
        if not ms or not mc_user_id:
            return None
        try:
            link = await ms.get_link_for_mc(str(mc_user_id))  # type: ignore
            if link and link.get("status") == "approved":
                return int(link["discord_id"])
        except Exception:
            pass
        return None

    # ---- Column helpers ----
    async def _table_columns(self, db: aiosqlite.Connection, table: str) -> List[str]:
        try:
            cur = await db.execute(f"PRAGMA table_info({table})")
            rows = await cur.fetchall()
            return [r[1] for r in rows]
        except Exception:
            return []

    async def _pick_ts_col(self, db: aiosqlite.Connection, table: str) -> Optional[str]:
        cols = [c.lower() for c in await self._table_columns(db, table)]
        for k in ("ts", "timestamp", "observed_at", "scraped_at", "updated_at", "created_at", "time"):
            if k in cols:
                return k
        return None

    # ---- Credits delta logic ----
    async def _credits_delta_top(self, start_utc: datetime, end_utc: datetime, top_n: int) -> List[Dict[str, Any]]:
        db_path = self._scraper_db_path()
        if not db_path:
            return []
        results: Dict[str, Dict[str, Any]] = {}
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            # find members_history table columns
            # expected: mc_user_id, name, earned_credits, ts-like, maybe role
            hist_table = "members_history"
            cols = await self._table_columns(db, hist_table)
            if not cols:
                return []
            id_col = "mc_user_id" if "mc_user_id" in cols else ("user_id" if "user_id" in cols else None)
            name_col = "name" if "name" in cols else ("username" if "username" in cols else None)
            credits_col = "earned_credits" if "earned_credits" in cols else ("credits" if "credits" in cols else None)
            ts_col = await self._pick_ts_col(db, hist_table)
            if not id_col or not credits_col or not ts_col:
                return []
            # end snapshot per user
            q_end = f"""
                SELECT {id_col} AS mc_user_id, {name_col} AS name, {credits_col} AS earned, {ts_col} AS ts
                FROM {hist_table}
                WHERE {ts_col} <= ?
                AND {ts_col} >= ?
                ORDER BY {id_col}, {ts_col} DESC
            """
            # We'll walk through rows and keep first per user as "end"
            start_cut = (start_utc - timedelta(days=2)).isoformat()  # widen window to catch a row before start
            end_cut = end_utc.isoformat()
            cur = await db.execute(q_end, (end_cut, start_cut))
            rows = await cur.fetchall()
            seen_end = set()
            for r in rows:
                uid = str(r["mc_user_id"])
                if uid not in seen_end:
                    results[uid] = {
                        "mc_user_id": uid,
                        "name": r["name"] or "",
                        "end": _clean_int(r["earned"]),
                        "start": None
                    }
                    seen_end.add(uid)
            # start snapshot strictly before start_utc
            q_start = f"""
                SELECT {id_col} AS mc_user_id, {credits_col} AS earned, {ts_col} AS ts
                FROM {hist_table}
                WHERE {ts_col} < ?
                ORDER BY {id_col}, {ts_col} DESC
            """
            cur = await db.execute(q_start, (start_utc.isoformat(),))
            rows = await cur.fetchall()
            # keep first per user as "start"
            seen_start = set()
            for r in rows:
                uid = str(r["mc_user_id"])
                if uid in seen_start:
                    continue
                if uid not in results:
                    results[uid] = {"mc_user_id": uid, "name": "", "end": None, "start": _clean_int(r["earned"])}
                else:
                    if results[uid].get("start") is None:
                        results[uid]["start"] = _clean_int(r["earned"])
                seen_start.add(uid)
        # compute deltas
        out: List[Dict[str, Any]] = []
        for uid, data in results.items():
            end = data.get("end")
            start = data.get("start")
            if end is None or start is None:
                continue
            delta = max(0, int(end) - int(start))
            if delta <= 0:
                continue
            out.append({
                "mc_user_id": uid,
                "name": data.get("name") or "Unknown",
                "delta": delta
            })
        out.sort(key=lambda x: (-x["delta"], x["name"].lower()))
        return out[:max(1, int(top_n))]

    # ---- Alliance logs counting ----
    async def _count_logs(self, start_utc: datetime, end_utc: datetime) -> Dict[str, int]:
        db_path = self._scraper_db_path()
        if not db_path:
            return {}
        keys = {
            "added_to_alliance",
            "left_alliance",
            "kicked_from_alliance",
            "chat_ban_set",
            "chat_ban_removed",
            "building_constructed",
            "building_destroyed",
            "extension_started",
            "expansion_finished",
            "large_mission_started",
            "alliance_event_started",
            "created_a_course",
            "course_completed",
            "set_as_admin","removed_as_admin",
            "set_as_co_admin","removed_as_co_admin",
            "set_as_staff","removed_as_staff",
            "set_as_finance_admin","removed_as_finance_admin",
            "set_as_education_admin","removed_as_education_admin",
            "set_as_moderator_action_admin","removed_as_moderator_action_admin",
            "promoted_to_event_manager","removed_as_event_manager",
            "set_transport_request_admin","removed_transport_request_admin"
        }
        out = {k: 0 for k in keys}
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            table = "alliance_logs"
            cols = await self._table_columns(db, table)
            if not cols:
                return out
            ts_col = "ts" if "ts" in cols else (await self._pick_ts_col(db, table) or "ts")
            key_col = "action_key" if "action_key" in cols else ("action" if "action" in cols else None)
            if not key_col:
                return out
            q = f"SELECT {key_col} AS k FROM {table} WHERE {ts_col} >= ? AND {ts_col} <= ?"
            cur = await db.execute(q, (start_utc.isoformat(), end_utc.isoformat()))
            rows = await cur.fetchall()
            for r in rows:
                k = str(r["k"] or "").strip()
                if k in out:
                    out[k] += 1
        return out

    async def _admin_activity_top(self, start_utc: datetime, end_utc: datetime, top_n: int = 3) -> List[Dict[str, Any]]:
        db_path = self._scraper_db_path()
        if not db_path:
            return []
        admin_keys = {
            "set_as_admin","removed_as_admin",
            "set_as_co_admin","removed_as_co_admin",
            "set_as_staff","removed_as_staff",
            "set_as_finance_admin","removed_as_finance_admin",
            "set_as_education_admin","removed_as_education_admin",
            "set_as_moderator_action_admin","removed_as_moderator_action_admin",
            "chat_ban_set","chat_ban_removed",
            "set_transport_request_admin","removed_transport_request_admin",
            "promoted_to_event_manager","removed_as_event_manager",
        }
        actors: Dict[str, Dict[str, Any]] = {}
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            table = "alliance_logs"
            cols = await self._table_columns(db, table)
            if not cols:
                return []
            ts_col = "ts" if "ts" in cols else (await self._pick_ts_col(db, table) or "ts")
            key_col = "action_key" if "action_key" in cols else ("action" if "action" in cols else None)
            exec_id_col = "executed_mc_id" if "executed_mc_id" in cols else ("executed_id" if "executed_id" in cols else None)
            exec_name_col = "executed_name" if "executed_name" in cols else ("executed" if "executed" in cols else None)
            if not key_col:
                return []
            q = f"SELECT {key_col} AS k, {exec_id_col} AS eid, {exec_name_col} AS ename FROM {table} WHERE {ts_col} >= ? AND {ts_col} <= ?"
            cur = await db.execute(q, (start_utc.isoformat(), end_utc.isoformat()))
            rows = await cur.fetchall()
            for r in rows:
                k = str(r["k"] or "").strip()
                if k not in admin_keys:
                    continue
                eid = str(r["eid"] or "") if exec_id_col else ""
                ename = str(r["ename"] or "") if exec_name_col else ""
                key = eid or ename or "unknown"
                if key not in actors:
                    actors[key] = {"mc_user_id": eid or None, "name": ename or "Unknown", "count": 0}
                actors[key]["count"] += 1
        out = list(actors.values())
        out.sort(key=lambda x: (-x["count"], (x["name"] or "").lower()))
        return out[:max(1, int(top_n))]

    async def _training_summary(self, start_utc: datetime, end_utc: datetime) -> Dict[str, Dict[str, int]]:
        """Return {'created': {course: n}, 'completed': {course: n}}"""
        db_path = self._scraper_db_path()
        if not db_path:
            return {"created": {}, "completed": {}}
        created: Dict[str, int] = {}
        completed: Dict[str, int] = {}
        async with aiosqlite.connect(db_path) as db:
            db.row_factory = aiosqlite.Row
            table = "alliance_logs"
            cols = await self._table_columns(db, table)
            if not cols:
                return {"created": {}, "completed": {}}
            ts_col = "ts" if "ts" in cols else (await self._pick_ts_col(db, table) or "ts")
            key_col = "action_key" if "action_key" in cols else ("action" if "action" in cols else None)
            desc_col = "description" if "description" in cols else ("action_text" if "action_text" in cols else None)
            if not key_col or not desc_col:
                return {"created": {}, "completed": {}}
            q = f"SELECT {key_col} AS k, {desc_col} AS d FROM {table} WHERE {ts_col} >= ? AND {ts_col} <= ?"
            cur = await db.execute(q, (start_utc.isoformat(), end_utc.isoformat()))
            rows = await cur.fetchall()
            for r in rows:
                k = (r["k"] or "").strip()
                d = (r["d"] or "").strip()
                # try to pull course inside parentheses e.g. "Course completed (Smoke Jumper Training)"
                m = re.search(r"\(([^)]+)\)", d)
                course = m.group(1).strip() if m else "Unknown"
                if k == "created_a_course":
                    created[course] = created.get(course, 0) + 1
                elif k == "course_completed":
                    completed[course] = completed.get(course, 0) + 1
        return {"created": created, "completed": completed}

    # ---- Streaks tracking ----
    async def _save_daily_top_for_streaks(self, ny_date: str, mc_ids: List[str]) -> None:
        async with aiosqlite.connect(self.state_db) as db:
            await db.executemany("INSERT OR REPLACE INTO top_history(day, mc_user_id) VALUES(?, ?)", [(ny_date, x) for x in mc_ids])
            await db.commit()

    async def _streak_for(self, ny_date: str, mc_user_id: str) -> int:
        # count how many consecutive days backwards
        async with aiosqlite.connect(self.state_db) as db:
            streak = 0
            dt = datetime.strptime(ny_date, "%Y-%m-%d").date()
            while True:
                day = dt.strftime("%Y-%m-%d")
                cur = await db.execute("SELECT 1 FROM top_history WHERE day=? AND mc_user_id=?", (day, mc_user_id))
                row = await cur.fetchone()
                if row:
                    streak += 1
                    dt = dt - timedelta(days=1)
                else:
                    break
            return streak

    async def _yesterday_delta_map(self, start_utc: datetime, end_utc: datetime) -> Dict[str, int]:
        # compute yesterday window and return delta map for rising stars calc
        ny_end = end_utc.astimezone(ZoneInfo(NY_TZ))
        y_ny = (ny_end - timedelta(days=1)).date()
        ys, ye = _ny_day_bounds(datetime(y_ny.year, y_ny.month, y_ny.day, tzinfo=ZoneInfo(NY_TZ)))
        top_y = await self._credits_delta_top(ys, ye, 10000)
        return {x["mc_user_id"]: int(x["delta"]) for x in top_y}

    # ---- Rendering ----
    async def _render_daily_top_embed(self, ny_date: datetime, top_n: int) -> Optional[discord.Embed]:
        start_utc, end_utc = _ny_day_bounds(ny_date)
        top = await self._credits_delta_top(start_utc, end_utc, top_n)
        if not top:
            return None
        # streaks + rising star
        cfg = await self.config.extras()
        streaks_on = bool(cfg.get("streaks", True))
        rising_on = bool(cfg.get("rising", True))

        # save today's top for streaks
        day_str = ny_date.strftime("%Y-%m-%d")
        await self._save_daily_top_for_streaks(day_str, [t["mc_user_id"] for t in top])

        ymap = await self._yesterday_delta_map(start_utc, end_utc) if rising_on else {}

        e = discord.Embed(
            title="Daily Top Players",
            description=f"`{day_str}` (America/New_York)",
            color=0x3498DB,
            timestamp=datetime.utcnow()
        )
        total_today = 0
        lines = []
        rank = 1
        for row in top:
            mcid = row["mc_user_id"]
            name = row["name"]
            delta = int(row["delta"])
            total_today += delta
            link = _fmt_mc_profile(mcid, name)
            # add [D] if linked
            did = await self._discord_id_for_mc(mcid)
            if did:
                link = f"{link} [[D]]({_discord_profile_url(did)})"
            extras = []
            if streaks_on:
                s = await self._streak_for(day_str, mcid)
                if s > 1:
                    extras.append(f"streak {s}")
            if rising_on and mcid in ymap:
                diff = delta - int(ymap[mcid])
                if diff > 0:
                    extras.append(f"+{diff} vs yesterday")
            extra_txt = f" _( {'; '.join(extras)} )_" if extras else ""
            lines.append(f"**{rank}.** {link} — `+{delta:,}`{extra_txt}")
            rank += 1
        e.add_field(name="Top 10", value=NL.join(lines), inline=False)
        e.set_footer(text=f"Total credits earned (Top {len(top)}): {total_today:,}")
        return e

    async def _render_monthly_top_embed(self, ny_date: datetime, top_n: int) -> Optional[discord.Embed]:
        y = ny_date.year
        m = ny_date.month
        start_utc, end_utc = _ny_month_bounds(y, m)
        top = await self._credits_delta_top(start_utc, end_utc, top_n)
        if not top:
            return None
        e = discord.Embed(
            title="Monthly Top Players",
            description=f"`{y}-{m:02d}` (America/New_York)",
            color=0x9B59B6,
            timestamp=datetime.utcnow()
        )
        lines = []
        total_month = 0
        rank = 1
        for row in top:
            mcid = row["mc_user_id"]
            name = row["name"]
            delta = int(row["delta"])
            total_month += delta
            link = _fmt_mc_profile(mcid, name)
            did = await self._discord_id_for_mc(mcid)
            if did:
                link = f"{link} [[D]]({_discord_profile_url(did)})"
            lines.append(f"**{rank}.** {link} — `+{delta:,}`")
            rank += 1
        e.add_field(name="Top 10", value=NL.join(lines), inline=False)
        e.set_footer(text=f"Total credits earned (Top {len(top)}): {total_month:,}")
        return e

    async def _render_overview_embed(self, ny_date: datetime) -> Optional[discord.Embed]:
        start_utc, end_utc = _ny_day_bounds(ny_date)
        counts = await self._count_logs(start_utc, end_utc)
        cfg = await self.config.extras()
        admin_on = bool(cfg.get("admin", True))
        training_on = bool(cfg.get("training", True))

        e = discord.Embed(
            title="Alliance Daily Briefing",
            description=f"`{ny_date.strftime('%Y-%m-%d')}` (America/New_York)",
            color=0xF1C40F,
            timestamp=datetime.utcnow()
        )
        # Members
        members_val = NL.join([
            f"Joined: `{counts.get('added_to_alliance', 0)}`",
            f"Left/Kicked: `{counts.get('left_alliance', 0) + counts.get('kicked_from_alliance', 0)}`",
            f"Muted: `{counts.get('chat_ban_set', 0)}`  Unmuted: `{counts.get('chat_ban_removed', 0)}`",
        ])
        e.add_field(name="Members", value=members_val, inline=False)

        # Buildings
        buildings_val = NL.join([
            f"Constructed: `{counts.get('building_constructed', 0)}`  Destroyed: `{counts.get('building_destroyed', 0)}`",
            f"Expansions started: `{counts.get('extension_started', 0)}`  Expansions finished: `{counts.get('expansion_finished', 0)}`",
        ])
        e.add_field(name="Buildings", value=buildings_val, inline=False)

        # Missions & Events
        me_val = NL.join([
            f"Large missions started: `{counts.get('large_mission_started', 0)}`",
            f"Alliance events started: `{counts.get('alliance_event_started', 0)}`",
        ])
        e.add_field(name="Missions & Events", value=me_val, inline=False)

        # Training summary
        if training_on:
            ts = await self._training_summary(start_utc, end_utc)
            if ts["created"] or ts["completed"]:
                def fmt_map(d: Dict[str,int]) -> str:
                    items = sorted(d.items(), key=lambda x: (-x[1], x[0].lower()))
                    top = items[:5]
                    body = ", ".join([f"{k} `x{v}`" for k,v in top]) if top else "-"
                    if len(items) > 5:
                        rest = sum(v for _,v in items[5:])
                        body += f", others `x{rest}`"
                    return body or "-"
                tr_val = f"Created: {fmt_map(ts['created'])}{NL}Completed: {fmt_map(ts['completed'])}"
                e.add_field(name="Training", value=tr_val, inline=False)

        # Funds
        # Try to compute today earned/spent and current total via scraper cog if exposed; otherwise leave placeholders.
        funds_line = "Total funds today: `N/A`  Earned today: `N/A`  Spent today: `N/A`"
        try:
            sc = self.bot.get_cog("AllianceScraper")
            if sc and hasattr(sc, "get_funds_summary"):  # optional API
                fs = await sc.get_funds_summary(start_utc, end_utc)  # type: ignore
                if fs:
                    funds_line = f"Total funds today: `{fs.get('total','N/A')}`  Earned today: `{fs.get('earned','N/A')}`  Spent today: `{fs.get('spent','N/A')}`"
        except Exception:
            pass
        e.add_field(name="Alliance Funds", value=funds_line, inline=False)

        # Admin activity (top 3)
        if admin_on:
            top_admins = await self._admin_activity_top(start_utc, end_utc, top_n=3)
            if top_admins:
                lines = []
                for a in top_admins:
                    name = a["name"] or "Unknown"
                    mcid = a.get("mc_user_id") or None
                    link = _fmt_mc_profile(mcid, name)
                    did = await self._discord_id_for_mc(mcid) if mcid else None
                    if did:
                        link = f"{link} [[D]]({_discord_profile_url(did)})"
                    lines.append(f"{link} — `{a['count']} action(s)`")
                e.add_field(name="Admin Activity (Top 3)", value=NL.join(lines), inline=False)

        return e

    # ---- Posting ----
    async def _post_embed(self, channel_id: Optional[int], embed: Optional[discord.Embed]) -> bool:
        if not channel_id or not embed:
            return False
        guild = self.bot.guilds[0] if self.bot.guilds else None
        if not guild:
            return False
        ch = guild.get_channel(int(channel_id))
        if not isinstance(ch, discord.TextChannel):
            return False
        try:
            await ch.send(embed=embed)
            return True
        except Exception:
            return False

    # ---- Schedulers ----
    async def _daily_job(self, ny_now: datetime, real_post: bool = True) -> int:
        # Optional hot refresh before computing
        if await self.config.hot_refresh():
            try:
                await self._hot_refresh_members()
            except Exception:
                pass
        cfg = await self.config.all()
        ch_daily = cfg["channels"]["daily"]
        ch_over = cfg["channels"]["overview"]
        top_n = int(cfg.get("top_n", 10))

        e_top = await self._render_daily_top_embed(ny_now, top_n)
        e_over = await self._render_overview_embed(ny_now)
        posted = 0
        if real_post:
            if await self._post_embed(ch_daily, e_top):
                posted += 1
            if await self._post_embed(ch_over, e_over):
                posted += 1
            # mark published for day
            await self.config.last_published.daily.set(ny_now.strftime("%Y-%m-%d"))
        return posted

    async def _monthly_job(self, ny_now: datetime, real_post: bool = True) -> int:
        cfg = await self.config.all()
        ch_month = cfg["channels"]["monthly"]
        top_n = int(cfg.get("top_n", 10))
        e_month = await self._render_monthly_top_embed(ny_now, top_n)
        posted = 0
        if real_post:
            if await self._post_embed(ch_month, e_month):
                posted += 1
            await self.config.last_published.monthly.set(ny_now.strftime("%Y-%m"))
        return posted

    def _next_fire_after(self, tzname: str, time_str: str, now: Optional[datetime] = None) -> datetime:
        # time_str "HH:MM"; return next datetime in tz
        now = now or _tz_now(tzname)
        hh, mm = map(int, time_str.split(":"))
        candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if candidate <= now:
            candidate = candidate + timedelta(days=1)
        return candidate

    def _next_monthly_fire(self, tzname: str, time_str: str, now: Optional[datetime] = None) -> datetime:
        now = now or _tz_now(tzname)
        hh, mm = map(int, time_str.split(":"))
        # compute last day of current month
        if now.month == 12:
            first_next = now.replace(year=now.year+1, month=1, day=1, hour=hh, minute=mm, second=0, microsecond=0)
        else:
            first_next = now.replace(month=now.month+1, day=1, hour=hh, minute=mm, second=0, microsecond=0)
        last_this = first_next - timedelta(days=1)
        candidate = last_this.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if candidate <= now:
            # go to last day of next month
            n = first_next
            if n.month == 12:
                f2 = n.replace(year=n.year+1, month=1, day=1, hour=hh, minute=mm, second=0, microsecond=0)
            else:
                f2 = n.replace(month=n.month+1, day=1, hour=hh, minute=mm, second=0, microsecond=0)
            last_next = f2 - timedelta(days=1)
            candidate = last_next.replace(hour=hh, minute=mm, second=0, microsecond=0)
        return candidate

    async def _bg_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                cfg = await self.config.all()
                tz = cfg.get("timezone", NY_TZ)
                t_daily = cfg["times"]["daily"]
                t_month = cfg["times"]["monthly"]
                now_tz = _tz_now(tz)

                next_daily = self._next_fire_after(tz, t_daily, now_tz)
                next_monthly = self._next_monthly_fire(tz, t_month, now_tz)
                next_fire = min(next_daily, next_monthly)
                sleep_s = max(5, int((next_fire - now_tz).total_seconds()))
                await asyncio.sleep(sleep_s)

                # run whoever is due (tolerate up to 2 minutes drift)
                now_tz = _tz_now(tz)
                ran = 0
                if abs((now_tz.replace(second=0, microsecond=0) - next_daily).total_seconds()) <= 120:
                    # avoid duplicate in same NY date
                    last_day = await self.config.last_published.daily()
                    today_str = now_tz.strftime("%Y-%m-%d")
                    if last_day != today_str:
                        ran += await self._daily_job(now_tz, real_post=True)
                if abs((now_tz.replace(second=0, microsecond=0) - next_monthly).total_seconds()) <= 120:
                    last_mon = await self.config.last_published.monthly()
                    mon_str = now_tz.strftime("%Y-%m")
                    if last_mon != mon_str:
                        ran += await self._monthly_job(now_tz, real_post=True)
                if ran == 0:
                    await asyncio.sleep(20)
            except Exception:
                log.exception("DailyBriefing background loop error")
                await asyncio.sleep(30)

    async def _maybe_start_background(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._bg_loop())

    # ---- Commands ----
    @commands.group(name="briefing")
    @checks.admin_or_permissions(manage_guild=True)
    async def briefing_group(self, ctx: commands.Context):
        """Daily/Monthly briefing configuration and testing."""

    @briefing_group.command(name="version")
    async def version(self, ctx: commands.Context):
        await ctx.send(f"DailyBriefing version {__version__}")

    @briefing_group.command(name="status")
    async def status(self, ctx: commands.Context):
        cfg = await self.config.all()
        tz = cfg.get("timezone", NY_TZ)
        t_daily = cfg["times"]["daily"]
        t_month = cfg["times"]["monthly"]
        now_tz = _tz_now(tz)
        nd = self._next_fire_after(tz, t_daily, now_tz)
        nm = self._next_monthly_fire(tz, t_month, now_tz)
        lines = [
            "```",
            f"Timezone: {tz}  Now: {now_tz.strftime('%Y-%m-%d %H:%M')}",
            f"Daily at: {t_daily}  Next: {nd.strftime('%Y-%m-%d %H:%M')}",
            f"Monthly at: {t_month}  Next: {nm.strftime('%Y-%m-%d %H:%M')}",
            f"Channels: daily={cfg['channels']['daily']} monthly={cfg['channels']['monthly']} overview={cfg['channels']['overview']}",
            f"Last published: daily={cfg['last_published']['daily']} monthly={cfg['last_published']['monthly']}",
            f"TopN: {cfg['top_n']}  Hot refresh: {cfg['hot_refresh']}  Extras: {cfg['extras']}",
            "```"
        ]
        await ctx.send(NL.join(lines))

    @briefing_group.command(name="setchannel")
    async def setchannel(self, ctx: commands.Context, which: str, channel: discord.TextChannel):
        which = which.lower()
        if which not in {"daily","monthly","overview","adminlog"}:
            await ctx.send("Channel must be one of: daily, monthly, overview, adminlog")
            return
        cfg = await self.config.channels()
        cfg[which] = int(channel.id)
        await self.config.channels.set(cfg)
        await ctx.send(f"Channel for {which} set to {channel.mention}")

    @briefing_group.command(name="settime")
    async def settime(self, ctx: commands.Context, which: str, hhmm: str, tzname: str = NY_TZ):
        which = which.lower()
        if which not in {"daily","monthly"}:
            await ctx.send("Which must be daily or monthly")
            return
        if not re.match(r"^\d{2}:\d{2}$", hhmm):
            await ctx.send("Time must be HH:MM")
            return
        if which == "daily":
            t = await self.config.times()
            t["daily"] = hhmm
            await self.config.times.set(t)
        else:
            t = await self.config.times()
            t["monthly"] = hhmm
            await self.config.times.set(t)
        await self.config.zoneinfo.set(tzname) if hasattr(self.config, "zoneinfo") else None
        await self.config.timezone.set(tzname)
        await ctx.send(f"{which.capitalize()} time set to {hhmm} in {tzname}")

    @briefing_group.command(name="sethotrefresh")
    async def sethotrefresh(self, ctx: commands.Context, enabled: bool):
        await self.config.hot_refresh.set(bool(enabled))
        await ctx.send(f"Hot refresh set to {bool(enabled)}")

    @briefing_group.command(name="settopn")
    async def settopn(self, ctx: commands.Context, n: int):
        await self.config.top_n.set(max(1, int(n)))
        await ctx.send(f"TopN set to {max(1,int(n))}")

    @briefing_group.command(name="setextras")
    async def setextras(self, ctx: commands.Context, feature: str, enabled: bool):
        feature = feature.lower()
        if feature not in {"streaks","rising","admin","training"}:
            await ctx.send("Feature must be one of: streaks, rising, admin, training")
            return
        ex = await self.config.extras()
        ex[feature] = bool(enabled)
        await self.config.extras.set(ex)
        await ctx.send(f"Extra '{feature}' set to {bool(enabled)}")

    # ---- Test & run commands ----
    @briefing_group.group(name="daily")
    async def daily_grp(self, ctx: commands.Context):
        pass

    @briefing_group.group(name="monthly")
    async def monthly_grp(self, ctx: commands.Context):
        pass

    @daily_grp.command(name="preview")
    async def daily_preview(self, ctx: commands.Context, date_str: Optional[str] = None, topn: Optional[int] = None):
        tz = (await self.config.timezone()) or NY_TZ
        if date_str:
            ny_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo(tz))
        else:
            ny_date = _tz_now(tz)
        if topn is None:
            topn = int(await self.config.top_n())
        e_top = await self._render_daily_top_embed(ny_date, int(topn))
        e_over = await self._render_overview_embed(ny_date)
        if e_top:
            await ctx.send(embed=e_top)
        else:
            await ctx.send("No data for daily top.")
        if e_over:
            await ctx.send(embed=e_over)
        else:
            await ctx.send("No data for overview.")

    @monthly_grp.command(name="preview")
    async def monthly_preview(self, ctx: commands.Context, ym: Optional[str] = None, topn: Optional[int] = None):
        tz = (await self.config.timezone()) or NY_TZ
        ny_now = _tz_now(tz)
        if ym:
            y, m = ym.split("-")
            ny_now = ny_now.replace(year=int(y), month=int(m))
        if topn is None:
            topn = int(await self.config.top_n())
        e = await self._render_monthly_top_embed(ny_now, int(topn))
        if e:
            await ctx.send(embed=e)
        else:
            await ctx.send("No data for monthly top.")

    @daily_grp.command(name="runnow")
    async def daily_runnow(self, ctx: commands.Context):
        tz = (await self.config.timezone()) or NY_TZ
        ny_now = _tz_now(tz)
        n = await self._daily_job(ny_now, real_post=True)
        await ctx.send(f"Daily posted embeds: {n}")

    @monthly_grp.command(name="runnow")
    async def monthly_runnow(self, ctx: commands.Context):
        tz = (await self.config.timezone()) or NY_TZ
        ny_now = _tz_now(tz)
        n = await self._monthly_job(ny_now, real_post=True)
        await ctx.send(f"Monthly posted embeds: {n}")

    @briefing_group.command(name="dryrun")
    async def dryrun(self, ctx: commands.Context, which: str = "daily"):
        tz = (await self.config.timezone()) or NY_TZ
        ny_now = _tz_now(tz)
        if which.lower() == "monthly":
            n = await self._monthly_job(ny_now, real_post=False)
            await ctx.send(f"[DRYRUN] Would post monthly embeds: {n}")
        else:
            n = await self._daily_job(ny_now, real_post=False)
            await ctx.send(f"[DRYRUN] Would post daily embeds: {n}")

async def setup(bot):
    cog = DailyBriefing(bot)
    await bot.add_cog(cog)
