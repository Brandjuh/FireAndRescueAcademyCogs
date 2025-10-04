\
import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import datetime, date, time
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import discord
from redbot.core import commands, Config, checks

__version__ = "1.1.0"

NY_TZ_DEFAULT = "America/New_York"

@dataclass
class PlayerDelta:
    member_id: str
    name: str
    delta: int

class TopPlayers(commands.Cog):
    """Daily Top Players based on members_history credits deltas (NY day window)."""

    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0x70F10AD1, force_registration=True)
        defaults = {
            "db_path": "",
            "tz": NY_TZ_DEFAULT,
            "topn": 10
        }
        self.config.register_global(**defaults)

    # ------------- helpers -------------
    async def _get_cfg(self):
        db_path = await self.config.db_path()
        tzname = await self.config.tz()
        topn = await self.config.topn()
        return db_path, tzname or NY_TZ_DEFAULT, max(1, int(topn or 10))

    def _resolve_tz(self, tzname: str) -> ZoneInfo:
        try:
            return ZoneInfo(tzname)
        except Exception:
            return ZoneInfo(NY_TZ_DEFAULT)

    def _ny_bounds(self, tzname: str, day: Optional[date] = None) -> Tuple[str, str, date]:
        """Return (START_UTC_str, END_UTC_str, ny_date) where strings are 'YYYY-MM-DD HH:MM:SS'."""
        tz = self._resolve_tz(tzname)
        utc = ZoneInfo("UTC")
        if day is None:
            ny_now = datetime.now(tz)
            day = ny_now.date()
        start_ny = datetime.combine(day, time(0, 0), tzinfo=tz)
        end_ny = datetime.combine(day, time(23, 50), tzinfo=tz)
        start_utc = start_ny.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")
        end_utc = end_ny.astimezone(utc).strftime("%Y-%m-%d %H:%M:%S")
        return start_utc, end_utc, day

    def _connect(self, db_path: str) -> sqlite3.Connection:
        con = sqlite3.connect(db_path)
        con.row_factory = sqlite3.Row
        return con

    def _fetch_top_deltas(self, con: sqlite3.Connection, start_utc: str, end_utc: str, limit: int) -> List[PlayerDelta]:
        """
        Compute per-member credits delta between last snapshot < START and last snapshot <= END.
        Works directly on members_history (robust everywhere). Uses window functions.
        """
        sql = """
        WITH base AS (
          SELECT
            member_id,
            COALESCE(earned_credits, 0) AS earned_credits,
            substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) AS ts
          FROM members_history
          WHERE ts <= :END
        ),
        end_snap AS (
          SELECT member_id, earned_credits AS endc
          FROM (
            SELECT
              member_id, earned_credits,
              ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY ts DESC) AS rn
            FROM base
          )
          WHERE rn = 1
        ),
        start_base AS (
          SELECT
            member_id, earned_credits,
            substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) AS ts
          FROM members_history
          WHERE ts < :START
        ),
        start_snap AS (
          SELECT member_id, earned_credits AS startc
          FROM (
            SELECT
              member_id, earned_credits,
              ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY ts DESC) AS rn
            FROM start_base
          )
          WHERE rn = 1
        ),
        ids AS (
          SELECT member_id FROM end_snap
          UNION
          SELECT member_id FROM start_snap
        ),
        joined AS (
          SELECT
            ids.member_id,
            COALESCE(e.endc, 0)   AS endc,
            COALESCE(s.startc, 0) AS startc
          FROM ids
          LEFT JOIN end_snap   e USING(member_id)
          LEFT JOIN start_snap s USING(member_id)
        )
        SELECT
          j.member_id,
          COALESCE(mc.name, mh.name, j.member_id) AS name,
          (j.endc - j.startc) AS delta
        FROM joined j
        LEFT JOIN members_current mc ON mc.member_id = j.member_id
        LEFT JOIN (
          SELECT member_id, MAX(ts) AS max_ts, MAX(name) AS name
          FROM (
            SELECT
              member_id,
              substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) AS ts,
              name
            FROM members_history
          )
          GROUP BY member_id
        ) AS mh ON mh.member_id = j.member_id
        WHERE (j.endc - j.startc) > 0
        ORDER BY delta DESC
        LIMIT :LIM;
        """
        cur = con.execute(sql, {"START": start_utc, "END": end_utc, "LIM": int(limit)})
        rows = cur.fetchall()
        out: List[PlayerDelta] = []
        for r in rows:
            out.append(PlayerDelta(member_id=str(r["member_id"]), name=str(r["name"]), delta=int(r["delta"])))
        return out

    def _chunk(self, text: str, limit: int = 1024) -> list[str]:
        text = text or ""
        lines = text.split("\\n")
        chunks, cur = [], ""
        for ln in lines:
            if len(cur) + len(ln) + 1 > limit:
                if cur:
                    chunks.append(cur.rstrip())
                    cur = ""
                while len(ln) > limit:
                    chunks.append(ln[:limit])
                    ln = ln[limit:]
            cur += ln + "\\n"
        if cur:
            chunks.append(cur.rstrip())
        return chunks

    def _render_embed(self, ny_day: date, tzname: str, top: List[PlayerDelta], topn: int) -> List[discord.Embed]:
        desc = f"`{ny_day.isoformat()}` ({tzname})"
        if not top:
            e = discord.Embed(title="Daily Top Players", description=desc, color=discord.Color.blurple())
            e.add_field(name="Top 0", value="Geen data.", inline=False)
            return [e]

        lines = []
        for i, t in enumerate(top, start=1):
            lines.append(f"**{i}.** {t.name} — `+{t.delta:,} cr`")
        text = "\\n".join(lines)

        embeds: List[discord.Embed] = []
        for part in self._chunk(text, 1024):
            e = discord.Embed(title="Daily Top Players", description=desc, color=discord.Color.blurple())
            e.add_field(name=f"Top {min(topn, len(top))}", value=part or "\\u200b", inline=False)
            embeds.append(e)
        return embeds

    # ------------- commands -------------
    @commands.group(name="tplayers", aliases=["topplayers"])
    @checks.admin_or_permissions(manage_guild=True)
    async def tp_group(self, ctx: commands.Context):
        """Top Players configuration and preview."""
        pass

    @tp_group.command(name="setdb")
    async def set_db(self, ctx: commands.Context, path: str):
        """Set the sqlite DB path used for members_history/members_current."""
        await self.config.db_path.set(path)
        await ctx.send(f"DB pad ingesteld op:\\n`{path}`")

    @tp_group.command(name="settz")
    async def set_tz(self, ctx: commands.Context, tzname: str = NY_TZ_DEFAULT):
        """Set the IANA time zone (default America/New_York)."""
        await self.config.tz.set(tzname)
        await ctx.send(f"Tijdzone ingesteld op: `{tzname}`")

    @tp_group.command(name="settopn")
    async def set_topn(self, ctx: commands.Context, n: int = 10):
        """Set how many top players to show (default 10)."""
        await self.config.topn.set(max(1, int(n)))
        await ctx.send(f"TopN ingesteld op: `{max(1, int(n))}`")

    def _parse_day(self, arg: Optional[str], tzname: str) -> date:
        """Parse 'today'|'daily'|YYYY-MM-DD or DD-MM-YYYY -> NY date."""
        tz = self._resolve_tz(tzname)
        if not arg or arg.lower() in {"today", "daily", "vandaag"}:
            return datetime.now(tz).date()
        try:
            if "-" in arg:
                parts = arg.split("-")
                if len(parts[0]) == 2:
                    d, m, y = parts
                    return date(int(y), int(m), int(d))
                elif len(parts[0]) == 4:
                    y, m, d = parts
                    return date(int(y), int(m), int(d))
        except Exception:
            pass
        # fallback
        return datetime.now(tz).date()

    @tp_group.command(name="preview")
    async def preview(self, ctx: commands.Context, day: Optional[str] = None):
        """Preview daily top players for a given NY date (YYYY-MM-DD). Use 'daily' or no arg for today."""
        db_path, tzname, topn = await self._get_cfg()
        if not db_path:
            await ctx.send("DB pad staat nog niet. Gebruik `tplayers setdb <pad>`.")
            return

        ny_day = self._parse_day(day, tzname)
        start, end, ny_day = self._ny_bounds(tzname, ny_day)

        try:
            con = self._connect(db_path)
        except Exception as e:
            await ctx.send(f"Kan DB niet openen: `{e}`")
            return

        try:
            top = self._fetch_top_deltas(con, start, end, topn)
        except Exception as e:
            await ctx.send(f"Query-fout: `{e}`")
            return
        finally:
            try:
                con.close()
            except Exception:
                pass

        embeds = self._render_embed(ny_day, tzname, top, topn)
        for e in embeds:
            await ctx.send(embed=e)

    @tp_group.command(name="debug")
    async def debug(self, ctx: commands.Context, day: Optional[str] = None):
        """Show START/END and a sample of computed deltas for troubleshooting."""
        db_path, tzname, topn = await self._get_cfg()
        if not db_path:
            await ctx.send("DB pad staat nog niet. Gebruik `tplayers setdb <pad>`.")
            return

        ny_day = self._parse_day(day, tzname)
        start, end, ny_day = self._ny_bounds(tzname, ny_day)

        try:
            con = self._connect(db_path)
            cur = con.cursor()
            q1 = """
            SELECT COUNT(*) FROM (
              SELECT 1
              FROM members_history
              WHERE substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) <= :END
              LIMIT 1
            );"""
            q2 = """
            SELECT COUNT(*) FROM (
              SELECT 1
              FROM members_history
              WHERE substr(replace(COALESCE(snapshot_utc, scraped_at),'T',' '),1,19) < :START
              LIMIT 1
            );"""
            c1 = cur.execute(q1, {"END": end}).fetchone()[0]
            c2 = cur.execute(q2, {"START": start}).fetchone()[0]
            top = self._fetch_top_deltas(con, start, end, min(3, topn))
            sample = "\\n".join([f"- {t.name}: +{t.delta}" for t in top]) or "(geen)"
            await ctx.send(
                f"NY dag: `{ny_day.isoformat()}` ({tzname})\\n"
                f"START_UTC: `{start}`  END_UTC: `{end}`\\n"
                f"Snapshots ≤ END aanwezig? `{bool(c1)}`  Snapshots < START aanwezig? `{bool(c2)}`\\n"
                f"Top sample:\\n{sample}"
            )
        except Exception as e:
            await ctx.send(f"Debug error: `{e}`")
        finally:
            try:
                con.close()
            except Exception:
                pass
