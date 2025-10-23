# mc_textcad/cad.py
import asyncio
import random
import secrets
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import discord
from redbot.core import commands, Config

from .models import UnitType, UNIT_LABEL, Status
from .economy import EconomyBridge
from .util import now_ts, tier_weights, weighted_choice


GUILD_DEFAULT = {
    "lang": "EN",
    "mode": "shift",                # "shift" or "continuous"
    "interval_min": 30,
    "interval_max": 420,
    "ops_channel_id": 0,
    "ladder_channel_id": 0,
    "audit_channel_id": 0,
    "shift_active": False,
    "shift_end_ts": 0,
    "auto_close_min": 60,
    "duty_fee": 50,
    "reward_base": {"1": 100, "2": 200, "3": 400, "4": 800},
    "reward_speed_bonus": 10,
    "reward_team_bonus": 10,
    "leaderboards_enabled": True,
    "leaderboard_daily": True,
    "leaderboard_weekly": True,
    "rp_enabled": True,
}
MEMBER_DEFAULT = {
    "units": {},            # unit_id -> {type, status, joined_iid}
    "active_unit": "",
    "stats": {"payout_sum": 0, "incidents": 0, "first_arrivals": 0},
    "rp_per_incident": {},  # iid -> bool
}


@dataclass
class Participant:
    user_id: int
    unit_id: str
    unit_type: UnitType
    status: Status
    joined_ts: int = field(default_factory=now_ts)
    arrival_ts: Optional[int] = None
    transport_ts: Optional[int] = None
    clear_ts: Optional[int] = None


@dataclass
class Incident:
    iid: str
    name: str
    tier: int
    requirements: Dict[UnitType, int]
    created_ts: int = field(default_factory=now_ts)
    resolved_ts: Optional[int] = None
    participants: Dict[str, Participant] = field(default_factory=dict)  # key "uid:unit"
    channel_id: Optional[int] = None
    thread_id: Optional[int] = None

    def all_roles_present(self) -> bool:
        tally: Dict[UnitType, int] = {}
        for p in self.participants.values():
            tally[p.unit_type] = tally.get(p.unit_type, 0) + 1
        for u, n in self.requirements.items():
            if tally.get(u, 0) < n:
                return False
        return True

    def has_onscene(self) -> bool:
        return any(p.status in (Status.ONSCENE, Status.TRANSPORT, Status.CLEARING) for p in self.participants.values())

    def ready(self) -> bool:
        return self.all_roles_present() and self.has_onscene() and not self.resolved_ts


POOL = [
    ("Kitchen Fire", 1, {UnitType.ENGINE: 1}),
    ("Electrical Fire Indoors", 1, {UnitType.ENGINE: 1}),
    ("Gas Odor Inside", 1, {UnitType.ENGINE: 1, UnitType.COMMAND: 1}),
    ("Elevator Entrapment", 1, {UnitType.ENGINE: 1, UnitType.RESCUE: 1}),
    ("Residential Structure Fire", 2, {UnitType.ENGINE: 2, UnitType.LADDER: 1, UnitType.COMMAND: 1}),
    ("Smoke in High-Rise", 2, {UnitType.ENGINE: 2, UnitType.LADDER: 1, UnitType.COMMAND: 1}),
    ("Vehicle Accident w/ Entrapment", 2, {UnitType.ENGINE: 1, UnitType.RESCUE: 1, UnitType.EMS: 1, UnitType.COMMAND: 1}),
    ("Brush Fire Windy", 2, {UnitType.ENGINE: 2, UnitType.COMMAND: 1}),
    ("Water Rescue", 2, {UnitType.RESCUE: 1, UnitType.EMS: 1, UnitType.COMMAND: 1}),
    ("Working Fire (Upgraded)", 3, {UnitType.ENGINE: 3, UnitType.LADDER: 1, UnitType.RESCUE: 1, UnitType.COMMAND: 1}),
    ("Freeway Pile-Up", 3, {UnitType.ENGINE: 2, UnitType.EMS: 1, UnitType.RESCUE: 1, UnitType.COMMAND: 1}),
    ("Airport Hot Brakes", 2, {UnitType.ARFF: 1, UnitType.COMMAND: 1}),
    ("Aircraft Gear Malfunction", 3, {UnitType.ARFF: 2, UnitType.COMMAND: 1}),
    ("Bus Accident (MCI)", 3, {UnitType.EMS: 2, UnitType.ENGINE: 1, UnitType.COMMAND: 1}),
    ("HazMat Tanker Spill", 4, {UnitType.ENGINE: 2, UnitType.RESCUE: 1, UnitType.COMMAND: 1}),
    ("Industrial Fire", 4, {UnitType.ENGINE: 3, UnitType.LADDER: 1, UnitType.RESCUE: 1, UnitType.COMMAND: 1}),
]


class TextCAD(commands.Cog):
    def __init__(self, bot, config: Config, econ: EconomyBridge):
        self.bot = bot
        self.config = config
        self.econ = econ

        # Section accessors (used throughout)
        self.guild = self.config.guild
        self.member = self.config.member

        self._loops: Dict[int, asyncio.Task] = {}

        # ✅ Proper registration goes on the Config object
        self.config.register_guild(**GUILD_DEFAULT, incidents={})
        self.config.register_member(**MEMBER_DEFAULT)

    async def post_init(self):
        allg = await self.config.all_guilds()
        now = now_ts()
        for gid, g in allg.items():
            if g.get("mode", "shift") == "continuous" or (g.get("shift_active") and g.get("shift_end_ts", 0) > now):
                await self._start_loop(gid)

    async def stop_all_loops(self):
        for t in list(self._loops.values()):
            t.cancel()
        self._loops.clear()

    # channels
    async def _ops_channel(self, guild: discord.Guild):
        cid = await self.guild(guild).ops_channel_id()
        return guild.get_channel(cid) if cid else None

    async def _ladder_channel(self, guild: discord.Guild):
        cid = await self.guild(guild).ladder_channel_id()
        return guild.get_channel(cid) if cid else None

    async def _audit(self, guild: discord.Guild, text: str):
        cid = await self.guild(guild).audit_channel_id()
        ch = guild.get_channel(cid) if cid else None
        if ch:
            try:
                await ch.send(f"[AUDIT] {text}")
            except Exception:
                pass

    # persistence
    def _mk_unit_id(self, ut: UnitType) -> str:
        prefix = {
            "engine": "ENG",
            "ladder": "LAD",
            "rescue": "RES",
            "ems": "EMS",
            "patrol": "LEO",
            "arff": "ARF",
            "tow": "TOW",
            "command": "CMD",
        }[ut.value]
        return f"{prefix}-{secrets.token_hex(2).upper()}"

    def _deserialize_inc(self, data: dict) -> Incident:
        req = {UnitType(k): v for k, v in data["requirements"].items()}
        inc = Incident(
            iid=data["iid"],
            name=data["name"],
            tier=int(data["tier"]),
            requirements=req,
            created_ts=data["created_ts"],
            resolved_ts=data.get("resolved_ts"),
            channel_id=data.get("channel_id"),
            thread_id=data.get("thread_id"),
        )
        for key, pd in (data.get("participants") or {}).items():
            inc.participants[key] = Participant(
                user_id=int(pd["user_id"]),
                unit_id=pd["unit_id"],
                unit_type=UnitType(pd["unit_type"]),
                status=Status(pd["status"]),
                joined_ts=pd.get("joined_ts", 0),
                arrival_ts=pd.get("arrival_ts"),
                transport_ts=pd.get("transport_ts"),
                clear_ts=pd.get("clear_ts"),
            )
        return inc

    async def _persist_inc(self, guild: discord.Guild, inc: Incident):
        gi = await self.guild(guild).incidents()
        gi[inc.iid] = {
            "iid": inc.iid,
            "name": inc.name,
            "tier": int(inc.tier),
            "requirements": {u.value: n for u, n in inc.requirements.items()},
            "created_ts": inc.created_ts,
            "resolved_ts": inc.resolved_ts,
            "participants": {
                key: {
                    "user_id": p.user_id,
                    "unit_id": p.unit_id,
                    "unit_type": p.unit_type.value,
                    "status": p.status.value,
                    "joined_ts": p.joined_ts,
                    "arrival_ts": p.arrival_ts,
                    "transport_ts": p.transport_ts,
                    "clear_ts": p.clear_ts,
                }
                for key, p in inc.participants.items()
            },
            "channel_id": inc.channel_id,
            "thread_id": inc.thread_id,
        }
        await self.guild(guild).incidents.set(gi)

    async def _count_on_duty_units(self, guild: discord.Guild) -> int:
        total = 0
        allm = await self.config.all_members(guild)
        for m in allm.values():
            for u in (m.get("units") or {}).values():
                if u.get("status") and u["status"] != Status.OFFDUTY.value:
                    total += 1
        return total

    async def _spawn_incident(self, guild: discord.Guild):
        ops = await self._ops_channel(guild)
        if not ops:
            return
        on_duty = await self._count_on_duty_units(guild)
        w = tier_weights(on_duty)
        rng = random.Random()
        tier_idx = weighted_choice(rng, list(w))
        tier = tier_idx + 1
        cands = [x for x in POOL if x[1] == tier] or POOL
        name, t, req = random.choice(cands)
        iid = secrets.token_hex(3).upper()
        inc = Incident(iid=iid, name=name, tier=t, requirements=req, channel_id=ops.id)

        embed = discord.Embed(title=f"🚨 Incident: {inc.name} • {iid} (T{t})", color=discord.Color.orange())
        need = ", ".join(f"{n}×{UNIT_LABEL[u]}" for u, n in req.items())
        embed.add_field(name="Needs", value=need or "—", inline=False)
        embed.set_footer(text="Use /dispatch join <id> to respond")
        msg = await ops.send(embed=embed)
        thread = await ops.create_thread(name=f"{inc.name} • {iid}", message=msg)
        inc.thread_id = thread.id

        await self._persist_inc(guild, inc)
        await self._audit(guild, f"incident_create {iid} '{inc.name}' T{t}")

    async def _try_resolve(self, guild: discord.Guild, iid: str):
        gi = await self.guild(guild).incidents()
        data = gi.get(iid)
        if not data or data.get("resolved_ts"):
            return
        inc = self._deserialize_inc(data)

        # timeout
        ttl = await self.guild(guild).auto_close_min()
        if not inc.ready() and now_ts() - inc.created_ts > ttl * 60:
            inc.resolved_ts = now_ts()
            gi[iid]["resolved_ts"] = inc.resolved_ts
            await self.guild(guild).incidents.set(gi)
            thread = guild.get_thread(inc.thread_id) if inc.thread_id else None
            if thread:
                await thread.send(f"❌ Incident **{inc.name}** • {iid} timed out (no resolution).")
            await self._audit(guild, f"incident_timeout {iid}")
            return

        if not inc.ready():
            return

        # resolve + payouts
        inc.resolved_ts = now_ts()
        gi[iid]["resolved_ts"] = inc.resolved_ts
        await self.guild(guild).incidents.set(gi)

        base = (await self.guild(guild).reward_base()).get(str(int(inc.tier)), 100)
        speed = await self.guild(guild).reward_speed_bonus()
        team = await self.guild(guild).reward_team_bonus()
        team_ok = inc.all_roles_present()

        lines = []
        for key, p in inc.participants.items():
            member = guild.get_member(p.user_id)
            if not member:
                continue
            class Ctx:
                def __init__(self, g, m):
                    self.guild = g
                    self.author = m
            payout = base
            if p.arrival_ts and (p.arrival_ts - p.joined_ts) <= 120:
                payout += speed
            if team_ok:
                payout += team
            ok, msg = await self.econ.deposit(Ctx(guild, member), payout)
            lines.append(f"<@{p.user_id}> +{payout}" + ("" if ok else f" (fail: {msg})"))
            mconf = self.member(member)
            stats = await mconf.stats()
            stats["payout_sum"] = stats.get("payout_sum", 0) + max(0, payout)
            stats["incidents"] = stats.get("incidents", 0) + 1
            await mconf.stats.set(stats)

        thread = guild.get_thread(inc.thread_id) if inc.thread_id else None
        if thread:
            await thread.send("✅ **Resolved** • {} • {}\nPayouts: {}".format(inc.name, iid, ", ".join(lines)))
        await self._audit(guild, f"incident_resolve {iid}")

        # lazy leaderboard post (snapshot)
        await self._post_leaderboard_snapshot(guild)

    async def _post_leaderboard_snapshot(self, guild: discord.Guild):
        if not await self.guild(guild).leaderboards_enabled():
            return
        ch = await self._ladder_channel(guild)
        if not ch:
            return
        allm = await self.config.all_members(guild)
        rows = []
        for uid, m in allm.items():
            s = m.get("stats") or {}
            rows.append((int(uid), s.get("payout_sum", 0), s.get("incidents", 0), s.get("first_arrivals", 0)))
        rows.sort(key=lambda r: (r[1], r[2], r[3]), reverse=True)
        lines = ["**Leaderboard (Total)**", "Rank | User | Payout | Incidents | First-arrivals"]
        for i, (uid, pay, incs, fa) in enumerate(rows[:10], start=1):
            lines.append(f"{i}. <@{uid}> • {pay} • {incs} • {fa}")
        try:
            await ch.send("\n".join(lines))
        except Exception:
            pass

    # scheduler
    async def _start_loop(self, guild_id: int):
        if guild_id in self._loops:
            return
        self._loops[guild_id] = self.bot.loop.create_task(self._runner(guild_id))

    async def _stop_loop(self, guild_id: int):
        t = self._loops.pop(guild_id, None)
        if t:
            t.cancel()

    async def _runner(self, guild_id: int):
        try:
            guild = self.bot.get_guild(guild_id)
            rng = random.Random()
            while True:
                g = await self.guild(guild).all()
                mode = g.get("mode", "shift")
                active = g.get("shift_active") if mode == "shift" else True
                if not active:
                    await asyncio.sleep(1)
                    continue
                if mode == "shift" and g.get("shift_end_ts", 0) <= now_ts():
                    await self.guild(guild).shift_active.set(False)
                    break
                await self._spawn_incident(guild)
                gi = await self.guild(guild).incidents()
                for iid in list(gi.keys()):
                    await self._try_resolve(guild, iid)
                delay = rng.uniform(float(g.get("interval_min", 30)), float(g.get("interval_max", 420)))
                await asyncio.sleep(max(15.0, delay))
        except asyncio.CancelledError:
            pass

    # ---- command helpers
    async def _select_unit_for_action(self, ctx: commands.Context) -> Optional[Tuple[str, dict]]:
        mid = self.member(ctx.author)
        units = await mid.units()
        if not units:
            await ctx.reply("You have no units on duty.", ephemeral=True)
            return None
        unit_id = await mid.active_unit()
        if not unit_id or unit_id not in units:
            unit_id = next(iter(units.keys()))
        return unit_id, units[unit_id]

    # ---- /dispatch commands
    @commands.hybrid_group(name="dispatch")
    async def dispatch(self, ctx: commands.Context):
        """Text CAD: on duty, respond, resolve, earn credits."""
        pass

    @dispatch.command(name="setup")
    @commands.admin()
    async def setup(
        self,
        ctx: commands.Context,
        ops: discord.TextChannel,
        ladder: Optional[discord.TextChannel] = None,
        audit: Optional[discord.TextChannel] = None,
        mode: Optional[str] = "shift",
        interval_min: Optional[int] = 30,
        interval_max: Optional[int] = 420,
        auto_close_min: Optional[int] = 60,
    ):
        await self.guild(ctx.guild).ops_channel_id.set(ops.id)
        await self.guild(ctx.guild).ladder_channel_id.set(ladder.id if ladder else 0)
        await self.guild(ctx.guild).audit_channel_id.set(audit.id if audit else 0)
        await self.guild(ctx.guild).mode.set("continuous" if str(mode).lower().startswith("cont") else "shift")
        await self.guild(ctx.guild).interval_min.set(max(15, int(interval_min or 30)))
        await self.guild(ctx.guild).interval_max.set(max(30, int(interval_max or 420)))
        await self.guild(ctx.guild).auto_close_min.set(max(10, int(auto_close_min or 60)))
        await ctx.reply("Configured.")

    @dispatch.command(name="shiftstart")
    @commands.admin()
    async def shiftstart(self, ctx: commands.Context, duration_min: int = 60):
        if (await self.guild(ctx.guild).mode()) != "shift":
            await ctx.reply("Mode is not 'shift'.", ephemeral=True)
            return
        end_ts = now_ts() + max(5 * 60, duration_min * 60)
        await self.guild(ctx.guild).shift_active.set(True)
        await self.guild(ctx.guild).shift_end_ts.set(end_ts)
        await self._start_loop(ctx.guild.id)
        await ctx.reply(f"Shift started until <t:{end_ts}:t>.")

    @dispatch.command(name="shiftstop")
    @commands.admin()
    async def shiftstop(self, ctx: commands.Context):
        await self.guild(ctx.guild).shift_active.set(False)
        await self.guild(ctx.guild).shift_end_ts.set(0)
        await self._stop_loop(ctx.guild.id)
        await ctx.reply("Shift stopped.")

    @dispatch.command(name="on")
    async def on(self, ctx: commands.Context, unit: str):
        try:
            ut = UnitType(unit.lower())
        except Exception:
            await ctx.reply("Unknown unit type.", ephemeral=True)
            return
        fee = await self.guild(ctx.guild).duty_fee()
        ok, msg = await self.econ.withdraw(ctx, fee)
        if not ok:
            await ctx.reply(f"Cannot go on duty: {msg}", ephemeral=True)
            return
        mid = self.member(ctx.author)
        units = await mid.units()
        unit_id = self._mk_unit_id(ut)
        units[unit_id] = {"type": ut.value, "status": Status.AVAILABLE.value, "joined_iid": ""}
        await mid.units.set(units)
        await mid.active_unit.set(unit_id)
        await ctx.reply(f"On duty as **{UNIT_LABEL[ut]}** (`{unit_id}`). Fee paid.")

    @dispatch.command(name="off")
    async def off(self, ctx: commands.Context, unit_id: Optional[str] = None):
        mid = self.member(ctx.author)
        units = await mid.units()
        if not units:
            await ctx.reply("You have no active units.", ephemeral=True)
            return
        if unit_id and unit_id in units:
            del units[unit_id]
        else:
            units = {}
        await mid.units.set(units)
        await mid.active_unit.set("")
        await ctx.reply("Off duty.")

    @dispatch.command(name="status")
    async def status(self, ctx: commands.Context, to: str, unit_id: Optional[str] = None):
        try:
            s = Status(to.lower())
        except Exception:
            await ctx.reply("Unknown status.", ephemeral=True)
            return
        mid = self.member(ctx.author)
        units = await mid.units()
        if not units:
            await ctx.reply("You have no units.", ephemeral=True)
            return
        if not unit_id or unit_id not in units:
            unit_id = (await mid.active_unit()) or next(iter(units.keys()))
        units[unit_id]["status"] = s.value
        await mid.units.set(units)
        await mid.active_unit.set(unit_id)
        await ctx.reply(f"Status of `{unit_id}` set to **{s.value}**.")

    @dispatch.command(name="list")
    async def list(self, ctx: commands.Context):
        gi = await self.guild(ctx.guild).incidents()
        openi = [d for d in gi.values() if not d.get("resolved_ts")]
        if not openi:
            await ctx.reply("No open incidents.")
            return

        def label(k):
            return {
                "engine": "Engine",
                "ladder": "Ladder",
                "rescue": "Rescue/USAR",
                "ems": "EMS",
                "patrol": "Patrol/LEO",
                "arff": "ARFF",
                "tow": "Tow",
                "command": "Command/BC",
            }[k]

        lines = [
            f"**{d['iid']}** — {d['name']} (T{d['tier']}) • Needs: "
            + ", ".join(f"{n}×{label(k)}" for k, n in d["requirements"].items())
            for d in openi[:20]
        ]
        await ctx.reply("\n".join(lines))

    @dispatch.command(name="join")
    async def join(self, ctx: commands.Context, iid: str):
        gi = await self.guild(ctx.guild).incidents()
        data = gi.get(iid)
        if not data or data.get("resolved_ts"):
            await ctx.reply("Incident not found or already resolved.", ephemeral=True)
            return
        sel = await self._select_unit_for_action(ctx)
        if not sel:
            return
        unit_id, unit = sel
        inc = self._deserialize_inc(data)
        key = f"{ctx.author.id}:{unit_id}"
        p = inc.participants.get(key)
        if not p:
            p = Participant(
                user_id=ctx.author.id,
                unit_id=unit_id,
                unit_type=UnitType(unit["type"]),
                status=Status.ENROUTE,
            )
        p.status = Status.ENROUTE
        p.joined_ts = now_ts()
        inc.participants[key] = p
        await self._persist_inc(ctx.guild, inc)

        mid = self.member(ctx.author)
        units = await mid.units()
        units[unit_id]["status"] = Status.ENROUTE.value
        units[unit_id]["joined_iid"] = iid
        await mid.units.set(units)
        await mid.active_unit.set(unit_id)

        await ctx.reply(f"`{unit_id}` enroute to **{inc.name}** ({iid}).")
        await self._audit(ctx.guild, f"join {iid} {ctx.author.id}:{unit_id}")

    @dispatch.command(name="arrive")
    async def arrive(self, ctx: commands.Context):
        sel = await self._select_unit_for_action(ctx)
        if not sel:
            return
        unit_id, unit = sel
        iid = unit.get("joined_iid") or ""
        if not iid:
            await ctx.reply("Your active unit is not assigned to an incident.", ephemeral=True)
            return
        gi = await self.guild(ctx.guild).incidents()
        data = gi.get(iid)
        inc = self._deserialize_inc(data)
        key = f"{ctx.author.id}:{unit_id}"
        p = inc.participants.get(key)
        if not p:
            await ctx.reply("This unit is not registered on that incident.", ephemeral=True)
            return

        # mark arrival
        first_before = any(pp.arrival_ts for pp in inc.participants.values())
        p.status = Status.ONSCENE
        p.arrival_ts = now_ts()
        await self._persist_inc(ctx.guild, inc)

        # member state
        mid = self.member(ctx.author)
        units = await mid.units()
        units[unit_id]["status"] = Status.ONSCENE.value
        await mid.units.set(units)

        # first-arrival stat
        if not first_before:
            stats = await mid.stats()
            stats["first_arrivals"] = stats.get("first_arrivals", 0) + 1
            await mid.stats.set(stats)

        await ctx.reply(f"`{unit_id}` arrived at **{inc.name}** ({iid}).")
        await self._audit(ctx.guild, f"arrive {iid} {ctx.author.id}:{unit_id}")
        await self._try_resolve(ctx.guild, iid)

    @dispatch.command(name="transport")
    async def transport(self, ctx: commands.Context):
        sel = await self._select_unit_for_action(ctx)
        if not sel:
            return
        unit_id, unit = sel
        iid = unit.get("joined_iid") or ""
        if not iid:
            await ctx.reply("Your active unit is not assigned to an incident.", ephemeral=True)
            return
        gi = await self.guild(ctx.guild).incidents()
        data = gi.get(iid)
        inc = self._deserialize_inc(data)
        key = f"{ctx.author.id}:{unit_id}"
        p = inc.participants.get(key)
        if not p:
            await ctx.reply("This unit is not registered on that incident.", ephemeral=True)
            return
        if p.unit_type != UnitType.EMS:
            await ctx.reply("Only EMS units can mark transport.", ephemeral=True)
            return
        p.status = Status.TRANSPORT
        p.transport_ts = now_ts()
        await self._persist_inc(ctx.guild, inc)

        mid = self.member(ctx.author)
        units = await mid.units()
        units[unit_id]["status"] = Status.TRANSPORT.value
        await mid.units.set(units)

        await ctx.reply(f"`{unit_id}` is transporting from **{inc.name}** ({iid}).")
        await self._audit(ctx.guild, f"transport {iid} {ctx.author.id}:{unit_id}")
        await self._try_resolve(ctx.guild, iid)

    @dispatch.command(name="clear")
    async def clear(self, ctx: commands.Context):
        sel = await self._select_unit_for_action(ctx)
        if not sel:
            return
        unit_id, unit = sel
        iid = unit.get("joined_iid") or ""
        if not iid:
            await ctx.reply("Your active unit is not assigned to an incident.", ephemeral=True)
            return
        gi = await self.guild(ctx.guild).incidents()
        data = gi.get(iid)
        inc = self._deserialize_inc(data)
        key = f"{ctx.author.id}:{unit_id}"
        p = inc.participants.get(key)
        if not p:
            await ctx.reply("This unit is not registered on that incident.", ephemeral=True)
            return
        p.status = Status.CLEARING
        p.clear_ts = now_ts()
        await self._persist_inc(ctx.guild, inc)

        mid = self.member(ctx.author)
        units = await mid.units()
        units[unit_id]["status"] = Status.AVAILABLE.value
        units[unit_id]["joined_iid"] = ""
        await mid.units.set(units)

        await ctx.reply(f"`{unit_id}` cleared from **{inc.name}** ({iid}).")
        await self._audit(ctx.guild, f"clear {iid} {ctx.author.id}:{unit_id}")
        await self._try_resolve(ctx.guild, iid)

    @dispatch.command(name="leave")
    async def leave(self, ctx: commands.Context):
        sel = await self._select_unit_for_action(ctx)
        if not sel:
            return
        unit_id, unit = sel
        iid = unit.get("joined_iid") or ""
        if not iid:
            await ctx.reply("Your active unit is not assigned to an incident.", ephemeral=True)
            return
        gi = await self.guild(ctx.guild).incidents()
        data = gi.get(iid)
        inc = self._deserialize_inc(data)
        key = f"{ctx.author.id}:{unit_id}"
        if key in inc.participants:
            del inc.participants[key]
        await self._persist_inc(ctx.guild, inc)

        mid = self.member(ctx.author)
        units = await mid.units()
        units[unit_id]["status"] = Status.AVAILABLE.value
        units[unit_id]["joined_iid"] = ""
        await mid.units.set(units)

        await ctx.reply(f"`{unit_id}` left **{inc.name}** ({iid}).")
        await self._audit(ctx.guild, f"leave {iid} {ctx.author.id}:{unit_id}")
        await self._try_resolve(ctx.guild, iid)
