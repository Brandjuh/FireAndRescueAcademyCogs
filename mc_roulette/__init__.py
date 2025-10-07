# mc_roulette: Incident Roulette game for Red-Discord-Bot
from __future__ import annotations
from redbot.core import commands, Config
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import box
from .economy import EconomyBridge
from .roulette import (
    CallSpec, CallPool, generate_run, score_run, is_perfect_run,
    RouletteView, now_utc_ts
)
import discord
import datetime
import secrets

__red_end_user_data_statement__ = (
    "This cog stores per-user game counters and active run state. "
    "It does not store message content beyond state."
)

GUILD_DEFAULT = {
    "ir_cost_per_play": 50,
    "ir_reward_per_point": 2,
    "ir_bonus_perfect": 10,
    "ir_daily_limit": 1,
    "allow_dupes": False,
    "hard_mode": False,
    "weekly_payout_cap": 10000,
}

MEMBER_DEFAULT = {
    "plays_today": 0,
    "last_play_date": "",
    "weekly_payout": 0,
    "weekly_reset": "",
    "lifetime_points": 0,
    "lifetime_runs": 0,
    "active_run": {},
}

def iso_week_tag(dt: datetime.date) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

class McRoulette(commands.Cog):
    """Incident Roulette — economy-enabled, seed-based, zero-mod mini-game."""

    def __init__(self, bot: Red) -> None:
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xD00D5EED, force_registration=True)
        self.config.register_guild(**GUILD_DEFAULT)
        self.config.register_member(**MEMBER_DEFAULT)
        self.econ = EconomyBridge()
        self.pool = CallPool.default_pool()

    async def _reset_daily_if_needed(self, member_conf: Config.member) -> None:
        today = datetime.date.today().isoformat()
        data = await member_conf.all()
        if data.get("last_play_date") != today:
            await member_conf.plays_today.set(0)
            await member_conf.last_play_date.set(today)

    async def _reset_weekly_if_needed(self, member_conf: Config.member) -> None:
        tag = iso_week_tag(datetime.date.today())
        data = await member_conf.all()
        if data.get("weekly_reset") != tag:
            await member_conf.weekly_payout.set(0)
            await member_conf.weekly_reset.set(tag)

    async def _ensure_no_active(self, ctx: commands.Context) -> bool:
        run = await self.config.member(ctx.author).active_run()
        return not bool(run)

    async def _charge_and_register(self, ctx: commands.Context) -> tuple[bool, str]:
        guild = await self.config.guild(ctx.guild).all()
        mem = self.config.member(ctx.author)
        await self._reset_daily_if_needed(mem)
        await self._reset_weekly_if_needed(mem)
        if (await mem.plays_today()) >= guild["ir_daily_limit"]:
            return False, f"Daglimiet bereikt ({guild['ir_daily_limit']})."
        ok, msg = await self.econ.withdraw(ctx, guild["ir_cost_per_play"])
        if not ok:
            return False, f"Onvoldoende saldo: {msg}"
        await mem.plays_today.set((await mem.plays_today()) + 1)
        return True, ""

    async def _payout(self, ctx: commands.Context, points: int) -> str:
        guild = await self.config.guild(ctx.guild).all()
        mem = self.config.member(ctx.author)
        await self._reset_weekly_if_needed(mem)
        payout = points * guild["ir_reward_per_point"]
        cap_left = max(0, guild["weekly_payout_cap"] - (await mem.weekly_payout()))
        payout = min(payout, cap_left)
        if payout <= 0:
            await mem.lifetime_points.set((await mem.lifetime_points()) + points)
            await mem.lifetime_runs.set((await mem.lifetime_runs()) + 1)
            return "Wekelijkse payout cap bereikt; geen uitbetaling."
        ok, msg = await self.econ.deposit(ctx, payout)
        if ok:
            await mem.weekly_payout.set((await mem.weekly_payout()) + payout)
            await mem.lifetime_points.set((await mem.lifetime_points()) + points)
            await mem.lifetime_runs.set((await mem.lifetime_runs()) + 1)
            return f"Uitbetaald: {self.econ.format_amount(ctx.guild, payout)}."
        return f"Kon niet uitbetalen: {msg}"

    @commands.hybrid_group(name="roulette")
    async def roulette(self, ctx: commands.Context):
        """Incident Roulette minigame."""
        pass

    @roulette.command(name="start")
    async def roulette_start(self, ctx: commands.Context):
        """Start een run (3 calls). Afschrijving bij start. UI is alleen voor de starter."""
        if not await self._ensure_no_active(ctx):
            await ctx.reply("Je hebt al een actieve run. Gebruik `/roulette claim` of `/roulette cancel`.", ephemeral=True)
            return
        can, msg = await self._charge_and_register(ctx)
        if not can:
            await ctx.reply(msg, ephemeral=True)
            return
        guild_conf = await self.config.guild(ctx.guild).all()
        seed = secrets.token_hex(2).upper()
        calls = generate_run(seed, self.pool, allow_dupes=guild_conf["allow_dupes"], hard_mode=guild_conf["hard_mode"])
        state = {
            "seed": seed,
            "calls": [c.to_json() for c in calls],
            "allocs": {str(i): {} for i in range(len(calls))},
            "per_call_time_s": [None] * len(calls),
            "started_at": now_utc_ts(),
            "expires_at": now_utc_ts() + 15*60,
            "current_idx": 0,
        }
        await self.config.member(ctx.author).active_run.set(state)

        embed = discord.Embed(
            title=f"Incident Roulette • Seed {seed}",
            description=("Je krijgt 3 calls. Wijs rollen toe. 60s soft timer per call. " 
                         "Perfecte match geeft bonus. Claim om te scoren en uitbetaling te krijgen."),
            color=discord.Color.blurple(),
        )
        embed.add_field(
            name="Calls",
            value="\n".join(f"{i+1}. {calls[i].name} • Req: {calls[i].requirements_str()}" for i in range(3)),
            inline=False
        )
        embed.set_footer(text=f"TTL 15m • Alleen {ctx.author.display_name} kan klikken")
        view = RouletteView(self, ctx, state, only_user_id=ctx.author.id)
        await ctx.reply(embed=embed, view=view, ephemeral=True)

    @roulette.command(name="claim")
    async def roulette_claim(self, ctx: commands.Context):
        """Boek score op je actieve run en ontvang payout."""
        state = await self.config.member(ctx.author).active_run()
        if not state:
            await ctx.reply("Geen actieve run.", ephemeral=True)
            return
        calls = [CallSpec.from_json(d) for d in state["calls"]]
        points, per_call = score_run(calls, state)
        guild_conf = await self.config.guild(ctx.guild).all()
        perfect_note = ""
        if is_perfect_run(calls, state):
            points += guild_conf["ir_bonus_perfect"]
            perfect_note = f" + perfect bonus {guild_conf['ir_bonus_perfect']}"
        payout_msg = await self._payout(ctx, points)
        await self.config.member(ctx.author).active_run.set({})

        lines = []
        for i, (p, detail) in enumerate(per_call):
            lines.append(f"Call {i+1}: {detail} → **{p}**")
        total_line = f"Totaal: **{points}** punten{perfect_note}. {payout_msg}"
        await ctx.send(f"{ctx.author.mention} Incident Roulette afgerond.\n" + "\n".join(lines) + f"\n{total_line}")

    @roulette.command(name="cancel")
    async def roulette_cancel(self, ctx: commands.Context):
        """Annuleer je actieve run (geen refund)."""
        state = await self.config.member(ctx.author).active_run()
        if not state:
            await ctx.reply("Geen actieve run.", ephemeral=True)
            return
        await self.config.member(ctx.author).active_run.set({})
        await ctx.reply("Run geannuleerd. Geen refund.", ephemeral=True)

    @commands.hybrid_group(name="roulset")
    @commands.admin()
    async def roulset(self, ctx: commands.Context):
        """Instellingen voor Incident Roulette."""
        pass

    @roulset.command(name="cost")
    async def roulset_cost(self, ctx: commands.Context, amount: int):
        await self.config.guild(ctx.guild).ir_cost_per_play.set(max(0, amount))
        await ctx.reply(f"Kosten per run: {amount}")

    @roulset.command(name="reward")
    async def roulset_reward(self, ctx: commands.Context, per_point: int, perfect_bonus: int):
        await self.config.guild(ctx.guild).ir_reward_per_point.set(max(0, per_point))
        await self.config.guild(ctx.guild).ir_bonus_perfect.set(max(0, perfect_bonus))
        await ctx.reply(f"Beloning per punt: {per_point}, perfect bonus: {perfect_bonus}")

    @roulset.command(name="limit")
    async def roulset_limit(self, ctx: commands.Context, runs_per_day: int, weekly_cap: int):
        await self.config.guild(ctx.guild).ir_daily_limit.set(max(1, runs_per_day))
        await self.config.guild(ctx.guild).weekly_payout_cap.set(max(0, weekly_cap))
        await ctx.reply(f"Daglimiet: {runs_per_day}, wekelijkse payout cap: {weekly_cap}")

    @roulset.command(name="flags")
    async def roulset_flags(self, ctx: commands.Context, allow_dupes: bool, hard_mode: bool):
        await self.config.guild(ctx.guild).allow_dupes.set(bool(allow_dupes))
        await self.config.guild(ctx.guild).hard_mode.set(bool(hard_mode))
        await ctx.reply(f"allow_dupes={allow_dupes}, hard_mode={hard_mode}")

async def setup(bot: Red):
    await bot.add_cog(McRoulette(bot))
