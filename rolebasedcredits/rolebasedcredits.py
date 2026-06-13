from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Optional

import discord
from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import pagify

log = logging.getLogger("red.fara.rolebasedcredits")


@dataclass(frozen=True)
class CreditRank:
    key: str
    name: str
    min_credits: int


CREDIT_RANKS: tuple[CreditRank, ...] = (
    CreditRank("probie", "Probie", 0),
    CreditRank("firefighter", "Firefighter", 200),
    CreditRank("senior_firefighter", "Senior Firefighter", 10_000),
    CreditRank("fire_apparatus_operator", "Fire Apparatus Operator", 100_000),
    CreditRank("lieutenant", "Lieutenant", 1_000_000),
    CreditRank("captain", "Captain", 5_000_000),
    CreditRank("staff_captain", "Staff Captain", 20_000_000),
    CreditRank("battalion_chief", "Battalion Chief", 50_000_000),
    CreditRank("division_chief", "Division Chief", 1_000_000_000),
    CreditRank("deputy_chief", "Deputy Chief", 2_000_000_000),
    CreditRank("fire_chief", "Fire Chief", 5_000_000_000),
    CreditRank("fire_commissioner", "Fire Commissioner", 10_000_000_000),
)

RANKS_BY_KEY = {rank.key: rank for rank in CREDIT_RANKS}

DEFAULT_RANK_ROLE_IDS = {
    "probie": 669488072911618048,
    "firefighter": 669488631811014657,
    "senior_firefighter": 669488681639346187,
    "fire_apparatus_operator": 669488729060147202,
    "lieutenant": 669488786480300062,
    "captain": 669488849780473856,
    "staff_captain": 669488888468733981,
    "battalion_chief": 669488934140641290,
    "division_chief": 669488982199107595,
    "deputy_chief": 669489030202916884,
    "fire_chief": 669489070166114314,
    "fire_commissioner": 1437513734364069940,
}

DEFAULT_GUILD = {
    "enabled": False,
    "sync_interval_minutes": 60,
    "rank_role_ids": DEFAULT_RANK_ROLE_IDS,
    "promotion_channel_id": 543935264708362251,
    "announce_first_assignment": False,
    "baseline_initialized": False,
    "last_rank_by_discord_id": {},
}


def normalize_rank_lookup(value: str) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def find_rank(value: str) -> Optional[CreditRank]:
    needle = normalize_rank_lookup(value)
    for rank in CREDIT_RANKS:
        if needle in {normalize_rank_lookup(rank.key), normalize_rank_lookup(rank.name)}:
            return rank
    return None


def rank_for_credits(credits: int) -> CreditRank:
    selected = CREDIT_RANKS[0]
    for rank in CREDIT_RANKS:
        if credits >= rank.min_credits:
            selected = rank
        else:
            break
    return selected


def is_promotion(previous_key: Optional[str], next_key: str) -> bool:
    if not previous_key or previous_key not in RANKS_BY_KEY or next_key not in RANKS_BY_KEY:
        return False
    previous_index = CREDIT_RANKS.index(RANKS_BY_KEY[previous_key])
    next_index = CREDIT_RANKS.index(RANKS_BY_KEY[next_key])
    return next_index > previous_index


def should_announce_rank_change(
    previous_key: Optional[str],
    next_key: str,
    *,
    baseline_initialized: bool,
    first_assignment: bool,
    announce_first_assignment: bool,
) -> bool:
    if not baseline_initialized:
        return False
    if first_assignment:
        return announce_first_assignment
    return is_promotion(previous_key, next_key)


class RoleBasedCredits(commands.Cog):
    """Assign Discord rank roles from MissionChief earned credits."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=0xFACA_E001, force_registration=True)
        self.config.register_guild(**DEFAULT_GUILD)
        self._task: Optional[asyncio.Task] = None

    async def cog_load(self):
        self._task = asyncio.create_task(self.sync_loop())

    def cog_unload(self):
        if self._task:
            self._task.cancel()

    def dependencies(self):
        return self.bot.get_cog("MembersScraper"), self.bot.get_cog("MemberSync")

    async def sync_loop(self):
        await self.bot.wait_until_red_ready()
        while True:
            try:
                sleep_minutes = 60
                for guild in self.bot.guilds:
                    cfg = await self.config.guild(guild).all()
                    interval = max(5, int(cfg.get("sync_interval_minutes") or 60))
                    sleep_minutes = min(sleep_minutes, interval)
                    if cfg.get("enabled"):
                        await self.sync_guild(guild, dry_run=False)
                await asyncio.sleep(sleep_minutes * 60)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("RoleBasedCredits sync loop failed")
                await asyncio.sleep(300)

    async def sync_guild(self, guild: discord.Guild, *, dry_run: bool) -> dict[str, int]:
        members_scraper, member_sync = self.dependencies()
        if not members_scraper or not member_sync:
            return {"updated": 0, "skipped": 0, "missing_dependencies": 1, "promotions": 0}

        rank_role_ids = await self.config.guild(guild).rank_role_ids()
        configured_role_ids = {
            int(role_id)
            for role_id in rank_role_ids.values()
            if str(role_id).isdigit() and guild.get_role(int(role_id))
        }
        if not configured_role_ids:
            return {"updated": 0, "skipped": 0, "missing_dependencies": 0, "promotions": 0}

        current_members = await members_scraper.get_members()
        updated = 0
        skipped = 0
        promotions = 0
        baseline_initialized = await self.config.guild(guild).baseline_initialized()
        announce_first_assignment = await self.config.guild(guild).announce_first_assignment()

        async with self.config.guild(guild).last_rank_by_discord_id() as last_ranks:
            for mc_member in current_members:
                mc_id = mc_member.get("user_id") or mc_member.get("mc_user_id")
                if not mc_id:
                    skipped += 1
                    continue

                link = await member_sync.get_link_for_mc(str(mc_id))
                if not link or link.get("status") != "approved":
                    skipped += 1
                    continue

                discord_id = link.get("discord_id")
                member = guild.get_member(int(discord_id)) if discord_id else None
                if not member or member.bot:
                    skipped += 1
                    continue

                try:
                    credits = int(mc_member.get("earned_credits") or 0)
                except (TypeError, ValueError):
                    skipped += 1
                    continue
                if credits < 0:
                    skipped += 1
                    continue

                target_rank = rank_for_credits(credits)
                target_role_id = rank_role_ids.get(target_rank.key)
                target_role = guild.get_role(int(target_role_id)) if target_role_id else None
                if not target_role:
                    skipped += 1
                    continue

                current_rank_key = self.current_configured_rank_key(member, configured_role_ids, rank_role_ids)
                previous_rank_key = last_ranks.get(str(member.id)) or current_rank_key
                roles_to_remove = [
                    role
                    for role in member.roles
                    if role.id in configured_role_ids and role.id != target_role.id
                ]
                roles_to_add = [] if target_role in member.roles else [target_role]
                first_assignment = previous_rank_key is None and current_rank_key is None
                should_announce = should_announce_rank_change(
                    previous_rank_key,
                    target_rank.key,
                    baseline_initialized=baseline_initialized,
                    first_assignment=first_assignment,
                    announce_first_assignment=announce_first_assignment,
                )

                if dry_run:
                    if roles_to_add or roles_to_remove:
                        updated += 1
                    if should_announce:
                        promotions += 1
                    continue

                try:
                    if roles_to_remove:
                        await member.remove_roles(
                            *roles_to_remove,
                            reason="RoleBasedCredits: rank changed from MissionChief credits",
                        )
                    if roles_to_add:
                        await member.add_roles(
                            *roles_to_add,
                            reason="RoleBasedCredits: rank reached from MissionChief credits",
                        )
                    if roles_to_add or roles_to_remove:
                        updated += 1
                    if should_announce:
                        await self.send_promotion_message(guild, member, target_rank, credits)
                        promotions += 1
                    last_ranks[str(member.id)] = target_rank.key
                except (discord.Forbidden, discord.HTTPException):
                    log.exception("Failed to update credit rank role for member %s", member.id)
                    skipped += 1

        if not dry_run and current_members and not baseline_initialized:
            await self.config.guild(guild).baseline_initialized.set(True)

        return {
            "updated": updated,
            "skipped": skipped,
            "missing_dependencies": 0,
            "promotions": promotions,
        }

    @staticmethod
    def current_configured_rank_key(
        member: discord.Member,
        configured_role_ids: set[int],
        rank_role_ids: dict[str, Any],
    ) -> Optional[str]:
        role_ids = {role.id for role in getattr(member, "roles", []) if role.id in configured_role_ids}
        if not role_ids:
            return None
        selected = None
        for rank in CREDIT_RANKS:
            role_id = rank_role_ids.get(rank.key)
            if role_id and int(role_id) in role_ids:
                selected = rank.key
        return selected

    async def send_promotion_message(
        self,
        guild: discord.Guild,
        member: discord.Member,
        rank: CreditRank,
        credits: int,
    ):
        channel_id = await self.config.guild(guild).promotion_channel_id()
        channel = guild.get_channel(channel_id) if channel_id else None
        if not channel:
            return
        embed = discord.Embed(
            title="Promotion",
            description=f"{member.mention} has been promoted to **{rank.name}**.",
            color=discord.Color.green(),
        )
        embed.add_field(name="Credits", value=f"{credits:,}", inline=True)
        embed.add_field(name="Required", value=f"{rank.min_credits:,}", inline=True)
        await channel.send(embed=embed)

    @commands.group(name="creditrankset")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def creditrankset(self, ctx: commands.Context):
        """Configure role based credits."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @creditrankset.command(name="enable")
    async def creditrankset_enable(self, ctx: commands.Context):
        """Enable automatic rank syncing."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("Role based credits enabled.")

    @creditrankset.command(name="disable")
    async def creditrankset_disable(self, ctx: commands.Context):
        """Disable automatic rank syncing."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("Role based credits disabled.")

    @creditrankset.command(name="interval")
    async def creditrankset_interval(self, ctx: commands.Context, minutes: int):
        """Set automatic sync interval in minutes. Minimum is 5."""
        if minutes < 5:
            await ctx.send("Interval must be at least 5 minutes.")
            return
        await self.config.guild(ctx.guild).sync_interval_minutes.set(minutes)
        await ctx.send(f"Role based credits interval set to {minutes} minutes.")

    @creditrankset.command(name="promotionchannel")
    async def creditrankset_promotionchannel(self, ctx: commands.Context, channel: discord.TextChannel):
        """Set the channel where promotion messages are posted."""
        await self.config.guild(ctx.guild).promotion_channel_id.set(channel.id)
        await ctx.send(f"Promotion channel set to {channel.mention}.")

    @creditrankset.command(name="announcefirst")
    async def creditrankset_announcefirst(self, ctx: commands.Context, enabled: bool):
        """Set whether first-time assignments should be announced."""
        await self.config.guild(ctx.guild).announce_first_assignment.set(enabled)
        await ctx.send(f"First assignment announcements {'enabled' if enabled else 'disabled'}.")

    @creditrankset.command(name="role")
    async def creditrankset_role(self, ctx: commands.Context, rank_name: str, role: discord.Role):
        """Map a rank to a Discord role. Quote rank names with spaces."""
        rank = find_rank(rank_name)
        if not rank:
            await ctx.send("Unknown rank. Use `[p]creditranks ranks` to see valid names.")
            return
        async with self.config.guild(ctx.guild).rank_role_ids() as rank_role_ids:
            rank_role_ids[rank.key] = role.id
        await ctx.send(f"{rank.name} will use role {role.mention}.")

    @creditrankset.command(name="clearrole")
    async def creditrankset_clearrole(self, ctx: commands.Context, *, rank_name: str):
        """Remove the Discord role mapping for a rank."""
        rank = find_rank(rank_name)
        if not rank:
            await ctx.send("Unknown rank. Use `[p]creditranks ranks` to see valid names.")
            return
        async with self.config.guild(ctx.guild).rank_role_ids() as rank_role_ids:
            removed = rank_role_ids.pop(rank.key, None)
        await ctx.send("Rank role mapping removed." if removed else "That rank had no role mapping.")

    @commands.group(name="creditranks")
    @commands.guild_only()
    @commands.admin_or_permissions(administrator=True)
    async def creditranks(self, ctx: commands.Context):
        """Inspect and run role based credits."""
        if ctx.invoked_subcommand is None:
            await ctx.send_help(ctx.command)

    @creditranks.command(name="ranks")
    async def creditranks_ranks(self, ctx: commands.Context):
        """Show the fixed credit rank table."""
        lines = [f"{rank.name}: {rank.min_credits:,} credits" for rank in CREDIT_RANKS]
        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(page)

    @creditranks.command(name="roles")
    async def creditranks_roles(self, ctx: commands.Context):
        """Show configured Discord role mappings."""
        rank_role_ids = await self.config.guild(ctx.guild).rank_role_ids()
        lines = []
        for rank in CREDIT_RANKS:
            role_id = rank_role_ids.get(rank.key)
            role = ctx.guild.get_role(int(role_id)) if role_id else None
            lines.append(f"{rank.name}: {role.mention if role else 'not configured'}")
        for page in pagify("\n".join(lines), page_length=1800):
            await ctx.send(page)

    @creditranks.command(name="dryrun")
    async def creditranks_dryrun(self, ctx: commands.Context):
        """Count what would change without changing roles."""
        async with ctx.typing():
            result = await self.sync_guild(ctx.guild, dry_run=True)
        if result["missing_dependencies"]:
            await ctx.send("MembersScraper and MemberSync must be loaded before syncing.")
            return
        await ctx.send(
            f"Dry-run complete. Would update: {result['updated']}, "
            f"promotions: {result['promotions']}, skipped: {result['skipped']}."
        )

    @creditranks.command(name="sync")
    async def creditranks_sync(self, ctx: commands.Context):
        """Run rank sync now."""
        async with ctx.typing():
            result = await self.sync_guild(ctx.guild, dry_run=False)
        if result["missing_dependencies"]:
            await ctx.send("MembersScraper and MemberSync must be loaded before syncing.")
            return
        await ctx.send(
            f"Sync complete. Updated: {result['updated']}, "
            f"promotions: {result['promotions']}, skipped: {result['skipped']}."
        )
