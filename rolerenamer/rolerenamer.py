import asyncio
import io
from dataclasses import dataclass

import discord
from redbot.core import commands


US_ROLE_RENAMES: tuple[tuple[int, str], ...] = (
    (908510950141861898, "US | Alabama (AL)"),
    (908511672203878481, "US | Alaska (AK)"),
    (908511814025904159, "US | Arizona (AZ)"),
    (908511897349947503, "US | Arkansas (AR)"),
    (908511970884456499, "US | California (CA)"),
    (908512033648025650, "US | Colorado (CO)"),
    (908512132629401632, "US | Connecticut (CT)"),
    (908512208101724200, "US | Delaware (DE)"),
    (909026316077137971, "US | District of Columbia (DC)"),
    (908512209943015444, "US | Florida (FL)"),
    (908512213885673493, "US | Georgia (GA)"),
    (908512419435917393, "US | Hawaii (HI)"),
    (908512524410974238, "US | Idaho (ID)"),
    (908512607382671361, "US | Illinois (IL)"),
    (908512702996041738, "US | Indiana (IN)"),
    (908512764287393802, "US | Iowa (IA)"),
    (908512855744217159, "US | Kansas (KS)"),
    (908512932260892692, "US | Kentucky (KY)"),
    (908512975952957470, "US | Louisiana (LA)"),
    (908513057884471347, "US | Maine (ME)"),
    (908513155196534815, "US | Maryland (MD)"),
    (908513195201810432, "US | Massachusetts (MA)"),
    (908513262092550144, "US | Michigan (MI)"),
    (908513331667697714, "US | Minnesota (MN)"),
    (908513391302295595, "US | Mississippi (MS)"),
    (908513471228969030, "US | Missouri (MO)"),
    (908513511867576372, "US | Montana (MT)"),
    (908513564757729280, "US | Nebraska (NE)"),
    (908513620344836106, "US | Nevada (NV)"),
    (908513733234548736, "US | New Hampshire (NH)"),
    (908513789559848960, "US | New Jersey (NJ)"),
    (908513840570974229, "US | New Mexico (NM)"),
    (908513897521233990, "US | New York (NY)"),
    (908513984985042974, "US | North Carolina (NC)"),
    (908514042493173761, "US | North Dakota (ND)"),
    (908514189151191090, "US | Ohio (OH)"),
    (908514330834763836, "US | Oklahoma (OK)"),
    (908514408152584202, "US | Oregon (OR)"),
    (908514524087345213, "US | Pennsylvania (PA)"),
    (908514604924157982, "US | Rhode Island (RI)"),
    (908514697001697313, "US | South Carolina (SC)"),
    (908514799560822855, "US | South Dakota (SD)"),
    (908514866422243379, "US | Tennessee (TN)"),
    (908514960009752667, "US | Texas (TX)"),
    (908515044051017758, "US | Utah (UT)"),
    (908515114611798077, "US | Vermont (VT)"),
    (908515157741801482, "US | Virginia (VA)"),
    (908515240621248562, "US | Washington (WA)"),
    (908515311517597706, "US | West Virginia (WV)"),
    (908515383005315114, "US | Wisconsin (WI)"),
    (908515459882680380, "US | Wyoming (WY)"),
)

COUNTRY_ROLES: tuple[str, ...] = (
    "COUNTRY | Canada",
    "COUNTRY | Netherlands",
    "COUNTRY | United Kingdom",
    "COUNTRY | Germany",
)


@dataclass(frozen=True)
class RenamePlanItem:
    role_id: int
    target_name: str
    current_name: str | None
    status: str


class RoleRenamer(commands.Cog):
    """Temporary owner-only bulk role rename tool."""

    def __init__(self, bot):
        self.bot = bot

    @commands.group(name="rolerenamer", invoke_without_command=True)
    @commands.guild_only()
    @commands.is_owner()
    async def rolerenamer(self, ctx: commands.Context):
        """Temporary role rename helper."""
        await ctx.send_help()

    @rolerenamer.command(name="dryrun")
    @commands.guild_only()
    @commands.is_owner()
    async def dryrun(self, ctx: commands.Context):
        """Show which roles would be renamed or created."""
        rename_plan = self._build_rename_plan(ctx.guild)
        country_plan = self._build_country_plan(ctx.guild)
        report = self._format_plan(rename_plan, country_plan)
        await self._send_report(ctx, "RoleRenamer dry-run complete. No changes were made.", report)

    @rolerenamer.command(name="apply")
    @commands.guild_only()
    @commands.is_owner()
    @commands.max_concurrency(1, per=commands.BucketType.guild, wait=False)
    async def apply(self, ctx: commands.Context, confirmation: str = "", delay_seconds: float = 2.0):
        """
        Rename configured roles and create missing country roles.

        Usage:
        `[p]rolerenamer apply CONFIRM`
        `[p]rolerenamer apply CONFIRM 3`
        """
        if confirmation != "CONFIRM":
            await ctx.send("Run `[p]rolerenamer dryrun` first, then use `apply CONFIRM`.")
            return

        delay_seconds = max(delay_seconds, 1.0)
        bot_member = ctx.guild.me
        if bot_member is None or not bot_member.guild_permissions.manage_roles:
            await ctx.send("I need the Manage Roles permission before I can update roles.")
            return

        rename_plan = self._build_rename_plan(ctx.guild)
        country_plan = self._build_country_plan(ctx.guild)
        planned_renames = [item for item in rename_plan if item.status == "rename"]
        planned_creates = [name for name, exists in country_plan if not exists]

        await ctx.send(
            "Starting RoleRenamer apply. "
            f"Renames: {len(planned_renames)}. Creates: {len(planned_creates)}. "
            f"Delay: {delay_seconds:.1f}s between Discord role changes."
        )

        reason = f"Temporary RoleRenamer command run by {ctx.author} ({ctx.author.id})"
        results: list[str] = []

        for item in rename_plan:
            if item.status == "unchanged":
                results.append(f"SKIP unchanged: {item.target_name} ({item.role_id})")
                continue
            if item.status == "missing":
                results.append(f"ERROR missing role ID: {item.role_id} -> {item.target_name}")
                continue
            if item.status == "blocked_hierarchy":
                results.append(
                    f"ERROR role hierarchy blocks rename: {item.current_name} "
                    f"({item.role_id}) -> {item.target_name}"
                )
                continue

            role = ctx.guild.get_role(item.role_id)
            if role is None:
                results.append(f"ERROR missing role ID: {item.role_id} -> {item.target_name}")
                continue

            try:
                await role.edit(name=item.target_name, reason=reason)
            except discord.Forbidden:
                results.append(
                    f"ERROR forbidden: {item.current_name} ({item.role_id}) -> {item.target_name}"
                )
            except discord.HTTPException as exc:
                results.append(
                    f"ERROR HTTP {exc.status}: {item.current_name} "
                    f"({item.role_id}) -> {item.target_name}"
                )
            else:
                results.append(f"RENAMED: {item.current_name} -> {item.target_name} ({item.role_id})")
                await asyncio.sleep(delay_seconds)

        for role_name, exists in country_plan:
            if exists:
                results.append(f"SKIP existing country role: {role_name}")
                continue

            try:
                await ctx.guild.create_role(name=role_name, reason=reason)
            except discord.Forbidden:
                results.append(f"ERROR forbidden creating country role: {role_name}")
            except discord.HTTPException as exc:
                results.append(f"ERROR HTTP {exc.status} creating country role: {role_name}")
            else:
                results.append(f"CREATED country role: {role_name}")
                await asyncio.sleep(delay_seconds)

        await self._send_report(ctx, "RoleRenamer apply complete.", "\n".join(results))

    def _build_rename_plan(self, guild: discord.Guild) -> list[RenamePlanItem]:
        bot_member = guild.me
        plan: list[RenamePlanItem] = []

        for role_id, target_name in US_ROLE_RENAMES:
            role = guild.get_role(role_id)
            if role is None:
                plan.append(RenamePlanItem(role_id, target_name, None, "missing"))
                continue

            if role.name == target_name:
                status = "unchanged"
            elif bot_member is not None and role >= bot_member.top_role:
                status = "blocked_hierarchy"
            else:
                status = "rename"

            plan.append(RenamePlanItem(role_id, target_name, role.name, status))

        return plan

    def _build_country_plan(self, guild: discord.Guild) -> list[tuple[str, bool]]:
        existing_names = {role.name for role in guild.roles}
        return [(role_name, role_name in existing_names) for role_name in COUNTRY_ROLES]

    def _format_plan(
        self, rename_plan: list[RenamePlanItem], country_plan: list[tuple[str, bool]]
    ) -> str:
        lines = ["US role renames:"]
        for item in rename_plan:
            if item.status == "rename":
                lines.append(f"RENAME: {item.current_name} -> {item.target_name} ({item.role_id})")
            elif item.status == "unchanged":
                lines.append(f"SKIP unchanged: {item.target_name} ({item.role_id})")
            elif item.status == "blocked_hierarchy":
                lines.append(
                    f"BLOCKED hierarchy: {item.current_name} -> "
                    f"{item.target_name} ({item.role_id})"
                )
            else:
                lines.append(f"MISSING: {item.role_id} -> {item.target_name}")

        lines.append("")
        lines.append("Country roles:")
        for role_name, exists in country_plan:
            lines.append(f"{'SKIP existing' if exists else 'CREATE'}: {role_name}")

        rename_count = sum(1 for item in rename_plan if item.status == "rename")
        create_count = sum(1 for _, exists in country_plan if not exists)
        missing_count = sum(1 for item in rename_plan if item.status == "missing")
        blocked_count = sum(1 for item in rename_plan if item.status == "blocked_hierarchy")
        lines.append("")
        lines.append(
            "Summary: "
            f"{rename_count} rename(s), {create_count} create(s), "
            f"{missing_count} missing role ID(s), {blocked_count} hierarchy block(s)."
        )
        return "\n".join(lines)

    async def _send_report(self, ctx: commands.Context, message: str, report: str):
        payload = report.encode("utf-8")
        file = discord.File(io.BytesIO(payload), filename="rolerenamer-report.txt")
        await ctx.send(message, file=file)
