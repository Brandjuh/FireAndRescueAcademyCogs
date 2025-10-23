import random
from redbot.core import commands, Config
from .economy import EconomyBridge
from .models import RP_SKILLS

class RPRolls(commands.Cog):
    """RP dice rolls: 2d6 vs DC; +2 credits on success (once per user per incident)."""

    def __init__(self, bot, config: Config, econ: EconomyBridge, cad):
        self.bot = bot
        self.config = config
        self.econ = econ
        self.cad = cad
        self.guild = config.guild
        self.member = config.member

    @commands.hybrid_group(name="rp")
    async def rp(self, ctx: commands.Context):
        """Roleplay checks for incidents (optional)."""
        pass

    @rp.command(name="roll")
    async def roll(self, ctx: commands.Context, skill: str, dc: int, iid: str):
        skill = skill.strip()
        if skill not in RP_SKILLS:
            await ctx.reply(f"Unknown skill. Choose one of: {', '.join(RP_SKILLS)}", ephemeral=True); return
        gi = await self.guild(ctx.guild).incidents()
        data = gi.get(iid)
        if not data or data.get("resolved_ts"):
            await ctx.reply("Incident not found or already resolved.", ephemeral=True); return

        mid = self.member(ctx.author)
        rpi = await mid.rp_per_incident()
        if rpi.get(iid):
            await ctx.reply("You've already received an RP reward for this incident.", ephemeral=True); return

        d1 = random.randint(1,6); d2 = random.randint(1,6)
        total = d1 + d2
        if total >= int(dc):
            class Ctx:
                def __init__(self, g, m): self.guild=g; self.author=m
            await self.econ.deposit(Ctx(ctx.guild, ctx.author), 2)
            rpi[iid] = True
            await mid.rp_per_incident.set(rpi)
            await ctx.reply(f"Roll: {d1}+{d2} = **{total}** vs DC {dc} • Success! +2 credits.")
        else:
            await ctx.reply(f"Roll: {d1}+{d2} = **{total}** vs DC {dc} • Fail.")
