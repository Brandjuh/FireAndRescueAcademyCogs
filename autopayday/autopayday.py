import discord
from redbot.core import commands, Config, bank
from redbot.core.bot import Red
from redbot.core.utils.chat_formatting import humanize_number
import asyncio
from datetime import datetime, timedelta
from typing import Optional


class AutoPayday(commands.Cog):
    """Automatische payday voor leden met de admin rol."""

    def __init__(self, bot: Red):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        default_guild = {
            "enabled": False,
            "amount": 1000,
            "interval": 86400,  # 24 uur in seconden
            "admin_role_id": None,
            "last_payday": None
        }
        
        self.config.register_guild(**default_guild)
        self.payday_task = None
        self.bot.loop.create_task(self.initialize())

    async def initialize(self):
        """Start de payday loop na het opstarten van de bot."""
        await self.bot.wait_until_ready()
        self.payday_task = self.bot.loop.create_task(self.payday_loop())

    def cog_unload(self):
        """Stop de payday loop bij het uitladen van de cog."""
        if self.payday_task:
            self.payday_task.cancel()

    async def payday_loop(self):
        """Loop die periodiek payday uitvoert."""
        await self.bot.wait_until_ready()
        
        while True:
            try:
                for guild in self.bot.guilds:
                    settings = await self.config.guild(guild).all()
                    
                    if not settings["enabled"] or not settings["admin_role_id"]:
                        continue
                    
                    # Check of het tijd is voor payday
                    last_payday = settings["last_payday"]
                    interval = settings["interval"]
                    
                    if last_payday is None or (datetime.utcnow().timestamp() - last_payday) >= interval:
                        await self.execute_payday(guild, settings)
                
                # Check elke 60 seconden
                await asyncio.sleep(60)
                
            except Exception as e:
                print(f"Error in payday loop: {e}")
                await asyncio.sleep(60)

    async def execute_payday(self, guild: discord.Guild, settings: dict):
        """Voer payday uit voor alle leden met de admin rol."""
        admin_role = guild.get_role(settings["admin_role_id"])
        
        if not admin_role:
            return
        
        amount = settings["amount"]
        currency_name = await bank.get_currency_name(guild)
        paid_count = 0
        
        for member in admin_role.members:
            if member.bot:
                continue
            
            try:
                await bank.deposit_credits(member, amount)
                paid_count += 1
            except Exception as e:
                print(f"Error paying {member.name}: {e}")
        
        # Update laatste payday tijd
        await self.config.guild(guild).last_payday.set(datetime.utcnow().timestamp())
        
        print(f"Auto-payday uitgevoerd in {guild.name}: {paid_count} admins ontvingen {amount} {currency_name}")

    @commands.group(name="autopayday")
    @commands.admin_or_permissions(administrator=True)
    async def autopayday(self, ctx: commands.Context):
        """Beheer automatische payday voor admins."""
        pass

    @autopayday.command(name="enable")
    async def autopayday_enable(self, ctx: commands.Context):
        """Schakel automatische payday in."""
        await self.config.guild(ctx.guild).enabled.set(True)
        await ctx.send("✅ Automatische payday is ingeschakeld!")

    @autopayday.command(name="disable")
    async def autopayday_disable(self, ctx: commands.Context):
        """Schakel automatische payday uit."""
        await self.config.guild(ctx.guild).enabled.set(False)
        await ctx.send("❌ Automatische payday is uitgeschakeld!")

    @autopayday.command(name="setrole")
    async def autopayday_setrole(self, ctx: commands.Context, role: discord.Role):
        """Stel de admin rol in die payday ontvangt.
        
        **Voorbeeld:**
        `[p]autopayday setrole @Admin`
        """
        await self.config.guild(ctx.guild).admin_role_id.set(role.id)
        await ctx.send(f"✅ Admin rol ingesteld op: {role.mention}")

    @autopayday.command(name="setamount")
    async def autopayday_setamount(self, ctx: commands.Context, amount: int):
        """Stel het payday bedrag in.
        
        **Voorbeeld:**
        `[p]autopayday setamount 5000`
        """
        if amount <= 0:
            await ctx.send("❌ Bedrag moet groter zijn dan 0!")
            return
        
        await self.config.guild(ctx.guild).amount.set(amount)
        currency_name = await bank.get_currency_name(ctx.guild)
        await ctx.send(f"✅ Payday bedrag ingesteld op: {humanize_number(amount)} {currency_name}")

    @autopayday.command(name="setinterval")
    async def autopayday_setinterval(self, ctx: commands.Context, hours: int):
        """Stel het interval in (in uren) tussen payday uitkeringen.
        
        **Voorbeeld:**
        `[p]autopayday setinterval 24` (voor dagelijkse payday)
        """
        if hours <= 0:
            await ctx.send("❌ Interval moet groter zijn dan 0 uur!")
            return
        
        seconds = hours * 3600
        await self.config.guild(ctx.guild).interval.set(seconds)
        await ctx.send(f"✅ Payday interval ingesteld op: {hours} uur")

    @autopayday.command(name="force")
    async def autopayday_force(self, ctx: commands.Context):
        """Forceer een directe payday voor alle admins."""
        settings = await self.config.guild(ctx.guild).all()
        
        if not settings["admin_role_id"]:
            await ctx.send("❌ Geen admin rol ingesteld! Gebruik `[p]autopayday setrole` eerst.")
            return
        
        admin_role = ctx.guild.get_role(settings["admin_role_id"])
        
        if not admin_role:
            await ctx.send("❌ Admin rol niet gevonden!")
            return
        
        amount = settings["amount"]
        currency_name = await bank.get_currency_name(ctx.guild)
        paid_count = 0
        
        async with ctx.typing():
            for member in admin_role.members:
                if member.bot:
                    continue
                
                try:
                    await bank.deposit_credits(member, amount)
                    paid_count += 1
                except Exception as e:
                    await ctx.send(f"⚠️ Fout bij betalen van {member.mention}: {e}")
        
        await self.config.guild(ctx.guild).last_payday.set(datetime.utcnow().timestamp())
        await ctx.send(f"✅ Payday uitgevoerd! {paid_count} admins hebben {humanize_number(amount)} {currency_name} ontvangen.")

    @autopayday.command(name="settings")
    async def autopayday_settings(self, ctx: commands.Context):
        """Toon de huidige instellingen."""
        settings = await self.config.guild(ctx.guild).all()
        currency_name = await bank.get_currency_name(ctx.guild)
        
        status = "✅ Ingeschakeld" if settings["enabled"] else "❌ Uitgeschakeld"
        
        admin_role = ctx.guild.get_role(settings["admin_role_id"]) if settings["admin_role_id"] else None
        role_mention = admin_role.mention if admin_role else "Niet ingesteld"
        
        interval_hours = settings["interval"] / 3600
        
        last_payday = "Nog niet uitgevoerd"
        if settings["last_payday"]:
            last_time = datetime.fromtimestamp(settings["last_payday"])
            last_payday = last_time.strftime("%d-%m-%Y %H:%M:%S")
        
        embed = discord.Embed(
            title="Auto Payday Instellingen",
            color=discord.Color.blue()
        )
        
        embed.add_field(name="Status", value=status, inline=False)
        embed.add_field(name="Admin Rol", value=role_mention, inline=True)
        embed.add_field(name="Bedrag", value=f"{humanize_number(settings['amount'])} {currency_name}", inline=True)
        embed.add_field(name="Interval", value=f"{interval_hours} uur", inline=True)
        embed.add_field(name="Laatste Payday", value=last_payday, inline=False)
        
        await ctx.send(embed=embed)


async def setup(bot: Red):
    """Setup functie voor het laden van de cog."""
    await bot.add_cog(AutoPayday(bot))
