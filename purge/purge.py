import discord
from redbot.core import commands, Config
from redbot.core.utils.predicates import MessagePredicate
import asyncio
from typing import Optional

class Purge(commands.Cog):
    """Cog voor het verwijderen van berichten in bulk"""
    
    def __init__(self, bot):
        self.bot = bot
        
    @commands.command()
    @commands.has_permissions(administrator=True)
    @commands.guild_only()
    @commands.bot_has_permissions(manage_messages=True, read_message_history=True)
    async def purge(self, ctx, amount: int):
        """
        Verwijder een bepaald aantal berichten uit het huidige kanaal.
        
        Parameters:
        -----------
        amount: int
            Het aantal berichten dat verwijderd moet worden (max 1000 per keer)
        """
        
        # Validatie van het aantal
        if amount <= 0:
            await ctx.send("❌ Het aantal berichten moet groter zijn dan 0.", delete_after=10)
            return
            
        if amount > 1000:
            await ctx.send("❌ Je kunt maximaal 1000 berichten per keer verwijderen.", delete_after=10)
            return
        
        # Bevestiging vragen
        confirm_msg = await ctx.send(
            f"⚠️ Weet je zeker dat je **{amount}** berichten wilt verwijderen uit {ctx.channel.mention}?\n"
            f"Reageer met `ja` of `yes` om te bevestigen, of `nee` of `no` om te annuleren.\n"
            f"*(Deze vraag vervalt na 30 seconden)*"
        )
        
        try:
            # Wacht op bevestiging
            pred = MessagePredicate.yes_or_no(ctx)
            await self.bot.wait_for("message", check=pred, timeout=30.0)
            
            if pred.result is True:
                # Gebruiker heeft bevestigd
                try:
                    # Verwijder de bevestigingsberichten eerst
                    await confirm_msg.delete()
                    await ctx.message.delete()
                    
                    # Berichten ophalen en verwijderen in batches (Discord rate limits)
                    deleted_total = 0
                    
                    # Discord bulk_delete kan max 100 berichten per keer
                    while amount > 0:
                        batch_size = min(amount, 100)
                        
                        try:
                            # Haal berichten op (+ extra voor de berichten die we net verwijderd hebben)
                            messages = []
                            async for msg in ctx.channel.history(limit=batch_size):
                                messages.append(msg)
                            
                            if not messages:
                                break
                            
                            # Filter berichten die ouder zijn dan 14 dagen
                            import datetime
                            two_weeks_ago = discord.utils.utcnow() - datetime.timedelta(days=14)
                            recent_messages = [msg for msg in messages if msg.created_at > two_weeks_ago]
                            old_messages = [msg for msg in messages if msg.created_at <= two_weeks_ago]
                            
                            # Bulk delete voor recente berichten
                            if recent_messages:
                                await ctx.channel.delete_messages(recent_messages)
                                deleted_total += len(recent_messages)
                            
                            # Individueel verwijderen voor oude berichten (langzamer)
                            for msg in old_messages:
                                try:
                                    await msg.delete()
                                    deleted_total += 1
                                    await asyncio.sleep(1)  # Rate limit voorkomen
                                except discord.HTTPException:
                                    pass
                            
                            amount -= batch_size
                            
                            # Kleine pauze tussen batches voor rate limits
                            if amount > 0:
                                await asyncio.sleep(1)
                                
                        except discord.HTTPException as e:
                            if e.status == 429:  # Rate limited
                                await asyncio.sleep(5)
                                continue
                            else:
                                raise
                    
                except discord.Forbidden:
                    await ctx.send("❌ Ik heb geen permissie om berichten te verwijderen in dit kanaal.", delete_after=10)
                    
                except discord.HTTPException as e:
                    await ctx.send(f"❌ Er is een fout opgetreden: {str(e)}", delete_after=10)
                    
            else:
                # Gebruiker heeft geannuleerd
                await confirm_msg.edit(content="❌ Actie geannuleerd.")
                await asyncio.sleep(5)
                await confirm_msg.delete()
                
        except asyncio.TimeoutError:
            # Timeout bereikt
            await confirm_msg.edit(content="❌ Bevestiging verlopen. Actie geannuleerd.")
            await asyncio.sleep(5)
            await confirm_msg.delete()

    @purge.error
    async def purge_error(self, ctx, error):
        """Error handler voor het purge commando"""
        
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ Je hebt administrator rechten nodig om dit commando te gebruiken.", delete_after=10)
            
        elif isinstance(error, commands.BotMissingPermissions):
            # Bot heeft geen permissies - doe niks zoals gevraagd
            pass
            
        elif isinstance(error, commands.NoPrivateMessage):
            await ctx.send("❌ Dit commando kan alleen in een server gebruikt worden.", delete_after=10)
            
        elif isinstance(error, commands.BadArgument):
            await ctx.send("❌ Geef een geldig aantal berichten op. Bijvoorbeeld: `!purge 50`", delete_after=10)
            
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send("❌ Je moet aangeven hoeveel berichten je wilt verwijderen. Bijvoorbeeld: `!purge 50`", delete_after=10)

async def setup(bot):
    await bot.add_cog(Purge(bot))
