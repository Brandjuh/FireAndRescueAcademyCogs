"""
Incident Roulette - Complete Red-Discord Bot Cog
Full implementation with all features from specification
"""
from __future__ import annotations
import discord
from discord import app_commands
from redbot.core import commands, Config
from typing import Optional
import secrets

from .roulette import (
    CallPool, CallSpec, generate_run, score_run, is_perfect_run,
    RouletteView, now_utc_ts
)

from .economy import EconomyBridge

class IncidentRoulette(commands.Cog):
    """
    Incident Roulette - Emergency Response Allocation Game
    
    Test your resource allocation skills with randomized incident calls.
    Match requirements, avoid oversupply, and beat the clock for bonus points!
    """
    
    def __init__(self, bot):
        self.bot = bot
        self.config = Config.get_conf(self, identifier=1234567890, force_registration=True)
        
        # Guild settings
        default_guild = {
            "ir_cost_per_play": 50,
            "ir_reward_per_point": 2,
            "ir_bonus_perfect": 10,
            "ir_daily_limit": 1,
            "ir_weekly_payout_cap": 10000,
            "allow_dupes": False,
            "hard_mode": False,
        }
        
        # Member data
        default_member = {
            "active_run": {},
            "daily_plays": {"last_reset": 0, "count": 0},
            "weekly_payouts": {"last_reset": 0, "total": 0},
            "score_history": [],
            "total_runs": 0,
            "best_score": 0,
        }
        
        self.config.register_guild(**default_guild)
        self.config.register_member(**default_member)
        
        self.economy = EconomyBridge()
        self.pool = CallPool.default_pool()

    @app_commands.command(name="roulette")
    @app_commands.describe(action="start/claim/cancel/stats/config")
    async def roulette(self, interaction: discord.Interaction, action: str = "start"):
        """Incident Roulette - Emergency response allocation game"""
        await interaction.response.defer(ephemeral=False)
        
        # Route to appropriate handler
        action = action.lower()
        if action == "start":
            await self._handle_start(interaction)
        elif action == "claim":
            await self._handle_claim(interaction)
        elif action == "cancel":
            await self._handle_cancel(interaction)
        elif action == "stats":
            await self._handle_stats(interaction)
        elif action == "config":
            await self._handle_config(interaction)
        else:
            await interaction.followup.send(
                "❌ Onbekende actie. Gebruik: start/claim/cancel/stats/config",
                ephemeral=True
            )

    async def _handle_start(self, interaction: discord.Interaction):
        """Start a new roulette run"""
        ctx = await self.bot.get_context(interaction)
        member_data = self.config.member(interaction.user)
        guild_config = await self.config.guild(interaction.guild).all()
        
        # Check if already has active run
        active_run = await member_data.active_run()
        if active_run and active_run.get("seed"):
            await interaction.followup.send(
                "⚠️ Je hebt al een actieve run! Gebruik `/roulette claim` of `/roulette cancel` eerst.",
                ephemeral=True
            )
            return
        
        # Check daily limit
        can_play, limit_msg, remaining = await self.economy.check_daily_limit(
            self.config, interaction.user, guild_config
        )
        if not can_play:
            await interaction.followup.send(limit_msg, ephemeral=True)
            return
        
        # Withdraw cost
        cost = guild_config.get("ir_cost_per_play", 50)
        success, msg = await self.economy.withdraw(ctx, cost)
        if not success:
            await interaction.followup.send(msg, ephemeral=True)
            return
        
        try:
            # Generate run
            seed = secrets.token_hex(2).upper()
            hard_mode = guild_config.get("hard_mode", False)
            allow_dupes = guild_config.get("allow_dupes", False)
            
            calls = generate_run(seed, self.pool, allow_dupes, hard_mode)
            
            # Create state
            state = {
                "seed": seed,
                "calls": [c.to_json() for c in calls],
                "allocs": {},
                "per_call_time_s": [None] * len(calls),
                "current_idx": 0,
                "started_at": now_utc_ts(),
                "expires_at": now_utc_ts() + (15 * 60),  # 15 min TTL
                "hard_mode": hard_mode,
            }
            
            # Save state
            await member_data.active_run.set(state)
            
            # Increment daily counter
            await self.economy.increment_daily_plays(self.config, interaction.user)
            
            # Create embed
            embed = discord.Embed(
                title="🚨 Incident Roulette - Run Started",
                description=(
                    f"**3 calls gegenereerd**\n"
                    f"Seed: `{seed}`{' 🔥 HARD MODE' if hard_mode else ''}\n"
                    f"Kosten: {self.economy.format_amount(interaction.guild, cost)}\n"
                    f"Dagelijkse runs: {remaining} remaining"
                ),
                color=discord.Color.orange()
            )
            
            # Show first call
            first_call = calls[0]
            embed.add_field(
                name=f"📞 Call 1/3: {first_call.name}",
                value=(
                    f"**Tier:** {first_call.tier}\n"
                    f"**Vereist:** {first_call.requirements_str()}\n"
                    f"⏱️ Timer gestart..."
                ),
                inline=False
            )
            
            embed.set_footer(text=f"TTL: 15 min | Seed: {seed}")
            
            # Create view
            view = RouletteView(self, ctx, state, interaction.user.id, timeout=15*60)
            
            await interaction.followup.send(embed=embed, view=view)
            
        except Exception as e:
            # Refund on error
            await self.economy.refund_on_error(ctx, cost, f"start error: {e}")
            await interaction.followup.send(f"❌ Error bij start: {e}", ephemeral=True)

    async def _handle_claim(self, interaction: discord.Interaction):
        """Claim score for current run"""
        ctx = await self.bot.get_context(interaction)
        member_data = self.config.member(interaction.user)
        guild_config = await self.config.guild(interaction.guild).all()
        
        # Get active run
        state = await member_data.active_run()
        if not state or not state.get("seed"):
            await interaction.followup.send(
                "❌ Geen actieve run. Start eerst met `/roulette start`.",
                ephemeral=True
            )
            return
        
        try:
            # Parse calls
            calls = [CallSpec.from_json(d) for d in state["calls"]]
            hard_mode = state.get("hard_mode", False)
            
            # Calculate score
            score, breakdown, is_perfect = score_run(calls, state, hard_mode)
            
            # Calculate payout
            payout = self.economy.calculate_payout(score, is_perfect, guild_config)
            
            # Check weekly cap
            can_payout, cap_msg = await self.economy.check_weekly_payout_cap(
                self.config, interaction.user, guild_config, payout
            )
            
            if not can_payout:
                await interaction.followup.send(cap_msg, ephemeral=True)
                # Still clear the run but no payout
                await member_data.active_run.clear()
                return
            
            # Deposit credits
            success, msg = await self.economy.deposit(ctx, payout)
            if not success:
                await interaction.followup.send(f"❌ Payout error: {msg}", ephemeral=True)
                return
            
            # Update weekly total
            await self.economy.add_weekly_payout(self.config, interaction.user, payout)
            
            # Update stats
            async with member_data.score_history() as history:
                history.append({
                    "timestamp": now_utc_ts(),
                    "score": score,
                    "payout": payout,
                    "perfect": is_perfect,
                    "seed": state.get("seed", ""),
                    "hard_mode": hard_mode,
                })
                if len(history) > 50:
                    history[:] = history[-50:]
            
            best_score = await member_data.best_score()
            if score > best_score:
                await member_data.best_score.set(score)
            
            await member_data.total_runs.set(await member_data.total_runs() + 1)
            
            # Clear active run
            await member_data.active_run.clear()
            
            # Create result embed
            embed = discord.Embed(
                title="📊 Incident Roulette - Score Claimed!",
                description=f"Seed: `{state.get('seed', 'N/A')}`{' 🔥 HARD MODE' if hard_mode else ''}",
                color=discord.Color.green() if is_perfect else discord.Color.blue()
            )
            
            # Add breakdown
            for i, (pts, details) in enumerate(breakdown):
                call = calls[i]
                embed.add_field(
                    name=f"Call {i+1}: {call.name} - {pts} pts",
                    value=details,
                    inline=False
                )
            
            # Total
            embed.add_field(
                name="💰 Totaal",
                value=(
                    f"**Score:** {score} punten\n"
                    f"**Payout:** {self.economy.format_amount(interaction.guild, payout)}\n"
                    f"{'🌟 **PERFECT RUN BONUS!**' if is_perfect else ''}"
                ),
                inline=False
            )
            
            if score > best_score:
                embed.add_field(
                    name="🏆 NEW PERSONAL BEST!",
                    value=f"Previous: {best_score}",
                    inline=False
                )
            
            embed.set_footer(text=f"Run completed | Seed: {state.get('seed', 'N/A')}")
            
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Claim error: {e}", ephemeral=True)

    async def _handle_cancel(self, interaction: discord.Interaction):
        """Cancel active run (no refund)"""
        member_data = self.config.member(interaction.user)
        
        state = await member_data.active_run()
        if not state or not state.get("seed"):
            await interaction.followup.send(
                "❌ Geen actieve run om te annuleren.",
                ephemeral=True
            )
            return
        
        # Clear run
        await member_data.active_run.clear()
        
        await interaction.followup.send(
            "❌ Run geannuleerd. Geen refund (per policy).",
            ephemeral=True
        )

    async def _handle_stats(self, interaction: discord.Interaction):
        """Show player statistics"""
        stats = await self.economy.get_economy_stats(
            self.config, interaction.user, interaction.guild
        )
        
        embed = discord.Embed(
            title=f"📊 Incident Roulette Stats - {interaction.user.display_name}",
            color=discord.Color.blue()
        )
        
        embed.add_field(
            name="🎮 Gameplay",
            value=(
                f"**Runs vandaag:** {stats['plays_today']}\n"
                f"**Totaal runs:** {stats['total_runs']}\n"
                f"**Perfect runs:** {stats['perfect_runs']}"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🏆 Scores",
            value=(
                f"**Best score:** {stats['best_score']}\n"
                f"**Gem. score:** {stats['avg_score']}\n"
                f"**Totaal punten:** {stats['total_score']}"
            ),
            inline=True
        )
        
        embed.add_field(
            name="💰 Economy",
            value=(
                f"**Deze week:** {stats['earned_this_week']} {stats['currency']}\n"
                f"**Totaal verdiend:** {stats['total_earned']} {stats['currency']}\n"
                f"**Huidige saldo:** {stats['balance']} {stats['currency']}"
            ),
            inline=False
        )
        
        await interaction.followup.send(embed=embed)

    async def _handle_config(self, interaction: discord.Interaction):
        """Show or modify config (admin only)"""
        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send(
                "❌ Alleen administrators kunnen config wijzigen.",
                ephemeral=True
            )
            return
        
        guild_config = await self.config.guild(interaction.guild).all()
        
        embed = discord.Embed(
            title="⚙️ Incident Roulette Config",
            description="Huidige instellingen voor deze server",
            color=discord.Color.purple()
        )
        
        embed.add_field(
            name="💰 Economy",
            value=(
                f"**Cost per play:** {guild_config['ir_cost_per_play']}\n"
                f"**Reward per point:** {guild_config['ir_reward_per_point']}\n"
                f"**Perfect bonus:** {guild_config['ir_bonus_perfect']}"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🎮 Limits",
            value=(
                f"**Daily limit:** {guild_config['ir_daily_limit']}\n"
                f"**Weekly cap:** {guild_config['ir_weekly_payout_cap']}"
            ),
            inline=True
        )
        
        embed.add_field(
            name="🎲 Gameplay",
            value=(
                f"**Allow dupes:** {guild_config['allow_dupes']}\n"
                f"**Hard mode:** {guild_config['hard_mode']}"
            ),
            inline=True
        )
        
        embed.set_footer(text="Use /roulette_config to modify settings")
        
        await interaction.followup.send(embed=embed)

    @app_commands.command(name="roulette_config")
    @app_commands.describe(
        setting="Setting to modify",
        value="New value"
    )
    @app_commands.choices(setting=[
        app_commands.Choice(name="cost_per_play", value="ir_cost_per_play"),
        app_commands.Choice(name="reward_per_point", value="ir_reward_per_point"),
        app_commands.Choice(name="perfect_bonus", value="ir_bonus_perfect"),
        app_commands.Choice(name="daily_limit", value="ir_daily_limit"),
        app_commands.Choice(name="weekly_cap", value="ir_weekly_payout_cap"),
        app_commands.Choice(name="allow_dupes", value="allow_dupes"),
        app_commands.Choice(name="hard_mode", value="hard_mode"),
    ])
    async def roulette_config(
        self, 
        interaction: discord.Interaction, 
        setting: str, 
        value: str
    ):
        """Modify roulette config (admin only)"""
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message(
                "❌ Alleen administrators kunnen config wijzigen.",
                ephemeral=True
            )
            return
        
        # Parse value
        if setting in ["allow_dupes", "hard_mode"]:
            new_value = value.lower() in ["true", "yes", "1", "on"]
        else:
            try:
                new_value = int(value)
            except ValueError:
                await interaction.response.send_message(
                    f"❌ Ongeldige waarde voor {setting}. Verwacht nummer.",
                    ephemeral=True
                )
                return
        
        # Update config
        await self.config.guild(interaction.guild).set_raw(setting, value=new_value)
        
        await interaction.response.send_message(
            f"✅ Config updated: **{setting}** = `{new_value}`",
            ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(IncidentRoulette(bot))
