from __future__ import annotations
from typing import Tuple, Optional
from redbot.core import bank
from redbot.core import commands
from datetime import datetime, timedelta
import time

def now_utc_ts() -> int:
    return int(time.time())

def get_day_start_ts() -> int:
    """Get timestamp for start of current UTC day"""
    now = datetime.utcnow()
    day_start = datetime(now.year, now.month, now.day, 0, 0, 0)
    return int(day_start.timestamp())

def get_week_start_ts() -> int:
    """Get timestamp for start of current UTC week (Monday)"""
    now = datetime.utcnow()
    days_since_monday = now.weekday()
    week_start = datetime(now.year, now.month, now.day, 0, 0, 0) - timedelta(days=days_since_monday)
    return int(week_start.timestamp())

class EconomyBridge:
    """Bridge between Roulette game and Red-bot economy system with limits and tracking"""
    
    async def withdraw(self, ctx: commands.Context, amount: int) -> Tuple[bool, str]:
        """
        Withdraw credits from user account.
        Returns: (success, error_message)
        """
        if amount <= 0:
            return True, ""
        
        try:
            bal = await bank.get_balance(ctx.author)
            if bal < amount:
                currency_name = self._get_currency_name(ctx.guild)
                return False, f"❌ Saldo {bal} {currency_name}, nodig {amount} {currency_name}."
            
            await bank.withdraw_credits(ctx.author, amount)
            return True, ""
        except Exception as e:
            return False, f"❌ Economy error: {e}"

    async def deposit(self, ctx: commands.Context, amount: int) -> Tuple[bool, str]:
        """
        Deposit credits to user account.
        Returns: (success, error_message)
        """
        if amount <= 0:
            return True, ""
        
        try:
            await bank.deposit_credits(ctx.author, amount)
            return True, ""
        except Exception as e:
            return False, f"❌ Economy error: {e}"

    async def check_daily_limit(self, config, member, guild_config: dict) -> Tuple[bool, str, int]:
        """
        Check if user has exceeded daily play limit.
        Returns: (can_play, error_message, plays_remaining)
        """
        daily_limit = guild_config.get("ir_daily_limit", 1)
        if daily_limit <= 0:
            return True, "", 999  # No limit
        
        member_data = await config.member(member).all()
        daily_plays = member_data.get("daily_plays", {})
        
        # Check if we need to reset (new day)
        today_start = get_day_start_ts()
        last_reset = daily_plays.get("last_reset", 0)
        
        if last_reset < today_start:
            # New day, reset counter
            return True, "", daily_limit
        
        plays_today = daily_plays.get("count", 0)
        
        if plays_today >= daily_limit:
            hours_until_reset = 24 - datetime.utcnow().hour
            return False, f"❌ Dagelijkse limiet bereikt ({daily_limit} runs/dag). Reset over ~{hours_until_reset}u.", 0
        
        return True, "", daily_limit - plays_today

    async def increment_daily_plays(self, config, member) -> None:
        """Increment daily play counter"""
        async with config.member(member).daily_plays() as daily_plays:
            today_start = get_day_start_ts()
            
            if daily_plays.get("last_reset", 0) < today_start:
                # New day
                daily_plays["last_reset"] = today_start
                daily_plays["count"] = 1
            else:
                # Same day
                daily_plays["count"] = daily_plays.get("count", 0) + 1

    async def check_weekly_payout_cap(self, config, member, guild_config: dict, payout_amount: int) -> Tuple[bool, str]:
        """
        Check if payout would exceed weekly cap.
        Returns: (allowed, error_message)
        """
        weekly_cap = guild_config.get("ir_weekly_payout_cap", 10000)
        if weekly_cap <= 0:
            return True, ""  # No cap
        
        member_data = await config.member(member).all()
        weekly_payouts = member_data.get("weekly_payouts", {})
        
        # Check if we need to reset (new week)
        week_start = get_week_start_ts()
        last_reset = weekly_payouts.get("last_reset", 0)
        
        if last_reset < week_start:
            # New week, would be first payout
            return True, ""
        
        total_this_week = weekly_payouts.get("total", 0)
        
        if total_this_week + payout_amount > weekly_cap:
            currency_name = self._get_currency_name(member.guild)
            remaining = max(0, weekly_cap - total_this_week)
            return False, (
                f"❌ Wekelijkse payout cap bereikt!\n"
                f"Deze week: {total_this_week}/{weekly_cap} {currency_name}\n"
                f"Resterend: {remaining} {currency_name}\n"
                f"Reset: volgende maandag 00:00 UTC"
            )
        
        return True, ""

    async def add_weekly_payout(self, config, member, amount: int) -> None:
        """Add to weekly payout total"""
        async with config.member(member).weekly_payouts() as weekly_payouts:
            week_start = get_week_start_ts()
            
            if weekly_payouts.get("last_reset", 0) < week_start:
                # New week
                weekly_payouts["last_reset"] = week_start
                weekly_payouts["total"] = amount
            else:
                # Same week
                weekly_payouts["total"] = weekly_payouts.get("total", 0) + amount

    async def get_weekly_stats(self, config, member) -> Tuple[int, int]:
        """
        Get weekly payout statistics.
        Returns: (total_earned_this_week, cap)
        """
        member_data = await config.member(member).all()
        weekly_payouts = member_data.get("weekly_payouts", {})
        
        week_start = get_week_start_ts()
        last_reset = weekly_payouts.get("last_reset", 0)
        
        if last_reset < week_start:
            return 0, 10000  # New week, no earnings yet
        
        return weekly_payouts.get("total", 0), 10000

    def calculate_payout(self, score: int, is_perfect: bool, guild_config: dict) -> int:
        """
        Calculate payout based on score and config.
        Includes perfect run bonus.
        """
        reward_per_point = guild_config.get("ir_reward_per_point", 2)
        bonus_perfect = guild_config.get("ir_bonus_perfect", 10)
        
        payout = score * reward_per_point
        
        if is_perfect:
            payout += bonus_perfect
        
        return max(0, payout)

    def format_amount(self, guild, amount: int) -> str:
        """Format amount with currency name - SYNC version"""
        currency_name = self._get_currency_name(guild)
        return f"{amount} {currency_name}"

    def _get_currency_name(self, guild) -> str:
        """Get currency name for guild - SYNC wrapper"""
        try:
            result = bank.get_currency_name(guild)
            # Handle both sync and async versions
            import inspect
            if inspect.iscoroutine(result):
                return "credits"  # Fallback if async
            return result
        except Exception:
            return "credits"

    async def refund_on_error(self, ctx: commands.Context, amount: int, reason: str = "error") -> None:
        """
        Refund credits on error (per spec: only on hard errors, not timeouts).
        """
        if amount <= 0:
            return
        
        try:
            await bank.deposit_credits(ctx.author, amount)
            currency_name = self._get_currency_name(ctx.guild)
            await ctx.send(
                f"💰 Refund: {amount} {currency_name} (reden: {reason})"
            )
        except Exception:
            pass  # Best effort

    async def get_economy_stats(self, config, member, guild) -> dict:
        """Get comprehensive economy stats for a user"""
        member_data = await config.member(member).all()
        
        # Daily stats
        daily_plays = member_data.get("daily_plays", {})
        today_start = get_day_start_ts()
        plays_today = daily_plays.get("count", 0) if daily_plays.get("last_reset", 0) >= today_start else 0
        
        # Weekly stats
        weekly_payouts = member_data.get("weekly_payouts", {})
        week_start = get_week_start_ts()
        earned_this_week = weekly_payouts.get("total", 0) if weekly_payouts.get("last_reset", 0) >= week_start else 0
        
        # Score history
        score_history = member_data.get("score_history", [])
        total_runs = len(score_history)
        total_score = sum(s.get("score", 0) for s in score_history)
        total_earned = sum(s.get("payout", 0) for s in score_history)
        perfect_runs = sum(1 for s in score_history if s.get("perfect", False))
        
        # Best score
        best_score = max((s.get("score", 0) for s in score_history), default=0)
        avg_score = total_score / total_runs if total_runs > 0 else 0
        
        # Current balance
        try:
            balance = await bank.get_balance(member)
        except Exception:
            balance = 0
        
        return {
            "plays_today": plays_today,
            "earned_this_week": earned_this_week,
            "total_runs": total_runs,
            "total_score": total_score,
            "total_earned": total_earned,
            "perfect_runs": perfect_runs,
            "best_score": best_score,
            "avg_score": round(avg_score, 1),
            "balance": balance,
            "currency": self._get_currency_name(guild),
        }
