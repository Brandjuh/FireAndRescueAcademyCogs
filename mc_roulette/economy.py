from __future__ import annotations
from typing import Tuple
from redbot.core import bank
from redbot.core import commands

class EconomyBridge:
    async def withdraw(self, ctx: commands.Context, amount: int) -> Tuple[bool, str]:
        if amount <= 0:
            return True, "No charge."
        try:
            bal = await bank.get_balance(ctx.author)
            if bal < amount:
                try:
                    name = bank.get_currency_name(ctx.guild)
                except Exception:
                    name = "credits"
                return False, f"Saldo {bal} {name}, nodig {amount} {name}."
            await bank.withdraw_credits(ctx.author, amount)
            return True, ""
        except Exception as e:
            return False, f"{e}"

    async def deposit(self, ctx: commands.Context, amount: int) -> Tuple[bool, str]:
        if amount <= 0:
            return True, "No reward."
        try:
            await bank.deposit_credits(ctx.author, amount)
            return True, ""
        except Exception as e:
            return False, f"{e}"

    def format_amount(self, guild, amount: int) -> str:
        try:
            unit = bank.get_currency_name(guild)
        except Exception:
            unit = "credits"
        return f"{amount} {unit}"
