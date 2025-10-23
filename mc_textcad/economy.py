from redbot.core import bank, commands

class EconomyBridge:
    async def withdraw(self, ctx: commands.Context, amount: int):
        if amount <= 0:
            return True, "No charge."
        try:
            bal = await bank.get_balance(ctx.author)
            try:
                unit = bank.get_currency_name(ctx.guild)
            except Exception:
                unit = "credits"
            if bal < amount:
                return False, f"Balance {bal} {unit}, need {amount} {unit}."
            await bank.withdraw_credits(ctx.author, amount)
            return True, ""
        except Exception as e:
            return False, f"{e}"

    async def deposit(self, ctx: commands.Context, amount: int):
        if amount <= 0:
            return True, "No reward."
        try:
            await bank.deposit_credits(ctx.author, amount)
            return True, ""
        except Exception as e:
            return False, f"{e}"

    def format_amount(self, guild, amount: int) -> str:
        try:
            name = bank.get_currency_name(guild)
        except Exception:
            name = "credits"
        return f"{amount} {name}"
