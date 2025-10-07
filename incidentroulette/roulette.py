from __future__ import annotations
import time, random, dataclasses
import discord
from discord.ui import View, Select, Button
from typing import List, Dict, Tuple, Optional

ROLES = ["E", "L", "HR", "BC", "EMS", "USAR", "ARFF"]

def now_utc_ts() -> int:
    return int(time.time())

@dataclasses.dataclass
class CallSpec:
    id: str
    name: str
    tier: int
    requirements: Dict[str, int]
    oversupply_penalty: bool = True

    def requirements_str(self) -> str:
        parts = []
        for r in ROLES:
            if self.requirements.get(r, 0):
                parts.append(f"{self.requirements[r]}{r}")
        return ", ".join(parts) if parts else "None"

    def to_json(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "tier": self.tier,
            "requirements": self.requirements,
            "oversupply_penalty": self.oversupply_penalty,
        }

    @classmethod
    def from_json(cls, d: dict) -> "CallSpec":
        return cls(
            id=d["id"], name=d["name"], tier=int(d["tier"]),
            requirements={k:int(v) for k,v in d["requirements"].items()},
            oversupply_penalty=bool(d.get("oversupply_penalty", True))
        )

class CallPool:
    def __init__(self, items: List[CallSpec]) -> None:
        self.items = items

    @classmethod
    def default_pool(cls) -> "CallPool":
        data = [
            ("HIGHRISE_SMOKE","High-Rise Smoke Investigation",2,{"E":2,"L":1,"BC":1}),
            ("MCI_BUS","Mass-Casualty Bus",3,{"EMS":2,"BC":1}),
            ("HAZMAT_SMALL","HazMat Small Leak",2,{"E":1,"HR":1,"BC":1}),
            ("ARFF_HOTBRAKES","ARFF Hot Brakes",2,{"ARFF":1,"BC":1}),
            ("WILDLAND_SPOT","Wildland Spot Fire",1,{"E":1}),
            ("STRUCTURE_WORKING","Working Structure Fire",3,{"E":3,"L":1,"HR":1,"BC":1}),
            ("TRAFFIC_PI","Traffic Crash with Injuries",1,{"E":1,"EMS":1}),
            ("TECH_TRENCH","Trench Rescue",3,{"HR":1,"USAR":1,"BC":1}),
            ("AIRCRAFT_GEAR","Aircraft Gear Issue",3,{"ARFF":2,"BC":1}),
            ("HAZMAT_TANKER","HazMat Tanker Spill",4,{"E":2,"HR":1,"BC":1}),
            ("HIGHRISE_ALARM","High-Rise Alarm",1,{"E":1,"L":1,"BC":1}),
            ("WATER_SWEEP","Water Rescue Sweep",2,{"HR":1,"EMS":1,"BC":1}),
            ("INDUSTRIAL_FIRE","Industrial Fire",4,{"E":3,"L":1,"HR":1,"BC":1}),
            ("WAREHOUSE_ALARM","Warehouse Alarm",2,{"E":2,"BC":1}),
            ("BRUSH_WINDY","Brush Fire Windy",2,{"E":2,"BC":1}),
            ("SCHOOL_MCI","School MCI",4,{"EMS":2,"E":1,"BC":1}),
            ("FREEWAY_PILEUP","Freeway Pile-Up",3,{"E":2,"EMS":1,"HR":1,"BC":1}),
            ("FUEL_DUMP_RTF","Fuel Dump RTF",3,{"ARFF":2,"BC":1}),
            ("ELEVATOR_STUCK","Elevator Entrapment",1,{"E":1,"HR":1}),
            ("GAS_LEAK_ODOR","Gas Odor Inside",1,{"E":1,"BC":1}),
        ]
        return cls([CallSpec(i,n,t,req) for (i,n,t,req) in data])

    def weighted_sample(self, k: int, allow_dupes: bool, rng: random.Random) -> List[CallSpec]:
        weights = {1:0.35, 2:0.50, 3:0.12, 4:0.03}
        items = self.items[:]
        if allow_dupes:
            pool = []
            for c in items:
                pool.extend([c] * max(1, int(weights.get(c.tier,0.1)*100)))
            return [rng.choice(pool) for _ in range(k)]
        picked = []
        candidates = items[:]
        for _ in range(k):
            if not candidates:
                break
            total = sum(weights.get(c.tier,0.1) for c in candidates)
            r = rng.random() * total
            acc = 0.0
            choice = candidates[0]
            for c in candidates:
                acc += weights.get(c.tier,0.1)
                if r <= acc:
                    choice = c
                    break
            picked.append(choice)
            candidates.remove(choice)
        return picked

def generate_run(seed_hex: str, pool: CallPool, allow_dupes: bool, hard_mode: bool) -> List[CallSpec]:
    rng = random.Random(int(seed_hex, 16))
    calls = pool.weighted_sample(3, allow_dupes, rng)
    if hard_mode:
        upgraded = []
        for c in calls:
            if c.tier < 4:
                upgraded.append(CallSpec(c.id, c.name + "*", min(4, c.tier + 1), c.requirements, c.oversupply_penalty))
            else:
                upgraded.append(c)
        calls = upgraded
    return calls

def score_run(calls: List[CallSpec], state: dict, hard_mode: bool = False) -> Tuple[int, List[Tuple[int,str]], bool]:
    total = 0
    breakdown: List[Tuple[int,str]] = []
    speed_threshold = 30
    per_call_times = state.get("per_call_time_s") or [None]*len(calls)
    allocs = state.get("allocs", {})
    all_perfect = True
    
    for idx, call in enumerate(calls):
        req = call.requirements
        alloc = allocs.get(str(idx), {}) or {}
        points = 0
        detail_parts = []
        
        for role, need in req.items():
            got = int(alloc.get(role, 0))
            matched = min(got, need)
            points += matched * 3
        
        if call.oversupply_penalty:
            oversupply_penalty_multiplier = 3 if hard_mode else 2
            for role in ROLES:
                got = int(alloc.get(role, 0))
                need = req.get(role, 0)
                if got > need:
                    penalty = (got - need) * oversupply_penalty_multiplier
                    points -= penalty
                    if penalty > 0:
                        detail_parts.append(f"{role} oversupply -{penalty}")
        
        t = per_call_times[idx]
        if not hard_mode and isinstance(t, (int, float)) and t is not None and t < speed_threshold:
            points += 1
            detail_parts.append(f"speed +1 ({int(t)}s)")
        
        is_perfect = _is_perfect_call(req, alloc)
        if is_perfect:
            points += 4
            detail_parts.append("perfect +4")
        else:
            all_perfect = False
        
        detail_parts.insert(0, f"match {_match_summary(req, alloc)}")
        breakdown.append((max(0, points), "; ".join(detail_parts)))
        total += max(0, points)
    
    return total, breakdown, all_perfect

def is_perfect_run(calls: List[CallSpec], state: dict) -> bool:
    allocs = state.get("allocs", {})
    for idx, call in enumerate(calls):
        if not _is_perfect_call(call.requirements, allocs.get(str(idx), {}) or {}):
            return False
    return True

def _is_perfect_call(req: dict, alloc: dict) -> bool:
    for r, need in req.items():
        if int(alloc.get(r, 0)) != int(need):
            return False
    for r, got in alloc.items():
        if int(got) > 0 and r not in req:
            return False
    return True

def _match_summary(req: dict, alloc: dict) -> str:
    parts = []
    for r in ROLES:
        need = req.get(r, 0)
        got = int(alloc.get(r, 0))
        if need or got:
            parts.append(f"{r}:{got}/{need}")
    return ", ".join(parts) if parts else "none"

class RoleSelect(Select):
    def __init__(self, role_code: str, current_qty: int):
        opts = [discord.SelectOption(label=str(i), value=str(i), default=(i==current_qty)) for i in range(0,5)]
        super().__init__(placeholder=f"{role_code} (0-4)", min_values=1, max_values=1, options=opts)
        self.role_code = role_code

class ConfirmButton(Button):
    def __init__(self, is_last: bool = False):
        label = "✓ Bevestig" if is_last else "✓ Volgende"
        super().__init__(style=discord.ButtonStyle.success, label=label)

class CancelButton(Button):
    def __init__(self):
        super().__init__(style=discord.ButtonStyle.danger, label="❌ Cancel")

class RouletteView(View):
    def __init__(self, cog, ctx, state: dict, only_user_id: int, timeout: float = 15*60):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.ctx = ctx
        self.state = state
        self.only_user_id = only_user_id
        self.started_call_ts = now_utc_ts()
        self.last_interaction_ts = now_utc_ts()
        self._build_for_current()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.only_user_id:
            await interaction.response.send_message("❌ Niet jouw run.", ephemeral=True)
            return False
        now = now_utc_ts()
        if now - self.last_interaction_ts < 1.5:
            await interaction.response.send_message("⏳ Te snel!", ephemeral=True)
            return False
        self.last_interaction_ts = now
        try:
            expires_at = int(self.state.get("expires_at", 0))
        except (ValueError, TypeError):
            await interaction.response.send_message("❌ State corrupted.", ephemeral=True)
            return False
        if now_utc_ts() > expires_at:
            await interaction.response.send_message("⏱️ TTL verlopen.", ephemeral=True)
            return False
        return True

    def _build_for_current(self):
        for child in list(self.children):
            self.remove_item(child)

        try:
            idx = int(self.state.get("current_idx", 0))
            calls = [CallSpec.from_json(d) for d in self.state["calls"]]
            call = calls[idx]
            alloc = self.state["allocs"].get(str(idx), {}) or {}
        except (ValueError, KeyError, IndexError):
            return
        
        # Add 7 role selects with explicit row assignment
        # Row 0: E, L, HR (3 items)
        # Row 1: BC, EMS, USAR (3 items)
        # Row 2: ARFF (1 item)
        for i, role in enumerate(ROLES):
            current = int(alloc.get(role, 0))
            sel = RoleSelect(role, current)
            sel.callback = self._on_select
            # Explicit row assignment - this is the FIX
            sel.row = 0 if i < 3 else (1 if i < 6 else 2)
            self.add_item(sel)
        
        # Row 3: Confirm button
        is_last = idx >= len(calls) - 1
        confirm = ConfirmButton(is_last=is_last)
        confirm.callback = self._on_confirm
        confirm.row = 3
        self.add_item(confirm)
        
        # Row 4: Cancel button
        cancel = CancelButton()
        cancel.callback = self._on_cancel
        cancel.row = 4
        self.add_item(cancel)

    async def _on_select(self, interaction: discord.Interaction):
        idx = str(int(self.state.get("current_idx", 0)))
        comp = interaction.component
        role_code = getattr(comp, "role_code", "E")
        qty = int(comp.values[0])
        alloc = self.state["allocs"].get(idx, {}) or {}
        alloc[role_code] = qty
        self.state["allocs"][idx] = alloc
        await self.cog.config.member(self.ctx.author).active_run.set(self.state)
        await interaction.response.defer()

    async def _on_confirm(self, interaction: discord.Interaction):
        idx = int(self.state.get("current_idx", 0))
        calls = [CallSpec.from_json(d) for d in self.state["calls"]]
        elapsed = now_utc_ts() - self.started_call_ts
        times = self.state.get("per_call_time_s") or [None]*len(self.state["calls"])
        times[idx] = int(elapsed)
        self.state["per_call_time_s"] = times
        is_last = idx >= len(calls) - 1
        
        if not is_last:
            self.state["current_idx"] = idx + 1
            self.started_call_ts = now_utc_ts()
            await self.cog.config.member(self.ctx.author).active_run.set(self.state)
            self._build_for_current()
            next_call = calls[idx + 1]
            await interaction.response.edit_message(
                content=f"✅ Call {idx+1}/{len(calls)} done ({elapsed}s)\n\n**Call {idx+2}/{len(calls)}: {next_call.name}**\nVereist: {next_call.requirements_str()}",
                view=self
            )
        else:
            await self.cog.config.member(self.ctx.author).active_run.set(self.state)
            hard_mode = self.state.get("hard_mode", False)
            score, breakdown, is_perfect = score_run(calls, self.state, hard_mode)
            summary = [f"✅ Done! Score: {score} pts"]
            if is_perfect:
                summary.append("🌟 PERFECT!")
            summary.append("\nUse `/roulette claim`")
            await interaction.response.edit_message(content="\n".join(summary), view=None)
            self.stop()

    async def _on_cancel(self, interaction: discord.Interaction):
        await self.cog.config.member(self.ctx.author).active_run.clear()
        await interaction.response.edit_message(content="❌ Cancelled", view=None)
        self.stop()

    async def on_timeout(self) -> None:
        pass
