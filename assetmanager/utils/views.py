import discord
from typing import List, Dict, Any, Optional


class CompareView(discord.ui.View):
    """Interactive view for comparing vehicles."""
    
    def __init__(self, vehicles: List[Dict[str, Any]], timeout: float = 180):
        super().__init__(timeout=timeout)
        self.vehicles = vehicles
        self.selected_vehicles = []
        
        # Create select menus
        self.add_item(VehicleSelect(vehicles, placeholder="Select first vehicle", row=0))
        self.add_item(VehicleSelect(vehicles, placeholder="Select second vehicle", row=1))
        self.add_item(VehicleSelect(vehicles, placeholder="Select third vehicle (optional)", row=2))
        
        # Add compare button
        self.compare_button = CompareButton(row=3)
        self.add_item(self.compare_button)
        
        # Add clear button
        self.clear_button = ClearButton(row=3)
        self.add_item(self.clear_button)
    
    async def on_timeout(self):
        """Disable all items when view times out."""
        for item in self.children:
            item.disabled = True


class VehicleSelect(discord.ui.Select):
    """Select menu for choosing a vehicle."""
    
    def __init__(self, vehicles: List[Dict[str, Any]], placeholder: str, row: int):
        # Discord select menus can have max 25 options
        # Sort vehicles by name
        sorted_vehicles = sorted(vehicles, key=lambda v: v['name'])
        
        options = []
        
        # Add "None" option for optional third vehicle (at the top)
        if "optional" in placeholder.lower():
            options.append(discord.SelectOption(
                label="Skip (compare only 2 vehicles)",
                value="none",
                description="Don't select a third vehicle",
                emoji="❌"
            ))
        
        # Add vehicle options (up to 24 if we have "none", or 25 if we don't)
        max_vehicles = 24 if options else 25
        for vehicle in sorted_vehicles[:max_vehicles]:
            price_desc = f"${vehicle.get('price', 0):,}" if vehicle.get('price') else "Price unknown"
            options.append(
                discord.SelectOption(
                    label=vehicle['name'][:100],  # Discord limit
                    value=str(vehicle['game_id']),
                    description=price_desc[:100]  # Discord limit
                )
            )
        
        super().__init__(
            placeholder=placeholder,
            options=options,
            row=row
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle vehicle selection."""
        view: CompareView = self.view
        
        # Store selection
        selected_id = self.values[0]
        
        # Update placeholder to show selection
        if selected_id == "none":
            self.placeholder = "Third vehicle: None"
        else:
            # Find vehicle name
            for vehicle in view.vehicles:
                if str(vehicle['game_id']) == selected_id:
                    self.placeholder = f"Selected: {vehicle['name'][:80]}"
                    break
        
        await interaction.response.edit_message(view=view)


class CompareButton(discord.ui.Button):
    """Button to execute the comparison."""
    
    def __init__(self, row: int):
        super().__init__(
            label="Compare Vehicles",
            style=discord.ButtonStyle.primary,
            emoji="🔍",
            row=row
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Execute comparison."""
        view: CompareView = self.view
        
        # Get all selected vehicles
        selected_ids = []
        for item in view.children:
            if isinstance(item, VehicleSelect) and item.values:
                if item.values[0] != "none":
                    selected_ids.append(item.values[0])
        
        if len(selected_ids) < 2:
            await interaction.response.send_message(
                "❌ Please select at least 2 vehicles to compare.",
                ephemeral=True
            )
            return
        
        # Get vehicle objects
        selected_vehicles = []
        for vid in selected_ids:
            for vehicle in view.vehicles:
                if str(vehicle['game_id']) == vid:
                    selected_vehicles.append(vehicle)
                    break
        
        # Import here to avoid circular import
        from .embeds import create_comparison_embed
        
        # Create comparison embed
        embed = create_comparison_embed(selected_vehicles)
        
        # Send comparison
        await interaction.response.send_message(embed=embed)
        
        # Disable view after comparing
        for item in view.children:
            item.disabled = True
        
        await interaction.message.edit(view=view)


class ClearButton(discord.ui.Button):
    """Button to clear selections."""
    
    def __init__(self, row: int):
        super().__init__(
            label="Clear",
            style=discord.ButtonStyle.secondary,
            emoji="🔄",
            row=row
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Clear all selections."""
        view: CompareView = self.view
        
        # Reset all select menus
        for i, item in enumerate(view.children):
            if isinstance(item, VehicleSelect):
                if i == 0:
                    item.placeholder = "Select first vehicle"
                elif i == 1:
                    item.placeholder = "Select second vehicle"
                elif i == 2:
                    item.placeholder = "Select third vehicle (optional)"
        
        await interaction.response.edit_message(view=view)
