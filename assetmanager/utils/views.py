import discord
from typing import List, Dict, Any, Optional


class CompareView(discord.ui.View):
    """Interactive view for comparing vehicles with category filtering."""
    
    def __init__(self, vehicles: List[Dict[str, Any]], user_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.vehicles = vehicles
        self.user_id = user_id  # Store who opened the menu
        self.selected_categories = [None, None, None]
        self.selected_vehicles = [None, None, None]
        
        # Categorize vehicles
        self.categories = self.categorize_vehicles(vehicles)
        
        self.update_view()
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        """Check if the user is allowed to interact."""
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "❌ Only the person who opened this menu can use it!",
                ephemeral=True
            )
            return False
        return True
    
    def categorize_vehicles(self, vehicles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Categorize vehicles by type based on name patterns."""
        categories = {
            "🚒 Fire & Rescue": [],
            "🚑 EMS & Medical": [],
            "👮 Police & Law": [],
            "🚧 Utility & Support": [],
            "🌊 Water & Marine": [],
            "✈️ Airport & Aviation": [],
            "🚛 Heavy & Specialized": [],
            "📦 Other": []
        }
        
        for vehicle in vehicles:
            name = vehicle['name'].lower()
            categorized = False
            
            # Fire & Rescue
            if any(word in name for word in ['fire', 'engine', 'ladder', 'truck', 'hazmat', 'rescue', 'pumper', 'tanker', 'quint']):
                categories["🚒 Fire & Rescue"].append(vehicle)
                categorized = True
            
            # EMS & Medical
            elif any(word in name for word in ['ambulance', 'ems', 'medical', 'paramedic', 'supervisor', 'fly-car', 'mass casualty']):
                categories["🚑 EMS & Medical"].append(vehicle)
                categorized = True
            
            # Police & Law
            elif any(word in name for word in ['police', 'sheriff', 'k-9', 'k9', 'swat', 'patrol', 'motorcycle', 'supervisor', 'detective', 'warden']):
                categories["👮 Police & Law"].append(vehicle)
                categorized = True
            
            # Water & Marine
            elif any(word in name for word in ['boat', 'rescue boat', 'large rescue', 'water', 'swift water', 'lifeguard']):
                categories["🌊 Water & Marine"].append(vehicle)
                categorized = True
            
            # Airport & Aviation
            elif any(word in name for word in ['airport', 'arff', 'crash tender', 'aircraft']):
                categories["✈️ Airport & Aviation"].append(vehicle)
                categorized = True
            
            # Heavy & Specialized
            elif any(word in name for word in ['heavy', 'dozer', 'tow', 'wrecker', 'rotator', 'crane', 'mobile command']):
                categories["🚛 Heavy & Specialized"].append(vehicle)
                categorized = True
            
            # Utility & Support
            elif any(word in name for word in ['utility', 'pickup', 'battalion', 'chief', 'command', 'crew carrier', 'transport']):
                categories["🚧 Utility & Support"].append(vehicle)
                categorized = True
            
            # Other
            if not categorized:
                categories["📦 Other"].append(vehicle)
        
        # Remove empty categories and sort vehicles within categories
        return {k: sorted(v, key=lambda x: x['name']) for k, v in categories.items() if v}
    
    def update_view(self):
        """Update view with current selections."""
        self.clear_items()
        
        # Row 0: First vehicle - category and vehicle select
        cat_select_0 = CategorySelect(0, self.categories, self.selected_categories[0], row=0, optional=False)
        self.add_item(cat_select_0)
        
        if self.selected_categories[0] and self.selected_categories[0] != "none":
            vehicles = self.categories.get(self.selected_categories[0], [])
            if vehicles:
                veh_select_0 = VehicleSelect(0, vehicles, self.selected_vehicles[0], row=0)
                self.add_item(veh_select_0)
        
        # Row 1: Second vehicle - category and vehicle select
        cat_select_1 = CategorySelect(1, self.categories, self.selected_categories[1], row=1, optional=False)
        self.add_item(cat_select_1)
        
        if self.selected_categories[1] and self.selected_categories[1] != "none":
            vehicles = self.categories.get(self.selected_categories[1], [])
            if vehicles:
                veh_select_1 = VehicleSelect(1, vehicles, self.selected_vehicles[1], row=1)
                self.add_item(veh_select_1)
        
        # Row 2: Third vehicle (optional) - category and vehicle select
        cat_select_2 = CategorySelect(2, self.categories, self.selected_categories[2], row=2, optional=True)
        self.add_item(cat_select_2)
        
        if self.selected_categories[2] and self.selected_categories[2] != "none":
            vehicles = self.categories.get(self.selected_categories[2], [])
            if vehicles:
                veh_select_2 = VehicleSelect(2, vehicles, self.selected_vehicles[2], row=2)
                self.add_item(veh_select_2)
        
        # Row 4: Action buttons (skip row 3 to avoid conflicts)
        compare_btn = CompareButton(row=4)
        self.add_item(compare_btn)
        
        clear_btn = ClearButton(row=4)
        self.add_item(clear_btn)
    
    async def on_timeout(self):
        """Disable all items when view times out."""
        for item in self.children:
            item.disabled = True


class CategorySelect(discord.ui.Select):
    """Select menu for choosing a vehicle category."""
    
    def __init__(self, selector_index: int, categories: Dict[str, List], current_selection: Optional[str], row: int, optional: bool = False):
        options = []
        
        # Add skip option for third selector
        if optional:
            options.append(discord.SelectOption(
                label="Skip third vehicle",
                value="none",
                description="Compare only 2 vehicles",
                emoji="❌"
            ))
        
        # Add category options
        for category_name, vehicles in categories.items():
            emoji = category_name.split()[0]  # Get emoji from category name
            label = category_name.split(maxsplit=1)[1] if len(category_name.split()) > 1 else category_name
            
            is_default = current_selection == category_name
            options.append(discord.SelectOption(
                label=label[:100],
                value=category_name,
                description=f"{len(vehicles)} vehicles",
                emoji=emoji,
                default=is_default
            ))
        
        placeholder = f"{'Optional: ' if optional else ''}Step 1: Choose category"
        if current_selection and current_selection != "none":
            placeholder = f"Category: {current_selection.split(maxsplit=1)[1][:50]}"
        elif current_selection == "none":
            placeholder = "Skipped"
        
        super().__init__(
            placeholder=placeholder,
            options=options,
            row=row,
            custom_id=f"cat_{selector_index}_{optional}"
        )
        self.selector_index = selector_index
    
    async def callback(self, interaction: discord.Interaction):
        """Handle category selection."""
        try:
            view: CompareView = self.view
            
            # Update selected category
            view.selected_categories[self.selector_index] = self.values[0]
            view.selected_vehicles[self.selector_index] = None  # Reset vehicle selection
            
            # Update view
            view.update_view()
            await interaction.response.edit_message(view=view)
        except Exception as e:
            # If already responded, use followup
            if interaction.response.is_done():
                await interaction.followup.send(f"Error: {str(e)}", ephemeral=True)
            else:
                await interaction.response.send_message(f"Error: {str(e)}", ephemeral=True)


class VehicleSelect(discord.ui.Select):
    """Select menu for choosing a specific vehicle."""
    
    def __init__(self, selector_index: int, vehicles: List[Dict[str, Any]], current_selection: Optional[Dict], row: int):
        options = []
        
        for vehicle in vehicles[:25]:  # Max 25 options
            price = vehicle.get('price', 0)
            price_desc = f"${price:,}" if price else "Free"
            
            is_default = current_selection and current_selection['game_id'] == vehicle['game_id']
            
            options.append(discord.SelectOption(
                label=vehicle['name'][:100],
                value=str(vehicle['game_id']),
                description=price_desc[:100],
                default=is_default
            ))
        
        placeholder = "Step 2: Choose vehicle"
        if current_selection:
            placeholder = f"Selected: {current_selection['name'][:50]}"
        
        super().__init__(
            placeholder=placeholder,
            options=options,
            row=row,
            custom_id=f"veh_{selector_index}"
        )
        self.selector_index = selector_index
        self.vehicles = vehicles
    
    async def callback(self, interaction: discord.Interaction):
        """Handle vehicle selection."""
        try:
            view: CompareView = self.view
            
            # Find selected vehicle
            selected_id = int(self.values[0])
            for vehicle in self.vehicles:
                if vehicle['game_id'] == selected_id:
                    view.selected_vehicles[self.selector_index] = vehicle
                    break
            
            # Update view
            view.update_view()
            await interaction.response.edit_message(view=view)
        except Exception as e:
            if interaction.response.is_done():
                await interaction.followup.send(f"Error: {str(e)}", ephemeral=True)
            else:
                await interaction.response.send_message(f"Error: {str(e)}", ephemeral=True)


class CompareButton(discord.ui.Button):
    """Button to execute the comparison."""
    
    def __init__(self, row: int):
        super().__init__(
            label="Compare Vehicles",
            style=discord.ButtonStyle.primary,
            emoji="🔍",
            row=row,
            custom_id="compare_btn"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Execute comparison."""
        view: CompareView = self.view
        
        # Get all selected vehicles
        selected = [v for v in view.selected_vehicles if v is not None]
        
        if len(selected) < 2:
            await interaction.response.send_message(
                "❌ Please select at least 2 vehicles to compare.",
                ephemeral=True
            )
            return
        
        # Import here to avoid circular import
        from .embeds import create_comparison_embed
        
        # Create comparison embed
        embed = create_comparison_embed(selected)
        
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
            label="Clear All",
            style=discord.ButtonStyle.secondary,
            emoji="🔄",
            row=row,
            custom_id="clear_btn"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Clear all selections."""
        view: CompareView = self.view
        
        # Reset all selections
        view.selected_categories = [None, None, None]
        view.selected_vehicles = [None, None, None]
        
        # Update view
        view.update_view()
        await interaction.response.edit_message(view=view)
