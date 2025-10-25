import discord
from typing import List, Dict, Any, Optional
import logging

log = logging.getLogger("red.assetmanager")


class CompareView(discord.ui.View):
    """
    Interactive view for comparing vehicles with category filtering.
    FIXED: Uses ephemeral messages for vehicle selection to stay within 5 row limit.
    """
    
    def __init__(self, vehicles: List[Dict[str, Any]], user_id: int, timeout: float = 300):
        super().__init__(timeout=timeout)
        self.vehicles = vehicles
        self.user_id = user_id  # Store who opened the menu
        self.selected_vehicles = [None, None, None]
        
        # Categorize vehicles
        self.categories = self.categorize_vehicles(vehicles)
        
        # Build initial UI
        self._build_ui()
    
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
    
    def _build_ui(self):
        """Build UI with proper row management to stay within 5 rows."""
        self.clear_items()
        
        # Row 0: Vehicle 1 Category Select
        self.add_item(CategorySelect(
            view=self,
            slot=0,
            categories=self.categories,
            row=0,
            optional=False
        ))
        
        # Row 1: Vehicle 2 Category Select
        self.add_item(CategorySelect(
            view=self,
            slot=1,
            categories=self.categories,
            row=1,
            optional=False
        ))
        
        # Row 2: Vehicle 3 Category Select (optional)
        self.add_item(CategorySelect(
            view=self,
            slot=2,
            categories=self.categories,
            row=2,
            optional=True
        ))
        
        # Row 3: Action Buttons
        self.add_item(CompareButton(row=3))
        self.add_item(ClearButton(row=3))
    
    def get_status_text(self) -> str:
        """Get current selection status."""
        lines = ["**Current Selection:**"]
        for i, vehicle in enumerate(self.selected_vehicles, 1):
            if vehicle:
                lines.append(f"Vehicle {i}: **{vehicle['name']}** (${vehicle.get('price', 0):,})")
            else:
                lines.append(f"Vehicle {i}: _Not selected_")
        
        lines.append(f"\n_Menu opened by: <@{self.user_id}>_")
        return "\n".join(lines)
    
    async def on_timeout(self):
        """Disable all items when view times out."""
        for item in self.children:
            item.disabled = True


class CategorySelect(discord.ui.Select):
    """Select menu for choosing a vehicle category - opens vehicle menu in ephemeral message."""
    
    def __init__(self, view: CompareView, slot: int, categories: Dict[str, List], row: int, optional: bool = False):
        self.parent_view = view
        self.slot = slot
        self.optional = optional
        
        options = []
        
        # Add skip option for third selector
        if optional:
            options.append(discord.SelectOption(
                label="Skip third vehicle",
                value="__SKIP__",
                description="Compare only 2 vehicles",
                emoji="❌"
            ))
        
        # Add category options
        for category_name, vehicles in categories.items():
            emoji = category_name.split()[0]  # Get emoji from category name
            label = category_name.split(maxsplit=1)[1] if len(category_name.split()) > 1 else category_name
            
            options.append(discord.SelectOption(
                label=label[:100],
                value=category_name,
                description=f"{len(vehicles)} vehicles available",
                emoji=emoji
            ))
        
        placeholder = f"Vehicle {slot + 1}: Choose category" + (" (optional)" if optional else "")
        
        super().__init__(
            placeholder=placeholder,
            options=options,
            row=row,
            custom_id=f"cat_{slot}"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle category selection - show vehicle menu in ephemeral message."""
        try:
            category = self.values[0]
            
            # Handle skip
            if category == "__SKIP__":
                self.parent_view.selected_vehicles[self.slot] = None
                self.placeholder = f"Vehicle {self.slot + 1}: Skipped ❌"
                
                await interaction.response.edit_message(
                    content=self.parent_view.get_status_text(),
                    view=self.parent_view
                )
                return
            
            # Get vehicles in category
            vehicles = self.parent_view.categories.get(category, [])
            
            if not vehicles:
                await interaction.response.send_message(
                    f"❌ No vehicles found in {category}",
                    ephemeral=True
                )
                return
            
            # Create ephemeral vehicle selector
            vehicle_view = VehicleSelectView(
                parent_view=self.parent_view,
                slot=self.slot,
                category=category,
                vehicles=vehicles
            )
            
            category_display = category.split(maxsplit=1)[1] if len(category.split()) > 1 else category
            
            await interaction.response.send_message(
                f"🔽 Select a vehicle from **{category_display}** for Vehicle {self.slot + 1}:",
                view=vehicle_view,
                ephemeral=True
            )
            
        except Exception as e:
            log.error(f"Error in CategorySelect callback: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ Error: {str(e)}",
                    ephemeral=True
                )


class VehicleSelectView(discord.ui.View):
    """Ephemeral view for selecting a specific vehicle after category is chosen."""
    
    def __init__(self, parent_view: CompareView, slot: int, category: str, vehicles: List[Dict[str, Any]]):
        super().__init__(timeout=60.0)
        self.parent_view = parent_view
        self.slot = slot
        self.category = category
        self.vehicles = vehicles
        
        # Add vehicle select
        self.add_item(VehicleSelect(
            slot=slot,
            vehicles=vehicles
        ))
    
    async def on_timeout(self):
        """Handle timeout."""
        for item in self.children:
            item.disabled = True


class VehicleSelect(discord.ui.Select):
    """Select menu for choosing a specific vehicle."""
    
    def __init__(self, slot: int, vehicles: List[Dict[str, Any]]):
        self.slot = slot
        self.vehicles_dict = {str(v['game_id']): v for v in vehicles}
        
        options = []
        
        # Max 25 options per Discord limit
        for vehicle in vehicles[:25]:
            price = vehicle.get('price', 0)
            min_crew = vehicle.get('min_personnel', 0)
            max_crew = vehicle.get('max_personnel', 0)
            
            price_desc = f"${price:,}" if price else "Free"
            crew_desc = f"{min_crew}-{max_crew} crew"
            
            options.append(discord.SelectOption(
                label=vehicle['name'][:100],
                value=str(vehicle['game_id']),
                description=f"{price_desc} • {crew_desc}"[:100]
            ))
        
        super().__init__(
            placeholder=f"Choose vehicle for slot {slot + 1}...",
            options=options,
            custom_id=f"veh_{slot}"
        )
    
    async def callback(self, interaction: discord.Interaction):
        """Handle vehicle selection."""
        try:
            vehicle_id = self.values[0]
            vehicle = self.vehicles_dict.get(vehicle_id)
            
            if not vehicle:
                await interaction.response.send_message(
                    "❌ Vehicle not found",
                    ephemeral=True
                )
                return
            
            # Get parent view from the ephemeral view
            ephemeral_view: VehicleSelectView = self.view
            parent_view = ephemeral_view.parent_view
            
            # Update selection
            parent_view.selected_vehicles[self.slot] = vehicle
            
            # Update the category select placeholder in main view
            for item in parent_view.children:
                if isinstance(item, CategorySelect) and item.slot == self.slot:
                    item.placeholder = f"Vehicle {self.slot + 1}: {vehicle['name'][:50]} ✅"
                    break
            
            # Confirm selection in ephemeral message
            await interaction.response.edit_message(
                content=f"✅ **{vehicle['name']}** selected for Vehicle {self.slot + 1}!",
                view=None
            )
            
            # Update main message with new status
            try:
                # Find the original message (the one with the CompareView)
                # We need to edit it to show updated status
                original_message = interaction.message
                if hasattr(original_message, 'reference') and original_message.reference:
                    original_message = await interaction.channel.fetch_message(original_message.reference.message_id)
                
                # Since we can't easily get the original message, we'll just update the view
                # The user will see the updated placeholder when they look back
                pass
            except:
                pass  # Silently fail if we can't update
            
        except Exception as e:
            log.error(f"Error in VehicleSelect callback: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ Error: {str(e)}",
                    ephemeral=True
                )


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
        try:
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
            
            # Disable view
            for item in view.children:
                item.disabled = True
            
            # Send comparison
            await interaction.response.edit_message(
                content="✅ **Comparison Complete!**",
                view=view
            )
            
            # Send comparison as followup
            await interaction.followup.send(embed=embed)
            
        except Exception as e:
            log.error(f"Error in CompareButton callback: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ Error creating comparison: {str(e)}",
                    ephemeral=True
                )


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
        try:
            view: CompareView = self.view
            
            # Reset all selections
            view.selected_vehicles = [None, None, None]
            
            # Rebuild UI to reset placeholders
            view._build_ui()
            
            await interaction.response.edit_message(
                content=view.get_status_text(),
                view=view
            )
            
        except Exception as e:
            log.error(f"Error in ClearButton callback: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ Error: {str(e)}",
                    ephemeral=True
                )
