import discord
from typing import Dict, Any, List
from datetime import datetime

# Color scheme
COLORS = {
    'vehicle': 0xFF0000,      # Red
    'building': 0x0000FF,     # Blue
    'equipment': 0x00FF00,    # Green
    'education': 0xFFFF00,    # Yellow
    'success': 0x00FF00,      # Green
    'error': 0xFF0000,        # Red
    'info': 0x3498DB,         # Blue
    'warning': 0xFFA500       # Orange
}


def format_number(num: int) -> str:
    """Format number with comma separators."""
    if num is None:
        return "N/A"
    return f"{num:,}"


def format_price(price: int) -> str:
    """Format price with $ and commas."""
    if price is None:
        return "N/A"
    return f"${price:,}"


def create_vehicle_embed(vehicle: Dict[str, Any], buildings: List[Dict] = None, educations: List[Dict] = None) -> discord.Embed:
    """Create detailed embed for a vehicle."""
    
    embed = discord.Embed(
        title=f"🚒 {vehicle['name']}",
        color=COLORS['vehicle'],
        timestamp=datetime.utcnow()
    )
    
    # Basic Info
    embed.add_field(
        name="💰 Price",
        value=format_price(vehicle.get('price')),
        inline=True
    )
    
    # Personnel
    min_p = vehicle.get('min_personnel')
    max_p = vehicle.get('max_personnel')
    if min_p is not None or max_p is not None:
        personnel_text = f"Min: {min_p or 'N/A'} | Max: {max_p or 'N/A'}"
        embed.add_field(
            name="👥 Personnel",
            value=personnel_text,
            inline=True
        )
    
    # Water capabilities
    water_info = []
    if vehicle.get('water_tank'):
        water_info.append(f"💧 Tank: {format_number(vehicle['water_tank'])} gal")
    if vehicle.get('foam_tank'):
        water_info.append(f"🧴 Foam: {format_number(vehicle['foam_tank'])} gal")
    if vehicle.get('pump_capacity'):
        water_info.append(f"⚡ Pump: {format_number(vehicle['pump_capacity'])} GPM")
    
    if water_info:
        embed.add_field(
            name="💦 Water Capabilities",
            value="\n".join(water_info),
            inline=False
        )
    
    # Specials
    specials = vehicle.get('specials', {})
    if specials and any(specials.values()):
        special_text = []
        if specials.get('pumpType'):
            special_text.append(f"Pump Type: {specials['pumpType']}")
        if specials.get('special'):
            special_text.append(f"Special: {specials['special']}")
        if specials.get('wtank'):
            special_text.append(f"Wtank: {specials['wtank']}")
        
        if special_text:
            embed.add_field(
                name="⭐ Special Features",
                value="\n".join(special_text),
                inline=False
            )
    
    # Required Buildings
    if buildings:
        building_list = [f"• {b['name']}" for b in buildings[:10]]
        if len(buildings) > 10:
            building_list.append(f"... and {len(buildings) - 10} more")
        
        embed.add_field(
            name=f"🏢 Possible Buildings ({len(buildings)})",
            value="\n".join(building_list) if building_list else "No buildings specified",
            inline=False
        )
    
    # Required Education
    if educations:
        edu_list = [f"• {e['name']}" for e in educations[:5]]
        if len(educations) > 5:
            edu_list.append(f"... and {len(educations) - 5} more")
        
        embed.add_field(
            name=f"🎓 Required Training ({len(educations)})",
            value="\n".join(edu_list) if edu_list else "No training required",
            inline=False
        )
    
    embed.set_footer(text=f"Vehicle ID: {vehicle['game_id']}")
    
    return embed


def create_building_embed(building: Dict[str, Any]) -> discord.Embed:
    """Create embed for a building."""
    
    embed = discord.Embed(
        title=f"🏢 {building['name']}",
        description=building.get('caption', ''),
        color=COLORS['building'],
        timestamp=datetime.utcnow()
    )
    
    embed.set_footer(text=f"Building ID: {building['game_id']}")
    
    return embed


def create_equipment_embed(equipment: Dict[str, Any]) -> discord.Embed:
    """Create embed for equipment."""
    
    embed = discord.Embed(
        title=f"🎒 {equipment['name']}",
        color=COLORS['equipment'],
        timestamp=datetime.utcnow()
    )
    
    # Price
    if equipment.get('credits'):
        embed.add_field(
            name="💰 Price",
            value=format_price(equipment['credits']),
            inline=True
        )
    
    if equipment.get('coins'):
        embed.add_field(
            name="🪙 Coins",
            value=str(equipment['coins']),
            inline=True
        )
    
    # Size
    if equipment.get('size'):
        embed.add_field(
            name="📏 Size",
            value=str(equipment['size']),
            inline=True
        )
    
    # Staff requirements
    min_staff = equipment.get('min_staff')
    max_staff = equipment.get('max_staff')
    
    if min_staff is not None or max_staff is not None:
        if max_staff:
            staff_text = f"Min: {min_staff or 0} | Max: {max_staff}"
        else:
            staff_text = f"Min: {min_staff or 0}"
        
        embed.add_field(
            name="👥 Staff Required",
            value=staff_text,
            inline=True
        )
    
    embed.set_footer(text=f"Equipment ID: {equipment['game_id']}")
    
    return embed


def create_education_embed(education: Dict[str, Any]) -> discord.Embed:
    """Create embed for education/training."""
    
    embed = discord.Embed(
        title=f"🎓 {education['name']}",
        color=COLORS['education'],
        timestamp=datetime.utcnow()
    )
    
    # Building type
    if education.get('building_type'):
        embed.add_field(
            name="🏢 Training Location",
            value=education['building_type'],
            inline=True
        )
    
    # Duration
    if education.get('duration'):
        embed.add_field(
            name="⏱️ Duration",
            value=education['duration'],
            inline=True
        )
    
    # Cost
    if education.get('cost'):
        embed.add_field(
            name="💰 Cost",
            value=format_price(education['cost']),
            inline=True
        )
    
    # Key (internal identifier)
    if education.get('key'):
        embed.add_field(
            name="🔑 Key",
            value=f"`{education['key']}`",
            inline=False
        )
    
    embed.set_footer(text=f"Training ID: {education['game_id']}")
    
    return embed


def create_comparison_embed(vehicles: List[Dict[str, Any]]) -> discord.Embed:
    """Create comparison embed for 2-3 vehicles."""
    
    embed = discord.Embed(
        title="🔍 Vehicle Comparison",
        color=COLORS['info'],
        timestamp=datetime.utcnow()
    )
    
    if len(vehicles) > 3:
        vehicles = vehicles[:3]
    
    # Header with vehicle names
    names = " **vs** ".join([v['name'] for v in vehicles])
    embed.description = f"**Comparing:** {names}\n_ _"
    
    # Create individual columns for each vehicle
    for i, vehicle in enumerate(vehicles):
        column_lines = []
        
        # Name as header
        column_lines.append(f"**{vehicle['name']}**")
        column_lines.append("─────────")
        
        # Price
        column_lines.append(f"💰 {format_price(vehicle.get('price'))}")
        
        # Personnel
        min_p = vehicle.get('min_personnel', 'N/A')
        max_p = vehicle.get('max_personnel', 'N/A')
        column_lines.append(f"👥 {min_p}-{max_p}")
        
        # Water tank
        tank = vehicle.get('water_tank')
        column_lines.append(f"💧 {format_number(tank) + ' gal' if tank else 'None'}")
        
        # Foam tank
        foam = vehicle.get('foam_tank')
        column_lines.append(f"🧴 {format_number(foam) + ' gal' if foam else 'None'}")
        
        # Pump
        pump = vehicle.get('pump_capacity')
        column_lines.append(f"⚡ {format_number(pump) + ' GPM' if pump else 'None'}")
        
        # Add as inline field
        embed.add_field(
            name=f"Vehicle {i+1}",
            value="\n".join(column_lines),
            inline=True
        )
    
    # Add separator if needed
    if len(vehicles) == 2:
        embed.add_field(name="_ _", value="_ _", inline=True)
    
    # Add best value indicators at the bottom
    best_value = []
    
    # Find cheapest
    prices_numeric = [v.get('price', float('inf')) for v in vehicles]
    if any(p < float('inf') for p in prices_numeric):
        cheapest_idx = prices_numeric.index(min(p for p in prices_numeric if p < float('inf')))
        best_value.append(f"💰 Cheapest: **{vehicles[cheapest_idx]['name']}**")
    
    # Find largest water tank
    water_numeric = [v.get('water_tank', 0) or 0 for v in vehicles]
    if any(w > 0 for w in water_numeric):
        largest_water_idx = water_numeric.index(max(water_numeric))
        best_value.append(f"💧 Largest Tank: **{vehicles[largest_water_idx]['name']}**")
    
    # Find highest pump capacity
    pump_numeric = [v.get('pump_capacity', 0) or 0 for v in vehicles]
    if any(p > 0 for p in pump_numeric):
        highest_pump_idx = pump_numeric.index(max(pump_numeric))
        best_value.append(f"⚡ Highest Pump: **{vehicles[highest_pump_idx]['name']}**")
    
    if best_value:
        embed.add_field(
            name="🏆 Best In Category",
            value=" • ".join(best_value),
            inline=False
        )
    
    return embed


def create_list_embed(items: List[Dict[str, Any]], item_type: str, page: int, total_pages: int) -> discord.Embed:
    """Create paginated list embed."""
    
    color_map = {
        'vehicle': COLORS['vehicle'],
        'building': COLORS['building'],
        'equipment': COLORS['equipment'],
        'education': COLORS['education']
    }
    
    embed = discord.Embed(
        title=f"📋 {item_type.title()} List",
        color=color_map.get(item_type.lower(), COLORS['info']),
        timestamp=datetime.utcnow()
    )
    
    item_list = []
    for item in items:
        name = item.get('name', 'Unknown')
        game_id = item.get('game_id', '?')
        
        # Add extra info based on type
        if item_type.lower() == 'vehicle':
            price = format_price(item.get('price'))
            item_list.append(f"`{game_id}` - **{name}** ({price})")
        else:
            item_list.append(f"`{game_id}` - **{name}**")
    
    embed.description = "\n".join(item_list) if item_list else "No items found"
    embed.set_footer(text=f"Page {page}/{total_pages} • Total items: {len(items)}")
    
    return embed


def create_sync_changelog_embed(changes: Dict[str, Any], source: str) -> discord.Embed:
    """Create embed for sync changelog."""
    
    embed = discord.Embed(
        title=f"🔄 {source.title()} Data Updated",
        color=COLORS['success'],
        timestamp=datetime.utcnow()
    )
    
    added = changes.get('added', [])
    updated = changes.get('updated', [])
    removed = changes.get('removed', [])
    
    if added:
        embed.add_field(
            name=f"➕ Added ({len(added)})",
            value=f"{len(added)} new {source}",
            inline=True
        )
    
    if updated:
        embed.add_field(
            name=f"🔄 Updated ({len(updated)})",
            value=f"{len(updated)} {source} modified",
            inline=True
        )
    
    if removed:
        embed.add_field(
            name=f"➖ Removed ({len(removed)})",
            value=f"{len(removed)} {source} deleted",
            inline=True
        )
    
    if not any([added, updated, removed]):
        embed.description = "✅ No changes detected - data is up to date"
    
    return embed


def create_error_embed(message: str) -> discord.Embed:
    """Create error embed."""
    
    embed = discord.Embed(
        title="❌ Error",
        description=message,
        color=COLORS['error'],
        timestamp=datetime.utcnow()
    )
    
    return embed


def create_success_embed(message: str) -> discord.Embed:
    """Create success embed."""
    
    embed = discord.Embed(
        title="✅ Success",
        description=message,
        color=COLORS['success'],
        timestamp=datetime.utcnow()
    )
    
    return embed
