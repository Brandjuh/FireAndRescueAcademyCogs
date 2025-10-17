import discord
from redbot.core import commands
import sqlite3
from pathlib import Path
from datetime import datetime
from redbot.core.data_manager import cog_data_path

class DataOverview(commands.Cog):
    """Shows overview of all scraped data"""
    
    def __init__(self, bot):
        self.bot = bot
        
        # Get database paths
        base_path = cog_data_path(self.bot.get_cog("CookieManager"))
        self.db_dir = base_path.parent / "scraper_databases"
    
    def _get_db_stats(self, db_name):
        """Get statistics from a database"""
        db_path = self.db_dir / db_name
        if not db_path.exists():
            return None
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            stats = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                
                # Get date range
                cursor.execute(f"SELECT MIN(timestamp), MAX(timestamp) FROM {table}")
                min_time, max_time = cursor.fetchone()
                
                stats[table] = {
                    'count': count,
                    'min_time': min_time,
                    'max_time': max_time
                }
            
            conn.close()
            return stats
        except Exception as e:
            return {'error': str(e)}
    
    @commands.command(name="dataoverview")
    @commands.is_owner()
    async def data_overview(self, ctx):
        """Show overview of all scraped data"""
        
        # 1. Members Database
        members_stats = self._get_db_stats("members_v2.db")
        embed1 = discord.Embed(title="📊 Data Overview - Members", color=discord.Color.blue())
        
        if members_stats and 'members' in members_stats:
            m = members_stats['members']
            embed1.add_field(
                name="👥 Members Data",
                value=f"**Records:** {m['count']:,}\n"
                      f"**First:** {m['min_time'][:10] if m['min_time'] else 'N/A'}\n"
                      f"**Last:** {m['max_time'][:10] if m['max_time'] else 'N/A'}",
                inline=False
            )
            
            if 'suspicious_members' in members_stats:
                s = members_stats['suspicious_members']
                embed1.add_field(
                    name="⚠️ Suspicious Entries",
                    value=f"**Records:** {s['count']:,}",
                    inline=False
                )
        else:
            embed1.add_field(name="❌ Status", value="No data or database not found", inline=False)
        
        embed1.set_footer(text="Database: members_v2.db")
        
        # 2. Logs Database
        logs_stats = self._get_db_stats("logs_v2.db")
        embed2 = discord.Embed(title="📊 Data Overview - Logs", color=discord.Color.green())
        
        if logs_stats and 'logs' in logs_stats:
            l = logs_stats['logs']
            embed2.add_field(
                name="📜 Alliance Logs",
                value=f"**Records:** {l['count']:,}\n"
                      f"**First:** {l['min_time'][:10] if l['min_time'] else 'N/A'}\n"
                      f"**Last:** {l['max_time'][:10] if l['max_time'] else 'N/A'}",
                inline=False
            )
            
            if 'training_courses' in logs_stats:
                t = logs_stats['training_courses']
                embed2.add_field(
                    name="🎓 Training Courses",
                    value=f"**Records:** {t['count']:,}\n"
                          f"**First:** {t['min_time'][:10] if t['min_time'] else 'N/A'}\n"
                          f"**Last:** {t['max_time'][:10] if t['max_time'] else 'N/A'}",
                    inline=False
                )
        else:
            embed2.add_field(name="❌ Status", value="No data or database not found", inline=False)
        
        embed2.set_footer(text="Database: logs_v2.db")
        
        # 3. Income Database
        income_stats = self._get_db_stats("income_v2.db")
        embed3 = discord.Embed(title="📊 Data Overview - Income/Expenses", color=discord.Color.gold())
        
        if income_stats and 'income' in income_stats:
            i = income_stats['income']
            
            # Get breakdown by type
            db_path = self.db_dir / "income_v2.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT entry_type, COUNT(*) FROM income GROUP BY entry_type")
            breakdown = dict(cursor.fetchall())
            
            cursor.execute("SELECT period, COUNT(*) FROM income GROUP BY period")
            periods = dict(cursor.fetchall())
            
            conn.close()
            
            embed3.add_field(
                name="💰 Income/Expense Records",
                value=f"**Total Records:** {i['count']:,}\n"
                      f"**Income:** {breakdown.get('income', 0):,}\n"
                      f"**Expenses:** {breakdown.get('expense', 0):,}",
                inline=False
            )
            
            embed3.add_field(
                name="📅 By Period",
                value=f"**Daily:** {periods.get('daily', 0):,}\n"
                      f"**Monthly:** {periods.get('monthly', 0):,}\n"
                      f"**Paginated:** {periods.get('paginated', 0):,}",
                inline=False
            )
            
            embed3.add_field(
                name="🗓️ Date Range",
                value=f"**First:** {i['min_time'][:10] if i['min_time'] else 'N/A'}\n"
                      f"**Last:** {i['max_time'][:10] if i['max_time'] else 'N/A'}",
                inline=False
            )
        else:
            embed3.add_field(name="❌ Status", value="No data or database not found", inline=False)
        
        embed3.set_footer(text="Database: income_v2.db")
        
        # 4. Buildings Database
        buildings_stats = self._get_db_stats("buildings_v2.db")
        embed4 = discord.Embed(title="📊 Data Overview - Buildings", color=discord.Color.purple())
        
        if buildings_stats and 'buildings' in buildings_stats:
            b = buildings_stats['buildings']
            
            # Get unique counts
            db_path = self.db_dir / "buildings_v2.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT COUNT(DISTINCT building_id) FROM buildings")
            unique_buildings = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT owner_name) FROM buildings")
            unique_owners = cursor.fetchone()[0]
            
            cursor.execute("SELECT SUM(classrooms) FROM buildings WHERE timestamp = (SELECT MAX(timestamp) FROM buildings)")
            total_classrooms = cursor.fetchone()[0] or 0
            
            conn.close()
            
            embed4.add_field(
                name="🏢 Buildings Data",
                value=f"**Total Records:** {b['count']:,}\n"
                      f"**Unique Buildings:** {unique_buildings:,}\n"
                      f"**Unique Owners:** {unique_owners:,}\n"
                      f"**Total Classrooms:** {total_classrooms:,}",
                inline=False
            )
            
            embed4.add_field(
                name="🗓️ Date Range",
                value=f"**First:** {b['min_time'][:10] if b['min_time'] else 'N/A'}\n"
                      f"**Last:** {b['max_time'][:10] if b['max_time'] else 'N/A'}",
                inline=False
            )
        else:
            embed4.add_field(name="❌ Status", value="No data or database not found", inline=False)
        
        embed4.set_footer(text="Database: buildings_v2.db")
        
        # 5. Summary
        embed5 = discord.Embed(title="📊 Data Overview - Summary", color=discord.Color.red())
        
        total_records = 0
        databases_found = 0
        
        for stats in [members_stats, logs_stats, income_stats, buildings_stats]:
            if stats:
                databases_found += 1
                for table, data in stats.items():
                    if isinstance(data, dict) and 'count' in data:
                        total_records += data['count']
        
        embed5.add_field(
            name="📈 Overall Statistics",
            value=f"**Databases Active:** {databases_found}/4\n"
                  f"**Total Records:** {total_records:,}\n"
                  f"**Storage Location:** `{self.db_dir}`",
            inline=False
        )
        
        # Database file sizes
        size_info = []
        for db_name in ["members_v2.db", "logs_v2.db", "income_v2.db", "buildings_v2.db"]:
            db_path = self.db_dir / db_name
            if db_path.exists():
                size_mb = db_path.stat().st_size / (1024 * 1024)
                size_info.append(f"**{db_name}:** {size_mb:.2f} MB")
        
        if size_info:
            embed5.add_field(
                name="💾 Database Sizes",
                value="\n".join(size_info),
                inline=False
            )
        
        embed5.add_field(
            name="🤖 Active Scrapers",
            value="✅ Members Scraper\n"
                  "✅ Logs Scraper\n"
                  "✅ Income Scraper\n"
                  "✅ Buildings Scraper",
            inline=False
        )
        
        embed5.set_footer(text=f"Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Send all embeds
        await ctx.send(embed=embed1)
        await ctx.send(embed=embed2)
        await ctx.send(embed=embed3)
        await ctx.send(embed=embed4)
        await ctx.send(embed=embed5)
