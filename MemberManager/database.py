# Dit is de FIXED versie van de relevante functies in database.py

# 1. UPDATE IN _create_tables() functie - Notes table schema:
async def _create_tables(self):
    """Create all database tables."""
    # Notes table - FIXED: added updated_by_name column
    await self._conn.execute("""
        CREATE TABLE IF NOT EXISTS notes (
            note_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ref_code TEXT UNIQUE NOT NULL,
            guild_id INTEGER NOT NULL,
            discord_id INTEGER,
            mc_user_id TEXT,
            note_text TEXT NOT NULL,
            author_id INTEGER NOT NULL,
            author_name TEXT NOT NULL,
            infraction_ref TEXT,
            sanction_ref INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER,
            updated_by INTEGER,
            updated_by_name TEXT,
            expires_at INTEGER,
            content_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            is_pinned INTEGER DEFAULT 0,
            tags TEXT
        )
    """)
    
    # Rest van de tables...
    # (laat de rest van je _create_tables functie intact)


# 2. UPDATE IN update_note() functie - FIXED: now includes updated_by_name
async def update_note(
    self,
    ref_code: str,
    new_text: str,
    updated_by: int,
    updated_by_name: str = None  # 🔧 NEW PARAMETER
) -> bool:
    """Update note text."""
    new_hash = _hash_content(new_text)
    
    # 🔧 FIXED: Now updates both updated_by and updated_by_name
    result = await self._conn.execute(
        """
        UPDATE notes 
        SET note_text=?, updated_at=?, updated_by=?, updated_by_name=?, content_hash=?
        WHERE ref_code=?
        """,
        (new_text, _timestamp(), updated_by, updated_by_name, new_hash, ref_code)
    )
    await self._conn.commit()
    
    return result.rowcount > 0


# 3. MIGRATION - Add this function to your MemberDatabase class:
async def _migrate_database(self):
    """
    Perform database migrations for schema updates.
    Call this AFTER _create_tables() in initialize()
    """
    try:
        # Check if updated_by_name column exists
        cursor = await self._conn.execute("PRAGMA table_info(notes)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        if 'updated_by_name' not in column_names:
            log.info("🔧 MIGRATION: Adding updated_by_name column to notes table")
            await self._conn.execute("ALTER TABLE notes ADD COLUMN updated_by_name TEXT")
            await self._conn.commit()
            log.info("✅ Migration complete: updated_by_name column added")
    except Exception as e:
        log.error(f"Migration error: {e}")


# 4. UPDATE IN initialize() functie:
async def initialize(self):
    """Initialize database and create tables."""
    self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    self._conn = await aiosqlite.connect(self.db_path)
    self._conn.row_factory = aiosqlite.Row
    
    # Execute schema from schema file
    await self._create_tables()
    
    # 🔧 NEW: Run migrations
    await self._migrate_database()
    
    log.info(f"Database initialized at {self.db_path}")


# ==================== VIEWS.PY FIX ====================

# 5. UPDATE IN views.py - EditNoteModal.on_submit():
async def on_submit(self, interaction: discord.Interaction):
    """Handle note edit submission."""
    try:
        # 🔧 FIXED: Now passes updated_by_name
        success = await self.parent_view.db.update_note(
            ref_code=self.ref_code.value,
            new_text=self.new_text.value,
            updated_by=interaction.user.id,
            updated_by_name=str(interaction.user)  # 🔧 NEW: Pass username
        )
        
        if not success:
            await interaction.response.send_message(
                f"❌ Note `{self.ref_code.value}` not found.",
                ephemeral=True
            )
            return
        
        await interaction.response.send_message(
            f"✅ Note `{self.ref_code.value}` updated successfully!",
            ephemeral=True
        )
        
        # Refresh the view
        embed = await self.parent_view.get_notes_embed()
        
        # Update button styles
        for item in self.parent_view.children:
            if isinstance(item, discord.ui.Button) and item.row == 0:
                tab_name = item.custom_id.split(":")[-1]
                if tab_name == "notes":
                    item.style = discord.ButtonStyle.primary
                else:
                    item.style = discord.ButtonStyle.secondary
        
        if self.parent_view.message:
            await self.parent_view.message.edit(embed=embed, view=self.parent_view)
    
    except Exception as e:
        log.error(f"Error editing note: {e}", exc_info=True)
        await interaction.response.send_message(
            f"❌ Failed to edit note: {str(e)}",
            ephemeral=True
        )
