"""
Database Management for FAQ System
Handles SQLite storage with CRUD operations and version history.
"""

import aiosqlite
import json
import time
from pathlib import Path
from typing import Optional, List, Dict, Any
from .models import FAQItem, FAQVersion


class FAQDatabase:
    """
    Manages FAQ storage with SQLite backend.
    Supports CRUD operations, soft delete, and version history.
    """
    
    def __init__(self, db_path: Path):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
    
    async def initialize(self):
        """Create database tables if they don't exist."""
        async with aiosqlite.connect(self.db_path) as db:
            # Main FAQ table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS faqs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    question TEXT NOT NULL,
                    answer_md TEXT NOT NULL,
                    category TEXT,
                    synonyms_json TEXT,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    author_id INTEGER NOT NULL,
                    source TEXT DEFAULT 'custom',
                    is_deleted INTEGER DEFAULT 0
                )
            """)
            
            # Version history table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS faq_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    faq_id INTEGER NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    saved_at INTEGER NOT NULL,
                    editor_id INTEGER NOT NULL,
                    FOREIGN KEY (faq_id) REFERENCES faqs(id)
                )
            """)
            
            # Create indexes for better performance
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_faqs_deleted 
                ON faqs(is_deleted)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_faqs_category 
                ON faqs(category)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_versions_faq_id 
                ON faq_versions(faq_id)
            """)
            
            await db.commit()
    
    async def add_faq(self, faq: FAQItem) -> int:
        """
        Add a new FAQ to the database.
        
        Args:
            faq: FAQItem to add
            
        Returns:
            ID of the newly created FAQ
        """
        current_time = int(time.time())
        faq.created_at = current_time
        faq.updated_at = current_time
        
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                INSERT INTO faqs 
                (question, answer_md, category, synonyms_json, created_at, updated_at, author_id, source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                faq.question,
                faq.answer_md,
                faq.category,
                json.dumps(faq.synonyms),
                faq.created_at,
                faq.updated_at,
                faq.author_id,
                'custom'
            ))
            await db.commit()
            return cursor.lastrowid
    
    async def get_faq(self, faq_id: int, include_deleted: bool = False) -> Optional[FAQItem]:
        """
        Retrieve a FAQ by ID.
        
        Args:
            faq_id: FAQ ID to retrieve
            include_deleted: Whether to include soft-deleted FAQs
            
        Returns:
            FAQItem or None if not found
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            query = "SELECT * FROM faqs WHERE id = ?"
            params = [faq_id]
            
            if not include_deleted:
                query += " AND is_deleted = 0"
            
            cursor = await db.execute(query, params)
            row = await cursor.fetchone()
            
            if row:
                return self._row_to_faq(row)
            return None
    
    async def get_all_faqs(self, include_deleted: bool = False) -> List[FAQItem]:
        """
        Retrieve all FAQs from the database.
        
        Args:
            include_deleted: Whether to include soft-deleted FAQs
            
        Returns:
            List of FAQItem objects
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            query = "SELECT * FROM faqs"
            if not include_deleted:
                query += " WHERE is_deleted = 0"
            query += " ORDER BY updated_at DESC"
            
            cursor = await db.execute(query)
            rows = await cursor.fetchall()
            
            return [self._row_to_faq(row) for row in rows]
    
    async def update_faq(self, faq: FAQItem, editor_id: int) -> bool:
        """
        Update an existing FAQ and create a version snapshot.
        
        Args:
            faq: FAQItem with updated data (must have ID)
            editor_id: Discord user ID making the edit
            
        Returns:
            True if successful, False otherwise
        """
        if not faq.id:
            return False
        
        # Get current version for snapshot
        current = await self.get_faq(faq.id)
        if not current:
            return False
        
        # Create version snapshot
        await self._save_version(current, editor_id)
        
        # Update FAQ
        faq.updated_at = int(time.time())
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                UPDATE faqs 
                SET question = ?, answer_md = ?, category = ?, 
                    synonyms_json = ?, updated_at = ?
                WHERE id = ?
            """, (
                faq.question,
                faq.answer_md,
                faq.category,
                json.dumps(faq.synonyms),
                faq.updated_at,
                faq.id
            ))
            await db.commit()
            return True
    
    async def delete_faq(self, faq_id: int, soft: bool = True) -> bool:
        """
        Delete a FAQ (soft or hard delete).
        
        Args:
            faq_id: ID of FAQ to delete
            soft: If True, mark as deleted; if False, remove from DB
            
        Returns:
            True if successful, False otherwise
        """
        async with aiosqlite.connect(self.db_path) as db:
            if soft:
                # Soft delete - mark as deleted
                await db.execute("""
                    UPDATE faqs SET is_deleted = 1 WHERE id = ?
                """, (faq_id,))
            else:
                # Hard delete - remove from database
                await db.execute("DELETE FROM faqs WHERE id = ?", (faq_id,))
            
            await db.commit()
            return True
    
    async def search_faqs(self, query: str, category: Optional[str] = None) -> List[FAQItem]:
        """
        Simple text search in FAQs (for basic fallback).
        Use fuzzy_search.py for advanced searching.
        
        Args:
            query: Search query
            category: Optional category filter
            
        Returns:
            List of matching FAQItems
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            sql = """
                SELECT * FROM faqs 
                WHERE is_deleted = 0 
                AND (question LIKE ? OR answer_md LIKE ? OR synonyms_json LIKE ?)
            """
            params = [f"%{query}%", f"%{query}%", f"%{query}%"]
            
            if category:
                sql += " AND category = ?"
                params.append(category)
            
            sql += " ORDER BY updated_at DESC LIMIT 20"
            
            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()
            
            return [self._row_to_faq(row) for row in rows]
    
    async def _save_version(self, faq: FAQItem, editor_id: int):
        """
        Save a version snapshot of a FAQ.
        
        Args:
            faq: FAQItem to snapshot
            editor_id: User making the edit
        """
        snapshot = faq.to_dict()
        version = FAQVersion(
            faq_id=faq.id,
            snapshot=snapshot,
            saved_at=int(time.time()),
            editor_id=editor_id
        )
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO faq_versions (faq_id, snapshot_json, saved_at, editor_id)
                VALUES (?, ?, ?, ?)
            """, (
                version.faq_id,
                json.dumps(version.snapshot),
                version.saved_at,
                version.editor_id
            ))
            await db.commit()
    
    async def get_versions(self, faq_id: int, limit: int = 10) -> List[FAQVersion]:
        """
        Get version history for a FAQ.
        
        Args:
            faq_id: FAQ ID
            limit: Maximum number of versions to retrieve
            
        Returns:
            List of FAQVersion objects, newest first
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            cursor = await db.execute("""
                SELECT * FROM faq_versions 
                WHERE faq_id = ? 
                ORDER BY saved_at DESC 
                LIMIT ?
            """, (faq_id, limit))
            
            rows = await cursor.fetchall()
            
            versions = []
            for row in rows:
                versions.append(FAQVersion(
                    id=row['id'],
                    faq_id=row['faq_id'],
                    snapshot=json.loads(row['snapshot_json']),
                    saved_at=row['saved_at'],
                    editor_id=row['editor_id']
                ))
            
            return versions
    
    def _row_to_faq(self, row: aiosqlite.Row) -> FAQItem:
        """Convert database row to FAQItem."""
        synonyms = json.loads(row['synonyms_json']) if row['synonyms_json'] else []
        
        return FAQItem(
            id=row['id'],
            question=row['question'],
            answer_md=row['answer_md'],
            category=row['category'],
            synonyms=synonyms,
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            author_id=row['author_id'],
            is_deleted=bool(row['is_deleted'])
        )
    
    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with stats (total FAQs, deleted, categories, etc.)
        """
        async with aiosqlite.connect(self.db_path) as db:
            # Total FAQs
            cursor = await db.execute("SELECT COUNT(*) FROM faqs WHERE is_deleted = 0")
            total = (await cursor.fetchone())[0]
            
            # Deleted FAQs
            cursor = await db.execute("SELECT COUNT(*) FROM faqs WHERE is_deleted = 1")
            deleted = (await cursor.fetchone())[0]
            
            # Categories
            cursor = await db.execute("""
                SELECT category, COUNT(*) as count 
                FROM faqs 
                WHERE is_deleted = 0 AND category IS NOT NULL
                GROUP BY category
            """)
            categories = {row[0]: row[1] for row in await cursor.fetchall()}
            
            # Total versions
            cursor = await db.execute("SELECT COUNT(*) FROM faq_versions")
            versions = (await cursor.fetchone())[0]
            
            return {
                'total_faqs': total,
                'deleted_faqs': deleted,
                'categories': categories,
                'total_versions': versions
            }


# Test functions for development
async def _test_database():
    """Test database operations."""
    import tempfile
    import os
    
    # Create temporary database
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_faq.db"
    
    try:
        db = FAQDatabase(db_path)
        await db.initialize()
        print("✓ Database initialized")
        
        # Test 1: Add FAQ
        faq = FAQItem(
            question="What is ARR?",
            answer_md="Alarm and Response Regulation system",
            category="Game Mechanics",
            synonyms=["arr", "alarm rules"],
            author_id=123456789
        )
        faq_id = await db.add_faq(faq)
        assert faq_id > 0
        print(f"✓ Test 1: FAQ added with ID {faq_id}")
        
        # Test 2: Retrieve FAQ
        retrieved = await db.get_faq(faq_id)
        assert retrieved is not None
        assert retrieved.question == "What is ARR?"
        print("✓ Test 2: FAQ retrieved successfully")
        
        # Test 3: Update FAQ
        retrieved.answer_md = "Updated answer about ARR"
        success = await db.update_faq(retrieved, editor_id=987654321)
        assert success
        print("✓ Test 3: FAQ updated")
        
        # Test 4: Version history
        versions = await db.get_versions(faq_id)
        assert len(versions) > 0
        print(f"✓ Test 4: {len(versions)} version(s) saved")
        
        # Test 5: Get all FAQs
        all_faqs = await db.get_all_faqs()
        assert len(all_faqs) > 0
        print(f"✓ Test 5: Retrieved {len(all_faqs)} FAQ(s)")
        
        # Test 6: Search
        results = await db.search_faqs("ARR")
        assert len(results) > 0
        print(f"✓ Test 6: Search found {len(results)} result(s)")
        
        # Test 7: Soft delete
        await db.delete_faq(faq_id, soft=True)
        deleted_check = await db.get_faq(faq_id)
        assert deleted_check is None
        print("✓ Test 7: Soft delete successful")
        
        # Test 8: Statistics
        stats = await db.get_statistics()
        assert 'total_faqs' in stats
        print(f"✓ Test 8: Statistics: {stats}")
        
        print("\n✅ All database tests passed!")
        
    finally:
        # Cleanup
        if db_path.exists():
            os.remove(db_path)
        os.rmdir(temp_dir)


if __name__ == "__main__":
    import asyncio
    asyncio.run(_test_database())
