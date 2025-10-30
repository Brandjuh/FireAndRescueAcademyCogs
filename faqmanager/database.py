"""
Database Management for FAQ System (v2)
Extended with Helpshift local storage and indexing.
"""

import aiosqlite
import json
import time
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime
from .models import FAQItem, FAQVersion, HelpshiftArticle, HelpshiftSection, ArticleVersion, CrawlReport

log = logging.getLogger("red.faqmanager.database")


class FAQDatabase:
    """
    Manages FAQ storage with SQLite backend.
    Supports both custom FAQs and local Helpshift article storage.
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
            # ==================== CUSTOM FAQ TABLES ====================
            
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
            
            # Version history table for custom FAQs
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
            
            # ==================== HELPSHIFT TABLES ====================
            
            # Sections table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS helpshift_sections (
                    id INTEGER PRIMARY KEY,
                    slug TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL
                )
            """)
            
            # Articles table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS helpshift_articles (
                    id INTEGER PRIMARY KEY,
                    slug TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    title TEXT NOT NULL,
                    section_id INTEGER,
                    section_name TEXT,
                    last_updated_text TEXT,
                    last_seen_utc TEXT NOT NULL,
                    body_md TEXT NOT NULL,
                    hash_body TEXT NOT NULL,
                    lang TEXT NOT NULL DEFAULT 'en',
                    is_deleted INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY (section_id) REFERENCES helpshift_sections(id)
                )
            """)
            
            # Article version history
            await db.execute("""
                CREATE TABLE IF NOT EXISTS article_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    article_id INTEGER NOT NULL,
                    saved_utc TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body_md TEXT NOT NULL,
                    hash_body TEXT NOT NULL,
                    FOREIGN KEY (article_id) REFERENCES helpshift_articles(id)
                )
            """)
            
            # Simple token index for search
            await db.execute("""
                CREATE TABLE IF NOT EXISTS helpshift_index (
                    term TEXT NOT NULL,
                    article_id INTEGER NOT NULL,
                    weight REAL NOT NULL,
                    PRIMARY KEY(term, article_id),
                    FOREIGN KEY (article_id) REFERENCES helpshift_articles(id)
                )
            """)
            
            # Crawl state tracking
            await db.execute("""
                CREATE TABLE IF NOT EXISTS crawl_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
            
            # ==================== INDEXES ====================
            
            # Custom FAQ indexes
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
            
            # Helpshift indexes
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_title 
                ON helpshift_articles(title)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_hash 
                ON helpshift_articles(hash_body)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_deleted 
                ON helpshift_articles(is_deleted)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_section 
                ON helpshift_articles(section_id)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_index_term 
                ON helpshift_index(term)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_article_versions_article_id 
                ON article_versions(article_id)
            """)
            
            await db.commit()
    
    # ==================== CUSTOM FAQ METHODS (unchanged) ====================
    
    async def add_faq(self, faq: FAQItem) -> int:
        """Add a new FAQ to the database."""
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
        """Retrieve a FAQ by ID."""
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
        """Retrieve all FAQs from the database."""
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
        """Update an existing FAQ and create a version snapshot."""
        if not faq.id:
            return False
        
        current = await self.get_faq(faq.id)
        if not current:
            return False
        
        await self._save_faq_version(current, editor_id)
        
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
        """Delete a FAQ (soft or hard delete)."""
        async with aiosqlite.connect(self.db_path) as db:
            if soft:
                await db.execute("""
                    UPDATE faqs SET is_deleted = 1 WHERE id = ?
                """, (faq_id,))
            else:
                await db.execute("DELETE FROM faqs WHERE id = ?", (faq_id,))
            
            await db.commit()
            return True
    
    async def _save_faq_version(self, faq: FAQItem, editor_id: int):
        """Save a version snapshot of a FAQ."""
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
    
    async def get_faq_versions(self, faq_id: int, limit: int = 10) -> List[FAQVersion]:
        """Get version history for a FAQ."""
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
    
    # ==================== HELPSHIFT METHODS (NEW) ====================
    
    async def upsert_section(self, section: HelpshiftSection):
        """Insert or update a Helpshift section."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO helpshift_sections (id, slug, url, name, last_seen_utc)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    slug = excluded.slug,
                    url = excluded.url,
                    name = excluded.name,
                    last_seen_utc = excluded.last_seen_utc
            """, (section.id, section.slug, section.url, section.name, section.last_seen_utc))
            await db.commit()
    
    async def upsert_article(self, article: HelpshiftArticle) -> Tuple[str, bool]:
        """
        Insert or update a Helpshift article with change detection.
        
        Returns:
            Tuple of (status, version_saved)
            status: 'new', 'updated', or 'unchanged'
            version_saved: whether a version snapshot was created
        """
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Check if article exists
            cursor = await db.execute("""
                SELECT id, hash_body, title, section_name FROM helpshift_articles WHERE id = ?
            """, (article.id,))
            existing = await cursor.fetchone()
            
            if existing:
                # Check if content changed
                if existing['hash_body'] == article.hash_body:
                    # Unchanged - just update last_seen and metadata
                    await db.execute("""
                        UPDATE helpshift_articles 
                        SET last_seen_utc = ?,
                            title = ?,
                            section_name = ?,
                            is_deleted = 0
                        WHERE id = ?
                    """, (article.last_seen_utc, article.title, article.section_name, article.id))
                    await db.commit()
                    return ('unchanged', False)
                else:
                    # Changed - save version and update
                    await self._save_article_version(db, existing)
                    
                    await db.execute("""
                        UPDATE helpshift_articles 
                        SET slug = ?, url = ?, title = ?, section_id = ?, section_name = ?,
                            last_updated_text = ?, last_seen_utc = ?, body_md = ?, hash_body = ?,
                            is_deleted = 0
                        WHERE id = ?
                    """, (
                        article.slug, article.url, article.title, article.section_id,
                        article.section_name, article.last_updated_text, article.last_seen_utc,
                        article.body_md, article.hash_body, article.id
                    ))
                    await db.commit()
                    return ('updated', True)
            else:
                # New article
                await db.execute("""
                    INSERT INTO helpshift_articles 
                    (id, slug, url, title, section_id, section_name, last_updated_text,
                     last_seen_utc, body_md, hash_body, lang, is_deleted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, (
                    article.id, article.slug, article.url, article.title, article.section_id,
                    article.section_name, article.last_updated_text, article.last_seen_utc,
                    article.body_md, article.hash_body, article.lang
                ))
                await db.commit()
                return ('new', False)
    
    async def _save_article_version(self, db: aiosqlite.Connection, existing_row: aiosqlite.Row):
        """Save a version snapshot of an article."""
        now_utc = datetime.utcnow().isoformat() + 'Z'
        
        await db.execute("""
            INSERT INTO article_versions (article_id, saved_utc, title, body_md, hash_body)
            VALUES (?, ?, ?, ?, ?)
        """, (
            existing_row['id'],
            now_utc,
            existing_row['title'],
            existing_row['body_md'] if 'body_md' in existing_row.keys() else '',
            existing_row['hash_body']
        ))
        await db.commit()
    
    async def mark_missing_articles_deleted(self, seen_article_ids: List[int]) -> int:
        """
        Mark articles not in seen_article_ids as deleted.
        
        Returns:
            Number of articles marked as deleted
        """
        if not seen_article_ids:
            return 0
        
        async with aiosqlite.connect(self.db_path) as db:
            placeholders = ','.join('?' * len(seen_article_ids))
            cursor = await db.execute(f"""
                UPDATE helpshift_articles 
                SET is_deleted = 1 
                WHERE id NOT IN ({placeholders}) AND is_deleted = 0
            """, seen_article_ids)
            await db.commit()
            return cursor.rowcount
    
    async def get_article(self, article_id: int, include_deleted: bool = False) -> Optional[HelpshiftArticle]:
        """Get a Helpshift article by ID."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            query = "SELECT * FROM helpshift_articles WHERE id = ?"
            params = [article_id]
            
            if not include_deleted:
                query += " AND is_deleted = 0"
            
            cursor = await db.execute(query, params)
            row = await cursor.fetchone()
            
            if row:
                return self._row_to_article(row)
            return None
    
    async def search_articles(
        self,
        query: str,
        limit: int = 20,
        include_deleted: bool = False
    ) -> List[HelpshiftArticle]:
        """Simple text search in articles (basic fallback)."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            # Split query into words for better matching
            words = query.lower().split()
            
            # Build WHERE clause with OR for each word
            where_parts = []
            params = []
            
            for word in words:
                if len(word) > 2:  # Skip very short words
                    where_parts.append("(LOWER(title) LIKE ? OR LOWER(body_md) LIKE ?)")
                    params.extend([f"%{word}%", f"%{word}%"])
            
            if not where_parts:
                # Fallback to simple query
                sql = """
                    SELECT * FROM helpshift_articles 
                    WHERE (LOWER(title) LIKE ? OR LOWER(body_md) LIKE ?)
                """
                params = [f"%{query.lower()}%", f"%{query.lower()}%"]
            else:
                sql = f"""
                    SELECT * FROM helpshift_articles 
                    WHERE ({' OR '.join(where_parts)})
                """
            
            if not include_deleted:
                sql += " AND is_deleted = 0"
            
            sql += " ORDER BY last_seen_utc DESC LIMIT ?"
            params.append(limit)
            
            cursor = await db.execute(sql, params)
            rows = await cursor.fetchall()
            
            log.debug(f"Database search for '{query}' returned {len(rows)} results")
            
            return [self._row_to_article(row) for row in rows]
    
    async def get_all_articles(self, include_deleted: bool = False) -> List[HelpshiftArticle]:
        """Get all Helpshift articles."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            
            query = "SELECT * FROM helpshift_articles"
            if not include_deleted:
                query += " WHERE is_deleted = 0"
            query += " ORDER BY title ASC"
            
            cursor = await db.execute(query)
            rows = await cursor.fetchall()
            
            return [self._row_to_article(row) for row in rows]
    
    async def get_article_titles(self, limit: int = 100) -> List[str]:
        """Get article titles for autocomplete."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT title FROM helpshift_articles 
                WHERE is_deleted = 0 
                ORDER BY last_seen_utc DESC 
                LIMIT ?
            """, (limit,))
            rows = await cursor.fetchall()
            return [row[0] for row in rows]
    
    def _row_to_article(self, row: aiosqlite.Row) -> HelpshiftArticle:
        """Convert database row to HelpshiftArticle."""
        return HelpshiftArticle(
            id=row['id'],
            slug=row['slug'],
            url=row['url'],
            title=row['title'],
            section_id=row['section_id'],
            section_name=row['section_name'],
            last_updated_text=row['last_updated_text'],
            last_seen_utc=row['last_seen_utc'],
            body_md=row['body_md'],
            hash_body=row['hash_body'],
            lang=row['lang'],
            is_deleted=bool(row['is_deleted'])
        )
    
    # ==================== CRAWL STATE ====================
    
    async def set_crawl_state(self, key: str, value: str):
        """Set a crawl state value."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT INTO crawl_state (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """, (key, value))
            await db.commit()
    
    async def get_crawl_state(self, key: str) -> Optional[str]:
        """Get a crawl state value."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("""
                SELECT value FROM crawl_state WHERE key = ?
            """, (key,))
            row = await cursor.fetchone()
            return row[0] if row else None
    
    async def save_crawl_report(self, report: CrawlReport):
        """Save a crawl report to state."""
        await self.set_crawl_state('last_crawl_report', json.dumps(report.to_dict()))
    
    async def get_last_crawl_report(self) -> Optional[CrawlReport]:
        """Get the last crawl report."""
        data = await self.get_crawl_state('last_crawl_report')
        if data:
            report_dict = json.loads(data)
            return CrawlReport(**report_dict)
        return None
    
    # ==================== STATISTICS ====================
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics."""
        async with aiosqlite.connect(self.db_path) as db:
            # Custom FAQs
            cursor = await db.execute("SELECT COUNT(*) FROM faqs WHERE is_deleted = 0")
            custom_total = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM faqs WHERE is_deleted = 1")
            custom_deleted = (await cursor.fetchone())[0]
            
            # Helpshift articles
            cursor = await db.execute("SELECT COUNT(*) FROM helpshift_articles WHERE is_deleted = 0")
            articles_total = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM helpshift_articles WHERE is_deleted = 1")
            articles_deleted = (await cursor.fetchone())[0]
            
            # Sections
            cursor = await db.execute("SELECT COUNT(*) FROM helpshift_sections")
            sections_total = (await cursor.fetchone())[0]
            
            # Version counts
            cursor = await db.execute("SELECT COUNT(*) FROM faq_versions")
            faq_versions = (await cursor.fetchone())[0]
            
            cursor = await db.execute("SELECT COUNT(*) FROM article_versions")
            article_versions = (await cursor.fetchone())[0]
            
            return {
                'custom_faqs': custom_total,
                'custom_deleted': custom_deleted,
                'helpshift_articles': articles_total,
                'helpshift_deleted': articles_deleted,
                'helpshift_sections': sections_total,
                'faq_versions': faq_versions,
                'article_versions': article_versions
            }


# Test functions
async def _test_helpshift_storage():
    """Test Helpshift storage functionality."""
    import tempfile
    import os
    from datetime import datetime
    
    temp_dir = tempfile.mkdtemp()
    db_path = Path(temp_dir) / "test_faq_v2.db"
    
    try:
        db = FAQDatabase(db_path)
        await db.initialize()
        print("✓ Database v2 initialized with Helpshift tables")
        
        # Test section
        section = HelpshiftSection(
            id=1,
            slug="user-interface",
            url="https://xyrality.helpshift.com/hc/en/23-mission-chief/section/1-user-interface/",
            name="User Interface Overview",
            last_seen_utc=datetime.utcnow().isoformat() + 'Z'
        )
        await db.upsert_section(section)
        print("✓ Section added")
        
        # Test article
        article = HelpshiftArticle(
            id=1000,
            slug="mission-list",
            url="https://xyrality.helpshift.com/hc/en/23-mission-chief/faq/1000-mission-list/",
            title="Mission List",
            section_id=1,
            section_name="User Interface Overview",
            last_updated_text="379d",
            last_seen_utc=datetime.utcnow().isoformat() + 'Z',
            body_md="The mission list shows all available missions...",
            hash_body=HelpshiftArticle.compute_hash("The mission list shows all available missions..."),
            lang='en'
        )
        
        status, version_saved = await db.upsert_article(article)
        assert status == 'new'
        print(f"✓ Article added (status: {status})")
        
        # Test update (unchanged)
        status, version_saved = await db.upsert_article(article)
        assert status == 'unchanged'
        print(f"✓ Article re-saved (status: {status})")
        
        # Test update (changed)
        article.body_md = "Updated content about mission list..."
        article.hash_body = HelpshiftArticle.compute_hash(article.body_md)
        status, version_saved = await db.upsert_article(article)
        assert status == 'updated'
        assert version_saved
        print(f"✓ Article updated (status: {status}, version saved: {version_saved})")
        
        # Test retrieval
        retrieved = await db.get_article(1000)
        assert retrieved is not None
        assert "Updated content" in retrieved.body_md
        print("✓ Article retrieved successfully")
        
        # Test statistics
        stats = await db.get_statistics()
        print(f"✓ Statistics: {stats}")
        
        print("\n✅ All Helpshift storage tests passed!")
        
    finally:
        if db_path.exists():
            os.remove(db_path)
        os.rmdir(temp_dir)


if __name__ == "__main__":
    import asyncio
    asyncio.run(_test_helpshift_storage())
