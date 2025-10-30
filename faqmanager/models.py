"""
FAQManager Data Models
Defines all data structures used throughout the FAQ system.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime
import hashlib


class Source(Enum):
    """Source of FAQ content."""
    CUSTOM = "custom"
    HELPSHIFT_LOCAL = "helpshift_local"
    HELPSHIFT_LIVE = "helpshift_live"


@dataclass
class FAQItem:
    """
    Represents a custom FAQ entry stored in the database.
    
    Attributes:
        id: Database primary key (None for new items)
        question: The FAQ question/title
        answer_md: Answer in Markdown format
        category: Optional category/section
        synonyms: List of synonym terms for better search
        created_at: Unix timestamp of creation
        updated_at: Unix timestamp of last update
        author_id: Discord user ID who created this
        is_deleted: Soft delete flag
    """
    question: str
    answer_md: str
    category: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)
    id: Optional[int] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None
    author_id: Optional[int] = None
    is_deleted: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'id': self.id,
            'question': self.question,
            'answer_md': self.answer_md,
            'category': self.category,
            'synonyms': self.synonyms,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'author_id': self.author_id,
            'is_deleted': self.is_deleted
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FAQItem':
        """Create FAQItem from dictionary."""
        return cls(
            id=data.get('id'),
            question=data['question'],
            answer_md=data['answer_md'],
            category=data.get('category'),
            synonyms=data.get('synonyms', []),
            created_at=data.get('created_at'),
            updated_at=data.get('updated_at'),
            author_id=data.get('author_id'),
            is_deleted=data.get('is_deleted', False)
        )

    def get_excerpt(self, max_length: int = 500) -> str:
        """Get a safe excerpt of the answer for display."""
        if len(self.answer_md) <= max_length:
            return self.answer_md
        return self.answer_md[:max_length].rsplit(' ', 1)[0] + '...'


@dataclass
class HelpshiftSection:
    """
    Represents a section from Mission Chief Help Center.
    
    Attributes:
        id: Numeric section ID from URL
        slug: URL slug
        url: Full section URL
        name: Section display name
        last_seen_utc: ISO timestamp when last crawled
    """
    id: int
    slug: str
    url: str
    name: str
    last_seen_utc: str

    @staticmethod
    def parse_id_from_url(url: str) -> Optional[int]:
        """Extract numeric ID from section URL."""
        try:
            # URL format: .../section/123-slug-name/
            parts = url.split('/section/')
            if len(parts) > 1:
                id_part = parts[1].split('-')[0]
                return int(id_part)
        except (ValueError, IndexError):
            pass
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'slug': self.slug,
            'url': self.url,
            'name': self.name,
            'last_seen_utc': self.last_seen_utc
        }


@dataclass
class HelpshiftArticle:
    """
    Represents a locally stored article from Mission Chief Help Center.
    
    Attributes:
        id: Numeric article ID from URL
        slug: URL slug
        url: Full article URL
        title: Article title
        section_id: Parent section ID
        section_name: Parent section name
        last_updated_text: Last updated text from article (e.g., "379d")
        last_seen_utc: ISO timestamp when last crawled
        body_md: Article body in markdown/plaintext
        hash_body: SHA256 hash of normalized body for change detection
        lang: Language code
        is_deleted: Whether article was removed from site
    """
    id: int
    slug: str
    url: str
    title: str
    body_md: str
    hash_body: str
    last_seen_utc: str
    section_id: Optional[int] = None
    section_name: Optional[str] = None
    last_updated_text: Optional[str] = None
    lang: str = 'en'
    is_deleted: bool = False

    @staticmethod
    def parse_id_from_url(url: str) -> Optional[int]:
        """Extract numeric ID from article URL."""
        try:
            # URL format: .../faq/1000-slug-name/
            parts = url.split('/faq/')
            if len(parts) > 1:
                id_part = parts[1].split('-')[0]
                return int(id_part)
        except (ValueError, IndexError):
            pass
        return None

    @staticmethod
    def compute_hash(body: str) -> str:
        """Compute SHA256 hash of normalized body."""
        normalized = ' '.join(body.split())  # Normalize whitespace
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

    def get_excerpt(self, max_length: int = 500) -> str:
        """Get a safe excerpt of the body."""
        if len(self.body_md) <= max_length:
            return self.body_md
        return self.body_md[:max_length].rsplit(' ', 1)[0] + '...'

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'slug': self.slug,
            'url': self.url,
            'title': self.title,
            'section_id': self.section_id,
            'section_name': self.section_name,
            'last_updated_text': self.last_updated_text,
            'last_seen_utc': self.last_seen_utc,
            'body_md': self.body_md,
            'hash_body': self.hash_body,
            'lang': self.lang,
            'is_deleted': self.is_deleted
        }


@dataclass
class SearchResult:
    """
    Unified search result that can represent Custom, Local Helpshift, or Live Helpshift content.
    
    Attributes:
        source: Source type
        title: Display title
        content: Main content/answer
        category: Category/section
        score: Fuzzy search score (0-100)
        url: Optional URL (for Helpshift)
        last_updated: Last update info
        faq_id: Database ID (for custom FAQs)
        article_id: Database ID (for local Helpshift)
    """
    source: Source
    title: str
    content: str
    score: float
    category: Optional[str] = None
    url: Optional[str] = None
    last_updated: Optional[str] = None
    faq_id: Optional[int] = None
    article_id: Optional[int] = None

    def get_excerpt(self, max_length: int = 500) -> str:
        """Get a safe excerpt of the content."""
        if len(self.content) <= max_length:
            return self.content
        return self.content[:max_length].rsplit(' ', 1)[0] + '...'

    @classmethod
    def from_faq_item(cls, item: FAQItem, score: float) -> 'SearchResult':
        """Create SearchResult from custom FAQItem."""
        return cls(
            source=Source.CUSTOM,
            title=item.question,
            content=item.answer_md,
            category=item.category,
            score=score,
            faq_id=item.id
        )

    @classmethod
    def from_helpshift_article(cls, article: HelpshiftArticle, score: float, live: bool = False) -> 'SearchResult':
        """Create SearchResult from Helpshift article."""
        return cls(
            source=Source.HELPSHIFT_LIVE if live else Source.HELPSHIFT_LOCAL,
            title=article.title,
            content=article.body_md,
            category=article.section_name,
            score=score,
            url=article.url,
            last_updated=article.last_updated_text,
            article_id=article.id
        )


@dataclass
class FAQVersion:
    """
    Represents a version snapshot of a FAQ entry.
    
    Attributes:
        id: Version ID
        faq_id: Reference to the FAQ
        snapshot: Dictionary snapshot of FAQ state
        saved_at: Unix timestamp
        editor_id: Discord user ID who made this edit
    """
    faq_id: int
    snapshot: Dict[str, Any]
    saved_at: int
    editor_id: int
    id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'faq_id': self.faq_id,
            'snapshot': self.snapshot,
            'saved_at': self.saved_at,
            'editor_id': self.editor_id
        }


@dataclass
class ArticleVersion:
    """
    Represents a version snapshot of a Helpshift article.
    
    Attributes:
        id: Version ID
        article_id: Reference to the article
        saved_utc: ISO timestamp
        title: Title at this version
        body_md: Body at this version
        hash_body: Hash at this version
    """
    article_id: int
    saved_utc: str
    title: str
    body_md: str
    hash_body: str
    id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'id': self.id,
            'article_id': self.article_id,
            'saved_utc': self.saved_utc,
            'title': self.title,
            'body_md': self.body_md,
            'hash_body': self.hash_body
        }


@dataclass
class CrawlReport:
    """
    Report of a crawl operation.
    
    Attributes:
        started_at: ISO timestamp when crawl started
        completed_at: ISO timestamp when crawl completed
        duration_seconds: Duration in seconds
        sections_found: Number of sections discovered
        articles_total: Total articles processed
        articles_new: New articles added
        articles_updated: Articles with changes
        articles_unchanged: Articles with no changes
        articles_deleted: Articles marked as deleted
        errors: List of error messages
    """
    started_at: str
    completed_at: str
    duration_seconds: float
    sections_found: int = 0
    articles_total: int = 0
    articles_new: int = 0
    articles_updated: int = 0
    articles_unchanged: int = 0
    articles_deleted: int = 0
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'started_at': self.started_at,
            'completed_at': self.completed_at,
            'duration_seconds': self.duration_seconds,
            'sections_found': self.sections_found,
            'articles_total': self.articles_total,
            'articles_new': self.articles_new,
            'articles_updated': self.articles_updated,
            'articles_unchanged': self.articles_unchanged,
            'articles_deleted': self.articles_deleted,
            'errors': self.errors
        }


@dataclass
class OutdatedReport:
    """
    Represents an outdated content report from a user.
    
    Attributes:
        source: CUSTOM or HELPSHIFT
        title: Title of the reported content
        url: URL (if Helpshift)
        reporter_id: Discord user ID
        channel_id: Channel where report was made
        query: Original search query
        timestamp: Unix timestamp of report
    """
    source: Source
    title: str
    reporter_id: int
    channel_id: int
    query: str
    timestamp: int
    url: Optional[str] = None
    faq_id: Optional[int] = None


# Test data for development/testing
MOCK_FAQ_ITEMS = [
    FAQItem(
        id=1,
        question="What is ARR?",
        answer_md="**Alarm and Response Regulation (ARR)** allows you to set up automated responses to missions. You can configure which vehicles respond to specific call types.",
        category="Game Mechanics",
        synonyms=["alarm and response regulation", "arr rules", "automatic dispatch"],
        created_at=1700000000,
        updated_at=1700000000,
        author_id=123456789
    ),
    FAQItem(
        id=2,
        question="How do I earn more credits?",
        answer_md="Credits can be earned by:\n- Completing missions\n- Daily login bonuses\n- Alliance missions (shared calls)\n- Special events (2x credit events)",
        category="Economy",
        synonyms=["money", "cash", "income", "earnings"],
        created_at=1700000100,
        updated_at=1700000100,
        author_id=123456789
    ),
    FAQItem(
        id=3,
        question="What are POIs?",
        answer_md="**Points of Interest (POIs)** are location markers on your map where missions can spawn. The more POIs in an area, the more missions you'll receive nearby.",
        category="Map & Locations",
        synonyms=["points of interest", "spawn points", "mission markers"],
        created_at=1700000200,
        updated_at=1700000200,
        author_id=123456789
    )
]
