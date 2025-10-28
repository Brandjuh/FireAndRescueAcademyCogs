"""
FAQManager Data Models
Defines all data structures used throughout the FAQ system.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from datetime import datetime


class Source(Enum):
    """Source of FAQ content."""
    CUSTOM = "custom"
    HELPSHIFT = "helpshift"


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
class HelpshiftArticle:
    """
    Represents a scraped article from Mission Chief Help Center.
    
    Attributes:
        title: Article title
        url: Full URL to the article
        section: Section/category name
        body: Article body (plain text or safe markdown)
        last_updated: Last update info (e.g., "379d")
        snippet: Short preview for search results
    """
    title: str
    url: str
    section: str
    body: str
    last_updated: Optional[str] = None
    snippet: Optional[str] = None

    def get_excerpt(self, max_length: int = 500) -> str:
        """Get a safe excerpt of the body for display."""
        text = self.snippet or self.body
        if len(text) <= max_length:
            return text
        return text[:max_length].rsplit(' ', 1)[0] + '...'


@dataclass
class SearchResult:
    """
    Unified search result that can represent either Custom or Helpshift content.
    
    Attributes:
        source: Source type (CUSTOM or HELPSHIFT)
        title: Display title
        content: Main content/answer
        category: Category/section
        score: Fuzzy search score (0-100)
        url: Optional URL (for Helpshift)
        last_updated: Last update info
        faq_id: Database ID (for custom FAQs)
    """
    source: Source
    title: str
    content: str
    score: float
    category: Optional[str] = None
    url: Optional[str] = None
    last_updated: Optional[str] = None
    faq_id: Optional[int] = None

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
    def from_helpshift_article(cls, article: HelpshiftArticle, score: float) -> 'SearchResult':
        """Create SearchResult from Helpshift article."""
        return cls(
            source=Source.HELPSHIFT,
            title=article.title,
            content=article.body,
            category=article.section,
            score=score,
            url=article.url,
            last_updated=article.last_updated
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

MOCK_HELPSHIFT_ARTICLES = [
    HelpshiftArticle(
        title="Mission List",
        url="https://xyrality.helpshift.com/hc/en/23-mission-chief/faq/1000-mission-list/",
        section="User Interface Overview",
        body="The mission list shows all available missions on the map. You can filter by type, sort by distance, and use the search function to find specific missions.",
        last_updated="379d",
        snippet="View and manage all your active missions..."
    ),
    HelpshiftArticle(
        title="Building Your Station",
        url="https://xyrality.helpshift.com/hc/en/23-mission-chief/faq/1001-building-station/",
        section="Getting Started",
        body="To build a new station, click on the Build menu and select the type of station you want. Fire stations cost 100,000 credits and can house 6 vehicles.",
        last_updated="120d",
        snippet="Learn how to expand your emergency services network..."
    )
]
