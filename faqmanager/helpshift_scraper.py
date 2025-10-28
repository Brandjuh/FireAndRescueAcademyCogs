"""
Helpshift Web Scraper
Scrapes Mission Chief Help Center (Xyrality Helpshift) for FAQ articles.
"""

import aiohttp
import asyncio
import time
import logging
from typing import List, Optional, Dict, Tuple
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from collections import defaultdict
from .models import HelpshiftArticle

log = logging.getLogger("red.faqmanager.helpshift")


class RateLimiter:
    """
    Token bucket rate limiter for HTTP requests.
    Allows burst of 4 requests, then 2 requests per second.
    """
    
    def __init__(self, rate: float = 2.0, burst: int = 4):
        """
        Initialize rate limiter.
        
        Args:
            rate: Requests per second
            burst: Maximum burst size
        """
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.monotonic()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        """Acquire a token, waiting if necessary."""
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            # Wait if no tokens available
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class HelpshiftScraper:
    """
    Scrapes Xyrality's Mission Chief Help Center.
    Implements rate limiting, caching, and fallback error handling.
    """
    
    BASE_URL = "https://xyrality.helpshift.com"
    HOME_URL = f"{BASE_URL}/hc/en/23-mission-chief/"
    USER_AGENT = "FARA-FAQBot/1.0 (Red-DiscordBot; +https://github.com/Cog-Creators/Red-DiscordBot)"
    
    # Timeouts
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 2
    
    def __init__(self, cache_ttl: int = 600):
        """
        Initialize scraper.
        
        Args:
            cache_ttl: Cache time-to-live in seconds (default 10 minutes)
        """
        self.rate_limiter = RateLimiter(rate=2.0, burst=4)
        self.cache_ttl = cache_ttl
        
        # In-memory caches
        self._section_cache: Dict[str, Tuple[List[Tuple[str, str]], float]] = {}
        self._article_cache: Dict[str, Tuple[HelpshiftArticle, float]] = {}
        self._title_cache: List[Tuple[str, float]] = []  # For autocomplete
        
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def _ensure_session(self):
        """Ensure aiohttp session exists."""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": self.USER_AGENT}
            )
    
    async def close(self):
        """Close the aiohttp session."""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _fetch(self, url: str, retries: int = MAX_RETRIES) -> Optional[str]:
        """
        Fetch HTML content from URL with rate limiting and retries.
        
        Args:
            url: URL to fetch
            retries: Number of retries on failure
            
        Returns:
            HTML content or None on failure
        """
        await self._ensure_session()
        await self.rate_limiter.acquire()
        
        for attempt in range(retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        log.warning(f"HTTP {response.status} for {url}")
                        if response.status >= 500 and attempt < retries:
                            # Server error - retry with backoff
                            wait = (2 ** attempt) + (asyncio.get_event_loop().time() % 1)
                            await asyncio.sleep(wait)
                            continue
                        return None
            
            except asyncio.TimeoutError:
                log.warning(f"Timeout fetching {url} (attempt {attempt + 1}/{retries + 1})")
                if attempt < retries:
                    await asyncio.sleep(1 + (asyncio.get_event_loop().time() % 0.5))
                    continue
                return None
            
            except aiohttp.ClientError as e:
                log.error(f"Client error fetching {url}: {e}")
                return None
        
        return None
    
    def _is_cache_valid(self, timestamp: float) -> bool:
        """Check if cached data is still valid."""
        return (time.time() - timestamp) < self.cache_ttl
    
    async def get_sections(self) -> List[Tuple[str, str]]:
        """
        Get all sections from the help center home page.
        
        Returns:
            List of (section_name, section_url) tuples
        """
        # Check cache
        if self.HOME_URL in self._section_cache:
            sections, timestamp = self._section_cache[self.HOME_URL]
            if self._is_cache_valid(timestamp):
                log.debug("Returning cached sections")
                return sections
        
        html = await self._fetch(self.HOME_URL)
        if not html:
            log.error("Failed to fetch help center home page")
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        sections = []
        
        # Try multiple selector strategies
        selectors = [
            ('a', {'href': lambda x: x and '/section/' in x}),
            ('div.section-card a', {}),
            ('a.section-link', {})
        ]
        
        for tag, attrs in selectors:
            links = soup.find_all(tag, attrs)
            if links:
                for link in links:
                    href = link.get('href', '')
                    if '/section/' in href:
                        title = link.get_text(strip=True)
                        full_url = href if href.startswith('http') else self.BASE_URL + href
                        sections.append((title, full_url))
                
                if sections:
                    break
        
        # Cache results
        if sections:
            self._section_cache[self.HOME_URL] = (sections, time.time())
            log.info(f"Found {len(sections)} sections")
        else:
            log.warning("No sections found with any selector")
        
        return sections
    
    async def get_section_articles(self, section_url: str) -> List[Tuple[str, str]]:
        """
        Get all articles from a section page.
        
        Args:
            section_url: URL of the section
            
        Returns:
            List of (article_title, article_url) tuples
        """
        # Check cache
        if section_url in self._section_cache:
            articles, timestamp = self._section_cache[section_url]
            if self._is_cache_valid(timestamp):
                return articles
        
        html = await self._fetch(section_url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        articles = []
        
        # Try multiple selectors
        selectors = [
            ('a', {'href': lambda x: x and '/faq/' in x}),
            ('div.article-list a', {}),
            ('a.article-link', {})
        ]
        
        for tag, attrs in selectors:
            links = soup.find_all(tag, attrs)
            if links:
                for link in links:
                    href = link.get('href', '')
                    if '/faq/' in href:
                        title = link.get_text(strip=True)
                        full_url = href if href.startswith('http') else self.BASE_URL + href
                        articles.append((title, full_url))
                
                if articles:
                    break
        
        # Cache results
        if articles:
            self._section_cache[section_url] = (articles, time.time())
        
        return articles
    
    async def get_article(self, article_url: str, section_name: str = "") -> Optional[HelpshiftArticle]:
        """
        Get full article content from an article URL.
        
        Args:
            article_url: URL of the article
            section_name: Name of the section (for metadata)
            
        Returns:
            HelpshiftArticle or None on failure
        """
        # Check cache
        if article_url in self._article_cache:
            article, timestamp = self._article_cache[article_url]
            if self._is_cache_valid(timestamp):
                return article
        
        html = await self._fetch(article_url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract title
        title = None
        for selector in ['h1', 'h2', '.article-title', '.faq-title']:
            title_elem = soup.select_one(selector)
            if title_elem:
                title = title_elem.get_text(strip=True)
                break
        
        if not title:
            log.warning(f"Could not find title for {article_url}")
            title = "Untitled Article"
        
        # Extract last updated
        last_updated = None
        for text in soup.stripped_strings:
            if 'Last Updated:' in text or 'Updated:' in text:
                # Extract something like "379d" or "3 months ago"
                parts = text.split(':')
                if len(parts) > 1:
                    last_updated = parts[1].strip()
                break
        
        # Extract body content
        body = ""
        body_selectors = [
            '.article-body',
            '.faq-body',
            '.content',
            'article',
            'main'
        ]
        
        for selector in body_selectors:
            body_elem = soup.select_one(selector)
            if body_elem:
                # Convert to plain text with some structure
                body = self._html_to_markdown(body_elem)
                break
        
        if not body:
            # Fallback: get all paragraphs
            paragraphs = soup.find_all('p')
            body = '\n\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        # Create snippet (first 300 chars)
        snippet = body[:300] + "..." if len(body) > 300 else body
        
        article = HelpshiftArticle(
            title=title,
            url=article_url,
            section=section_name,
            body=body,
            last_updated=last_updated,
            snippet=snippet
        )
        
        # Cache result
        self._article_cache[article_url] = (article, time.time())
        
        # Add to title cache for autocomplete
        self._add_to_title_cache(title)
        
        return article
    
    def _html_to_markdown(self, element) -> str:
        """
        Convert HTML element to simplified markdown-like text.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            Formatted text
        """
        text_parts = []
        
        for child in element.descendants:
            if child.name == 'h1' or child.name == 'h2':
                text_parts.append(f"\n**{child.get_text(strip=True)}**\n")
            elif child.name == 'h3' or child.name == 'h4':
                text_parts.append(f"\n*{child.get_text(strip=True)}*\n")
            elif child.name == 'li':
                text_parts.append(f"• {child.get_text(strip=True)}\n")
            elif child.name == 'p':
                text_parts.append(f"{child.get_text(strip=True)}\n\n")
            elif isinstance(child, str) and child.strip():
                if not any(child in str(p) for p in text_parts[-3:]):
                    text_parts.append(child.strip() + " ")
        
        return ''.join(text_parts).strip()
    
    def _add_to_title_cache(self, title: str):
        """Add title to autocomplete cache."""
        current_time = time.time()
        # Remove expired entries
        self._title_cache = [(t, ts) for t, ts in self._title_cache if self._is_cache_valid(ts)]
        # Add new title if not already present
        if title not in [t for t, _ in self._title_cache]:
            self._title_cache.append((title, current_time))
    
    def get_cached_titles(self) -> List[str]:
        """
        Get all cached article titles for autocomplete.
        
        Returns:
            List of article titles
        """
        current_time = time.time()
        valid_titles = [
            title for title, timestamp in self._title_cache
            if self._is_cache_valid(timestamp)
        ]
        return valid_titles
    
    async def search_all_articles(self, query: str, max_articles: int = 20) -> List[HelpshiftArticle]:
        """
        Search across all sections for articles matching query.
        This is the main search entry point.
        
        Args:
            query: Search query
            max_articles: Maximum articles to fetch
            
        Returns:
            List of HelpshiftArticle objects
        """
        sections = await self.get_sections()
        if not sections:
            log.error("No sections found")
            return []
        
        articles = []
        
        # Fetch articles from all sections (limited)
        for section_name, section_url in sections[:5]:  # Limit to first 5 sections
            section_articles = await self.get_section_articles(section_url)
            
            for article_title, article_url in section_articles:
                if len(articles) >= max_articles:
                    break
                
                # Fetch full article (will be cached)
                article = await self.get_article(article_url, section_name)
                if article:
                    articles.append(article)
            
            if len(articles) >= max_articles:
                break
        
        log.info(f"Fetched {len(articles)} articles from Helpshift")
        return articles
    
    def clear_cache(self):
        """Clear all caches."""
        self._section_cache.clear()
        self._article_cache.clear()
        self._title_cache.clear()
        log.info("Cleared all caches")


# Mock HTML responses for testing
MOCK_HOME_HTML = """
<html>
<body>
    <div class="sections">
        <a href="/hc/en/23-mission-chief/section/1-user-interface/">User Interface Overview</a>
        <a href="/hc/en/23-mission-chief/section/2-getting-started/">Getting Started</a>
        <a href="/hc/en/23-mission-chief/section/3-game-mechanics/">Game Mechanics</a>
    </div>
</body>
</html>
"""

MOCK_SECTION_HTML = """
<html>
<body>
    <div class="article-list">
        <a href="/hc/en/23-mission-chief/faq/1000-mission-list/">Mission List</a>
        <a href="/hc/en/23-mission-chief/faq/1001-dispatch-center/">Dispatch Center</a>
    </div>
</body>
</html>
"""

MOCK_ARTICLE_HTML = """
<html>
<body>
    <h1>Mission List</h1>
    <div class="meta">Last Updated: 379d</div>
    <article class="article-body">
        <p>The mission list shows all available missions on your map.</p>
        <h3>Features:</h3>
        <ul>
            <li>Filter by mission type</li>
            <li>Sort by distance</li>
            <li>Search function</li>
        </ul>
        <p>You can customize the mission list view in the settings.</p>
    </article>
</body>
</html>
"""


async def _test_scraper():
    """Test scraper with mock responses."""
    print("=== Helpshift Scraper Tests ===\n")
    
    scraper = HelpshiftScraper(cache_ttl=300)
    
    # Test 1: Rate limiter
    print("Test 1: Rate limiter")
    start = time.time()
    for i in range(5):
        await scraper.rate_limiter.acquire()
    elapsed = time.time() - start
    assert elapsed >= 0.5  # Should take at least 0.5s for 5 requests (burst 4 + 1 wait)
    print(f"✓ Rate limiter working (5 requests in {elapsed:.2f}s)")
    
    # Test 2: HTML parsing (mock)
    print("\nTest 2: HTML parsing")
    soup = BeautifulSoup(MOCK_ARTICLE_HTML, 'lxml')
    title = soup.find('h1').get_text(strip=True)
    assert title == "Mission List"
    print(f"✓ Parsed title: {title}")
    
    # Test 3: Markdown conversion
    print("\nTest 3: HTML to markdown conversion")
    soup = BeautifulSoup(MOCK_ARTICLE_HTML, 'lxml')
    body_elem = soup.find('article')
    body_text = scraper._html_to_markdown(body_elem)
    assert "Mission List" in body_text or "mission list" in body_text
    assert "Filter" in body_text or "filter" in body_text
    print(f"✓ Converted HTML to text ({len(body_text)} chars)")
    
    # Test 4: Cache functionality
    print("\nTest 4: Cache functionality")
    test_url = "https://test.example.com/article"
    test_article = HelpshiftArticle(
        title="Test Article",
        url=test_url,
        section="Test",
        body="Test content",
        last_updated="1d"
    )
    scraper._article_cache[test_url] = (test_article, time.time())
    
    # Should return cached version
    assert scraper._is_cache_valid(time.time())
    print("✓ Cache validation working")
    
    # Test 5: Title cache for autocomplete
    print("\nTest 5: Title cache for autocomplete")
    scraper._add_to_title_cache("Mission List")
    scraper._add_to_title_cache("Dispatch Center")
    titles = scraper.get_cached_titles()
    assert len(titles) == 2
    print(f"✓ Title cache: {titles}")
    
    await scraper.close()
    print("\n✅ All scraper tests passed!")


if __name__ == "__main__":
    asyncio.run(_test_scraper())
