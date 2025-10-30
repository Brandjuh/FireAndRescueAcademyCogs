"""
Helpshift Web Crawler (v2)
Periodically scrapes and stores Mission Chief Help Center locally.
"""

import aiohttp
import asyncio
import time
import logging
from typing import List, Optional, Dict
from bs4 import BeautifulSoup
from .models import HelpshiftArticle
from .database import FAQDatabase

log = logging.getLogger("red.faqmanager.crawler")


class RateLimiter:
    """Token bucket rate limiter for HTTP requests."""
    
    def __init__(self, rate: float = 2.0, burst: int = 4):
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
            
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class HelpshiftCrawler:
    """Crawls Mission Chief Help Center and stores articles locally."""
    
    BASE_URL = "https://xyrality.helpshift.com"
    HOME_URL = f"{BASE_URL}/hc/en/23-mission-chief/"
    USER_AGENT = "FARA-FAQBot/1.0 (Red-DiscordBot)"
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 2
    
    def __init__(self, database: FAQDatabase, max_concurrency: int = 4):
        self.database = database
        self.rate_limiter = RateLimiter(rate=2.0, burst=4)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()


class HelpshiftScraper:
    """Backwards compatibility wrapper for old HelpshiftScraper interface."""
    
    def __init__(self, cache_ttl: int = 600):
        self.cache_ttl = cache_ttl
        self.database: Optional[FAQDatabase] = None
    
    def set_database(self, database: FAQDatabase):
        """Set database reference (called by cog)."""
        self.database = database
    
    async def close(self):
        """Compatibility method - no-op."""
        pass
    
    async def search_all_articles(self, query: str, max_articles: int = 20) -> List[HelpshiftArticle]:
        """
        Search local database for articles (compatibility method).
        
        Args:
            query: Search query
            max_articles: Maximum articles to return
            
        Returns:
            List of HelpshiftArticle objects from local database
        """
        if not self.database:
            return []
        
        try:
            articles = await self.database.search_articles(query, limit=max_articles)
            return articles
        except Exception as e:
            log.error(f"Error searching local articles: {e}")
            return []
    
    def get_cached_titles(self) -> List[str]:
        """Get cached article titles for autocomplete."""
        return []
    
    def clear_cache(self):
        """Compatibility method - no-op."""
        pass
