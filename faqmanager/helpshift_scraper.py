"""
Helpshift Web Crawler (v2)
Periodically scrapes and stores Mission Chief Help Center locally.
"""

import aiohttp
import asyncio
import time
import logging
from typing import List, Optional, Dict, Set
from bs4 import BeautifulSoup
from datetime import datetime
from .models import HelpshiftArticle, HelpshiftSection, CrawlReport
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
    MAX_BODY_LENGTH = 80000
    
    def __init__(self, database: FAQDatabase, max_concurrency: int = 4):
        self.database = database
        self.rate_limiter = RateLimiter(rate=2.0, burst=4)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session: Optional[aiohttp.ClientSession] = None
        
        self.stats = {
            'sections_found': 0,
            'articles_new': 0,
            'articles_updated': 0,
            'articles_unchanged': 0,
            'articles_total': 0,
            'errors': []
        }
    
    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": self.USER_AGENT}
            )
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _fetch(self, url: str, retries: int = MAX_RETRIES) -> Optional[str]:
        await self._ensure_session()
        await self.rate_limiter.acquire()
        
        for attempt in range(retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status >= 500 and attempt < retries:
                        wait = (2 ** attempt) + (asyncio.get_event_loop().time() % 1)
                        await asyncio.sleep(wait)
                        continue
                    else:
                        log.warning(f"HTTP {response.status} for {url}")
                        return None
            except asyncio.TimeoutError:
                if attempt < retries:
                    await asyncio.sleep(1)
                    continue
                return None
            except aiohttp.ClientError as e:
                log.error(f"Client error: {e}")
                return None
        
        return None
    
    def _extract_body(self, soup: BeautifulSoup) -> str:
        body_selectors = ['.article-body', '.faq-body', '.content', 'article', 'main']
        
        body_elem = None
        for selector in body_selectors:
            body_elem = soup.select_one(selector)
            if body_elem:
                break
        
        if not body_elem:
            paragraphs = soup.find_all('p')
            return '\n\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        parts = []
        for child in body_elem.descendants:
            if child.name in ['h1', 'h2']:
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"\n## {text}\n")
            elif child.name in ['h3', 'h4']:
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"\n### {text}\n")
            elif child.name == 'li':
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"• {text}\n")
            elif child.name == 'p':
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"{text}\n\n")
        
        result = ''.join(parts)
        lines = [line.strip() for line in result.split('\n') if line.strip()]
        return '\n\n'.join(lines)


# Backwards compatibility wrapper
class HelpshiftScraper:
    """Compatibility wrapper for old interface."""
    
    def __init__(self, cache_ttl: int = 600):
        self.cache_ttl = cache_ttl
        self.database: Optional[FAQDatabase] = None
    
    def set_database(self, database: FAQDatabase):
        self.database = database
    
    async def close(self):
        pass
    
    async def search_all_articles(self, query: str, max_articles: int = 20) -> List[HelpshiftArticle]:
        if not self.database:
            return []
        
        try:
            articles = await self.database.search_articles(query, limit=max_articles)
            return articles
        except Exception as e:
            log.error(f"Error searching local articles: {e}")
            return []
    
    def get_cached_titles(self) -> List[str]:
        return []
    
    def clear_cache(self):
        pass"""
Helpshift Web Crawler (v2)
Periodically scrapes and stores Mission Chief Help Center locally.
"""

import aiohttp
import asyncio
import time
import logging
from typing import List, Optional, Dict, Set
from bs4 import BeautifulSoup
from datetime import datetime
from .models import HelpshiftArticle, HelpshiftSection, CrawlReport
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
    MAX_BODY_LENGTH = 80000
    
    def __init__(self, database: FAQDatabase, max_concurrency: int = 4):
        self.database = database
        self.rate_limiter = RateLimiter(rate=2.0, burst=4)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session: Optional[aiohttp.ClientSession] = None
        
        self.stats = {
            'sections_found': 0,
            'articles_new': 0,
            'articles_updated': 0,
            'articles_unchanged': 0,
            'articles_total': 0,
            'errors': []
        }
    
    async def _ensure_session(self):
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT)
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": self.USER_AGENT}
            )
    
    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def _fetch(self, url: str, retries: int = MAX_RETRIES) -> Optional[str]:
        await self._ensure_session()
        await self.rate_limiter.acquire()
        
        for attempt in range(retries + 1):
            try:
                async with self.session.get(url) as response:
                    if response.status == 200:
                        return await response.text()
                    elif response.status >= 500 and attempt < retries:
                        wait = (2 ** attempt) + (asyncio.get_event_loop().time() % 1)
                        await asyncio.sleep(wait)
                        continue
                    else:
                        log.warning(f"HTTP {response.status} for {url}")
                        return None
            except asyncio.TimeoutError:
                if attempt < retries:
                    await asyncio.sleep(1)
                    continue
                return None
            except aiohttp.ClientError as e:
                log.error(f"Client error: {e}")
                return None
        
        return None
    
    def _extract_body(self, soup: BeautifulSoup) -> str:
        body_selectors = ['.article-body', '.faq-body', '.content', 'article', 'main']
        
        body_elem = None
        for selector in body_selectors:
            body_elem = soup.select_one(selector)
            if body_elem:
                break
        
        if not body_elem:
            paragraphs = soup.find_all('p')
            return '\n\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        parts = []
        for child in body_elem.descendants:
            if child.name in ['h1', 'h2']:
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"\n## {text}\n")
            elif child.name in ['h3', 'h4']:
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"\n### {text}\n")
            elif child.name == 'li':
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"• {text}\n")
            elif child.name == 'p':
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"{text}\n\n")
        
        result = ''.join(parts)
        lines = [line.strip() for line in result.split('\n') if line.strip()]
        return '\n\n'.join(lines)


# Backwards compatibility wrapper
class HelpshiftScraper:
    """Compatibility wrapper for old interface."""
    
    def __init__(self, cache_ttl: int = 600):
        self.cache_ttl = cache_ttl
        self.database: Optional[FAQDatabase] = None
    
    def set_database(self, database: FAQDatabase):
        self.database = database
    
    async def close(self):
        pass
    
    async def search_all_articles(self, query: str, max_articles: int = 20) -> List[HelpshiftArticle]:
        if not self.database:
            return []
        
        try:
            articles = await self.database.search_articles(query, limit=max_articles)
            return articles
        except Exception as e:
            log.error(f"Error searching local articles: {e}")
            return []
    
    def get_cached_titles(self) -> List[str]:
        return []
    
    def clear_cache(self):
        pass
