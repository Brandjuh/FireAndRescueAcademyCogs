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
            
            # Refill tokens
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            # Wait if no tokens
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class HelpshiftCrawler:
    """
    Crawls Mission Chief Help Center and stores articles locally.
    Supports incremental updates with change detection.
    """
    
    BASE_URL = "https://xyrality.helpshift.com"
    HOME_URL = f"{BASE_URL}/hc/en/23-mission-chief/"
    USER_AGENT = "FARA-FAQBot/1.0 (Red-DiscordBot; +https://github.com/Cog-Creators/Red-DiscordBot)"
    
    REQUEST_TIMEOUT = 10
    MAX_RETRIES = 2
    MAX_BODY_LENGTH = 80000  # Truncate very long articles
    
    def __init__(self, database: FAQDatabase, max_concurrency: int = 4):
        """
        Initialize crawler.
        
        Args:
            database: FAQDatabase instance for storage
            max_concurrency: Max concurrent article fetches
        """
        self.database = database
        self.rate_limiter = RateLimiter(rate=2.0, burst=4)
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Crawl statistics
        self.stats = {
            'sections_found': 0,
            'articles_new': 0,
            'articles_updated': 0,
            'articles_unchanged': 0,
            'articles_total': 0,
            'errors': []
        }
    
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
        Fetch HTML content with rate limiting and retries.
        
        Args:
            url: URL to fetch
            retries: Number of retry attempts
            
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
                    elif response.status == 429:
                        # Rate limited - wait and retry
                        retry_after = int(response.headers.get('Retry-After', 5))
                        await asyncio.sleep(retry_after + (asyncio.get_event_loop().time() % 1))
                        continue
                    elif response.status >= 500 and attempt < retries:
                        # Server error - retry with backoff
                        wait = (2 ** attempt) + (asyncio.get_event_loop().time() % 1)
                        await asyncio.sleep(wait)
                        continue
                    else:
                        log.warning(f"HTTP {response.status} for {url}")
                        return None
            
            except asyncio.TimeoutError:
                if attempt < retries:
                    await asyncio.sleep(1 + (asyncio.get_event_loop().time() % 0.5))
                    continue
                log.warning(f"Timeout fetching {url}")
                return None
            
            except aiohttp.ClientError as e:
                log.error(f"Client error fetching {url}: {e}")
                return None
        
        return None
    
    async def crawl_full(self) -> CrawlReport:
        """
        Perform a full crawl of all sections and articles.
        
        Returns:
            CrawlReport with statistics
        """
        start_time = datetime.utcnow()
        start_iso = start_time.isoformat() + 'Z'
        
        # Reset stats
        self.stats = {
            'sections_found': 0,
            'articles_new': 0,
            'articles_updated': 0,
            'articles_unchanged': 0,
            'articles_total': 0,
            'errors': []
        }
        
        try:
            log.info("Starting full crawl of Helpshift...")
            
            # Fetch sections from home page
            sections = await self._crawl_sections()
            self.stats['sections_found'] = len(sections)
            
            if not sections:
                log.error("No sections found on home page")
                self.stats['errors'].append("No sections found on home page")
            
            # Track seen article IDs
            seen_article_ids: Set[int] = set()
            
            # Crawl each section
            for section in sections:
                try:
                    article_ids = await self._crawl_section(section)
                    seen_article_ids.update(article_ids)
                except Exception as e:
                    error_msg = f"Error crawling section {section.name}: {e}"
                    log.error(error_msg, exc_info=True)
                    self.stats['errors'].append(error_msg)
            
            # Mark missing articles as deleted
            if seen_article_ids:
                deleted_count = await self.database.mark_missing_articles_deleted(list(seen_article_ids))
                log.info(f"Marked {deleted_count} articles as deleted")
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            report = CrawlReport(
                started_at=start_iso,
                completed_at=end_time.isoformat() + 'Z',
                duration_seconds=duration,
                sections_found=self.stats['sections_found'],
                articles_total=self.stats['articles_total'],
                articles_new=self.stats['articles_new'],
                articles_updated=self.stats['articles_updated'],
                articles_unchanged=self.stats['articles_unchanged'],
                articles_deleted=deleted_count if seen_article_ids else 0,
                errors=self.stats['errors']
            )
            
            # Save report
            await self.database.save_crawl_report(report)
            
            log.info(
                f"Crawl completed: {self.stats['articles_total']} articles "
                f"({self.stats['articles_new']} new, {self.stats['articles_updated']} updated, "
                f"{self.stats['articles_unchanged']} unchanged) in {duration:.1f}s"
            )
            
            return report
        
        except Exception as e:
            log.error(f"Crawl failed: {e}", exc_info=True)
            
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            
            self.stats['errors'].append(f"Fatal error: {str(e)}")
            
            return CrawlReport(
                started_at=start_iso,
                completed_at=end_time.isoformat() + 'Z',
                duration_seconds=duration,
                sections_found=self.stats['sections_found'],
                articles_total=self.stats['articles_total'],
                articles_new=self.stats['articles_new'],
                articles_updated=self.stats['articles_updated'],
                articles_unchanged=self.stats['articles_unchanged'],
                errors=self.stats['errors']
            )
    
    async def _crawl_sections(self) -> List[HelpshiftSection]:
        """Crawl home page and extract sections."""
        html = await self._fetch(self.HOME_URL)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        sections = []
        now_utc = datetime.utcnow().isoformat() + 'Z'
        
        # Try multiple selectors
        selectors = [
            ('a', {'href': lambda x: x and '/section/' in x}),
            ('div.section-card a', {}),
            ('.section-link', {})
        ]
        
        for tag, attrs in selectors:
            links = soup.find_all(tag, attrs) if isinstance(tag, str) else soup.select(tag)
            
            for link in links:
                href = link.get('href', '')
                if '/section/' not in href:
                    continue
                
                full_url = href if href.startswith('http') else self.BASE_URL + href
                title = link.get_text(strip=True)
                
                # Parse section ID
                section_id = HelpshiftSection.parse_id_from_url(full_url)
                if not section_id:
                    continue
                
                # Extract slug
                slug = full_url.split('/section/')[-1].rstrip('/')
                
                section = HelpshiftSection(
                    id=section_id,
                    slug=slug,
                    url=full_url,
                    name=title,
                    last_seen_utc=now_utc
                )
                
                sections.append(section)
                
                # Save section to DB
                try:
                    await self.database.upsert_section(section)
                except Exception as e:
                    log.error(f"Failed to save section {section_id}: {e}")
            
            if sections:
                break
        
        log.info(f"Found {len(sections)} sections")
        return sections
    
    async def _crawl_section(self, section: HelpshiftSection) -> List[int]:
        """
        Crawl a section page and extract article links.
        
        Returns:
            List of article IDs processed
        """
        html = await self._fetch(section.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        article_links = []
        
        # Try multiple selectors for article links
        selectors = [
            ('a', {'href': lambda x: x and '/faq/' in x}),
            ('div.article-list a', {}),
            ('.article-link', {})
        ]
        
        for tag, attrs in selectors:
            links = soup.find_all(tag, attrs) if isinstance(tag, str) else soup.select(tag)
            
            for link in links:
                href = link.get('href', '')
                if '/faq/' not in href:
                    continue
                
                full_url = href if href.startswith('http') else self.BASE_URL + href
                title = link.get_text(strip=True)
                
                article_links.append((title, full_url))
            
            if article_links:
                break
        
        log.debug(f"Section '{section.name}': found {len(article_links)} articles")
        
        # Fetch articles with concurrency limit
        tasks = [
            self._fetch_and_store_article(url, title, section)
            for title, url in article_links
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Collect successful article IDs
        article_ids = []
        for result in results:
            if isinstance(result, int):
                article_ids.append(result)
            elif isinstance(result, Exception):
                error_msg = f"Article fetch error: {result}"
                log.error(error_msg)
                self.stats['errors'].append(error_msg)
        
        return article_ids
    
    async def _fetch_and_store_article(
        self,
        url: str,
        expected_title: str,
        section: HelpshiftSection
    ) -> Optional[int]:
        """
        Fetch an article and store it in the database.
        
        Returns:
            Article ID if successful, None otherwise
        """
        async with self.semaphore:  # Limit concurrency
            try:
                html = await self._fetch(url)
                if not html:
                    return None
                
                soup = BeautifulSoup(html, 'lxml')
                
                # Extract title
                title = None
                for selector in ['h1', 'h2', '.article-title', '.faq-title']:
                    elem = soup.select_one(selector)
                    if elem:
                        title = elem.get_text(strip=True)
                        break
                
                if not title:
                    title = expected_title
                
                # Extract last updated
                last_updated = None
                for text in soup.stripped_strings:
                    if 'Last Updated:' in text or 'Updated:' in text:
                        parts = text.split(':')
                        if len(parts) > 1:
                            last_updated = parts[1].strip()
                        break
                
                # Extract body
                body_md = self._extract_body(soup)
                
                # Truncate if too long
                if len(body_md) > self.MAX_BODY_LENGTH:
                    body_md = body_md[:self.MAX_BODY_LENGTH] + "\n\n[Content truncated...]"
                
                # Parse article ID
                article_id = HelpshiftArticle.parse_id_from_url(url)
                if not article_id:
                    log.warning(f"Could not parse article ID from {url}")
                    return None
                
                # Extract slug
                slug = url.split('/faq/')[-1].rstrip('/')
                
                # Compute hash
                hash_body = HelpshiftArticle.compute_hash(body_md)
                
                # Create article
                article = HelpshiftArticle(
                    id=article_id,
                    slug=slug,
                    url=url,
                    title=title,
                    section_id=section.id,
                    section_name=section.name,
                    last_updated_text=last_updated,
                    last_seen_utc=datetime.utcnow().isoformat() + 'Z',
                    body_md=body_md,
                    hash_body=hash_body,
                    lang='en'
                )
                
                # Store in database
                status, version_saved = await self.database.upsert_article(article)
                
                self.stats['articles_total'] += 1
                
                if status == 'new':
                    self.stats['articles_new'] += 1
                elif status == 'updated':
                    self.stats['articles_updated'] += 1
                else:
                    self.stats['articles_unchanged'] += 1
                
                return article_id
            
            except Exception as e:
                error_msg = f"Error processing article {url}: {e}"
                log.error(error_msg, exc_info=True)
                self.stats['errors'].append(error_msg)
                return None
    
    def _extract_body(self, soup: BeautifulSoup) -> str:
        """
        Extract and normalize article body to markdown-like text.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            Normalized markdown text
        """
        body_selectors = [
            '.article-body',
            '.faq-body',
            '.content',
            'article',
            'main'
        ]
        
        body_elem = None
        for selector in body_selectors:
            body_elem = soup.select_one(selector)
            if body_elem:
                break
        
        if not body_elem:
            # Fallback: get all paragraphs
            paragraphs = soup.find_all('p')
            return '\n\n'.join(p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True))
        
        # Convert HTML to markdown-like text
        return self._html_to_markdown(body_elem)
    
    def _html_to_markdown(self, element) -> str:
        """Convert HTML element to simplified markdown."""
        parts = []
        
        for child in element.descendants:
            if child.name in ['h1', 'h2']:
                text = child.get_text(strip=True)
                if text and text not in ''.join(parts[-5:]):  # Avoid duplicates
                    parts.append(f"\n## {text}\n")
            elif child.name in ['h3', 'h4']:
                text = child.get_text(strip=True)
                if text and text not in ''.join(parts[-5:]):
                    parts.append(f"\n### {text}\n")
            elif child.name == 'li':
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"• {text}\n")
            elif child.name == 'p':
                text = child.get_text(strip=True)
                if text:
                    parts.append(f"{text}\n\n")
            elif isinstance(child, str):
                text = child.strip()
                if text and len(text) > 3:
                    # Only add if not already in recent parts
                    if text not in ''.join(parts[-10:]):
                        parts.append(text + " ")
        
        result = ''.join(parts)
        
        # Clean up excessive whitespace
        lines = [line.strip() for line in result.split('\n')]
        lines = [line for line in lines if line]
        
        return '\n\n'.join(lines)
    
    async def test_crawl(self, max_sections: int = 2, max_articles: int = 5) -> Dict[str, any]:
        """
        Test crawl without saving to database.
        
        Args:
            max_sections: Maximum sections to test
            max_articles: Maximum articles per section to test
            
        Returns:
            Dictionary with test results
        """
        log.info(f"Starting test crawl (max {max_sections} sections, {max_articles} articles each)")
        
        # Fetch sections
        html = await self._fetch(self.HOME_URL)
        if not html:
            return {'error': 'Failed to fetch home page'}
        
        soup = BeautifulSoup(html, 'lxml')
        section_links = soup.find_all('a', href=lambda x: x and '/section/' in x)
        
        results = {
            'sections_found': len(section_links),
            'sections_tested': [],
            'articles_tested': []
        }
        
        for link in section_links[:max_sections]:
            section_url = link.get('href')
            if not section_url.startswith('http'):
                section_url = self.BASE_URL + section_url
            
            section_name = link.get_text(strip=True)
            results['sections_tested'].append({'name': section_name, 'url': section_url})
            
            # Fetch section articles
            section_html = await self._fetch(section_url)
            if not section_html:
                continue
            
            section_soup = BeautifulSoup(section_html, 'lxml')
            article_links = section_soup.find_all('a', href=lambda x: x and '/faq/' in x)
            
            for article_link in article_links[:max_articles]:
                article_url = article_link.get('href')
                if not article_url.startswith('http'):
                    article_url = self.BASE_URL + article_url
                
                article_title = article_link.get_text(strip=True)
                
                # Fetch article
                article_html = await self._fetch(article_url)
                if article_html:
                    article_soup = BeautifulSoup(article_html, 'lxml')
                    body = self._extract_body(article_soup)
                    
                    results['articles_tested'].append({
                        'title': article_title,
                        'url': article_url,
                        'body_length': len(body),
                        'body_preview': body[:200] + '...' if len(body) > 200 else body
                    })
        
        return results
