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
    
    async def _fetch(self, url: str, retries: int = 2) -> Optional[str]:
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
                log.warning(f"Timeout fetching {url}")
                return None
            except aiohttp.ClientError as e:
                log.error(f"Client error: {e}")
                return None
        
        return None
    
    async def crawl_full(self) -> CrawlReport:
        """Perform a full crawl of all sections and articles."""
        start_time = datetime.utcnow()
        start_iso = start_time.isoformat() + 'Z'
        
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
            
            sections = await self._crawl_sections()
            self.stats['sections_found'] = len(sections)
            
            if not sections:
                log.error("No sections found")
                self.stats['errors'].append("No sections found on home page")
            
            seen_article_ids: Set[int] = set()
            
            for section in sections:
                try:
                    article_ids = await self._crawl_section(section)
                    seen_article_ids.update(article_ids)
                except Exception as e:
                    error_msg = f"Error crawling section {section.name}: {e}"
                    log.error(error_msg, exc_info=True)
                    self.stats['errors'].append(error_msg)
            
            deleted_count = 0
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
                articles_deleted=deleted_count,
                errors=self.stats['errors']
            )
            
            await self.database.save_crawl_report(report)
            
            log.info(
                f"Crawl completed: {self.stats['articles_total']} articles "
                f"({self.stats['articles_new']} new, {self.stats['articles_updated']} updated) "
                f"in {duration:.1f}s"
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
        
        links = soup.find_all('a', href=lambda x: x and '/section/' in x)
        
        for link in links:
            href = link.get('href', '')
            if '/section/' not in href:
                continue
            
            full_url = href if href.startswith('http') else self.BASE_URL + href
            title = link.get_text(strip=True)
            
            section_id = HelpshiftSection.parse_id_from_url(full_url)
            if not section_id:
                continue
            
            slug = full_url.split('/section/')[-1].rstrip('/')
            
            section = HelpshiftSection(
                id=section_id,
                slug=slug,
                url=full_url,
                name=title,
                last_seen_utc=now_utc
            )
            
            sections.append(section)
            
            try:
                await self.database.upsert_section(section)
            except Exception as e:
                log.error(f"Failed to save section {section_id}: {e}")
        
        log.info(f"Found {len(sections)} sections")
        return sections
    
    async def _crawl_section(self, section: HelpshiftSection) -> List[int]:
        """Crawl a section page and extract article links."""
        html = await self._fetch(section.url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'lxml')
        article_links = []
        
        links = soup.find_all('a', href=lambda x: x and '/faq/' in x)
        
        for link in links:
            href = link.get('href', '')
            if '/faq/' not in href:
                continue
            
            full_url = href if href.startswith('http') else self.BASE_URL + href
            title = link.get_text(strip=True)
            
            article_links.append((title, full_url))
        
        log.debug(f"Section '{section.name}': found {len(article_links)} articles")
        
        tasks = [
            self._fetch_and_store_article(url, title, section)
            for title, url in article_links
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
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
        """Fetch an article and store it in the database."""
        async with self.semaphore:
            try:
                html = await self._fetch(url)
                if not html:
                    return None
                
                soup = BeautifulSoup(html, 'lxml')
                
                title = None
                for selector in ['h1', 'h2', '.article-title', '.faq-title']:
                    elem = soup.select_one(selector)
                    if elem:
                        title = elem.get_text(strip=True)
                        break
                
                if not title:
                    title = expected_title
                
                last_updated = None
                for text in soup.stripped_strings:
                    if 'Last Updated:' in text or 'Updated:' in text:
                        parts = text.split(':')
                        if len(parts) > 1:
                            last_updated = parts[1].strip()
                        break
                
                body_md = self._extract_body(soup)
                
                if len(body_md) > self.MAX_BODY_LENGTH:
                    body_md = body_md[:self.MAX_BODY_LENGTH] + "\n\n[Content truncated...]"
                
                article_id = HelpshiftArticle.parse_id_from_url(url)
                if not article_id:
                    log.warning(f"Could not parse article ID from {url}")
                    return None
                
                slug = url.split('/faq/')[-1].rstrip('/')
                hash_body = HelpshiftArticle.compute_hash(body_md)
                
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
        """Extract and normalize article body to markdown-like text."""
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
    
    async def test_crawl(self, max_sections: int = 2, max_articles: int = 5) -> Dict:
        """Test crawl without saving to database."""
        log.info(f"Starting test crawl (max {max_sections} sections, {max_articles} articles each)")
        
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


class HelpshiftScraper:
    """Backwards compatibility wrapper for old HelpshiftScraper interface."""
    
    def __init__(self, cache_ttl: int = 600):
        self.cache_ttl = cache_ttl
        self.database: Optional[FAQDatabase] = None
    
    def set_database(self, database: FAQDatabase):
        """Set database reference (called by cog)."""
        self.database = database
    
    async def close(self):
        """Compatibility method."""
        pass
    
    async def search_all_articles(self, query: str, max_articles: int = 20) -> List[HelpshiftArticle]:
        """Search local database for articles."""
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
        """Compatibility method."""
        pass
