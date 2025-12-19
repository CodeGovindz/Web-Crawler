"""
Main Crawler - Orchestrates all crawling components.

Features:
- Async crawl loop with worker pool
- Progress tracking with rich console
- Signal handling for graceful shutdown
- Statistics and reporting
"""

import asyncio
import signal
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from .config import CrawlerConfig, DomainConfig
from .fetcher import HTTPFetcher, FetchResult
from .frontier import URLFrontier, Priority, URLItem, get_domain
from .parser import HTMLParser, ParsedPage
from .renderer import BrowserRenderer, RenderResult
from .robots import RobotsManager
from .sitemap import SitemapManager
from .storage import StorageManager


@dataclass
class CrawlStats:
    """Crawl statistics."""
    pages_crawled: int = 0
    pages_failed: int = 0
    pages_skipped: int = 0
    bytes_downloaded: int = 0
    
    @property
    def total(self) -> int:
        return self.pages_crawled + self.pages_failed + self.pages_skipped


class Crawler:
    """
    Main crawler orchestrator.
    
    Coordinates all components for efficient web crawling.
    """
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.console = Console()
        
        # Components
        self.frontier = URLFrontier()
        self.fetcher: Optional[HTTPFetcher] = None
        self.renderer: Optional[BrowserRenderer] = None
        self.robots = RobotsManager()
        self.sitemap = SitemapManager()
        self.storage: Optional[StorageManager] = None
        
        # Domain tracking
        self._domain_configs: dict[str, DomainConfig] = {}
        
        # Stats
        self.stats = CrawlStats()
        
        # Control
        self._running = False
        self._shutdown_event = asyncio.Event()
    
    async def _init_components(self) -> None:
        """Initialize all crawl components."""
        # HTTP Fetcher
        self.fetcher = HTTPFetcher(self.config)
        await self.fetcher.start()
        
        # Browser renderer (only if enabled)
        if self.config.enable_rendering:
            self.renderer = BrowserRenderer(self.config)
            await self.renderer.start()
        
        # Storage
        self.storage = StorageManager(
            self.config.db_path,
            self.config.output_dir
        )
    
    async def _cleanup(self) -> None:
        """Cleanup resources."""
        if self.fetcher:
            await self.fetcher.stop()
        if self.renderer:
            await self.renderer.stop()
        if self.storage:
            await self.storage.close()
    
    def _get_domain_config(self, url: str) -> DomainConfig:
        """Get or create domain configuration."""
        domain = get_domain(url)
        if domain not in self._domain_configs:
            self._domain_configs[domain] = DomainConfig(domain)
        return self._domain_configs[domain]
    
    async def _check_robots(self, url: str) -> bool:
        """Check if URL is allowed by robots.txt."""
        if not self.config.respect_robots_txt:
            return True
        
        domain = get_domain(url)
        domain_config = self._get_domain_config(url)
        
        # Get robots rules
        rules = await self.robots.get_rules(domain, self.fetcher.fetch_simple)
        
        # Update crawl delay
        if rules.crawl_delay:
            domain_config.update_crawl_delay(rules.crawl_delay)
            self.fetcher.rate_limiter.update_delay(domain, rules.crawl_delay)
        
        # Check if allowed
        return await self.robots.can_fetch(url, rules)
    
    async def _process_sitemaps(self, domain: str) -> None:
        """Process sitemaps for a domain."""
        if not self.config.parse_sitemaps:
            return
        
        # Get robots rules for sitemap URLs
        rules = await self.robots.get_rules(domain, self.fetcher.fetch_simple)
        
        # Discover and process sitemaps
        sitemaps = await self.sitemap.discover_sitemaps(
            domain,
            rules.sitemaps,
            self.fetcher.fetch_simple
        )
        
        for sitemap_url in sitemaps[:5]:  # Limit to 5 sitemaps
            urls = await self.sitemap.process_sitemap(
                sitemap_url,
                self.fetcher.fetch_simple
            )
            
            # Add URLs to frontier with high priority
            for url_item in urls[:1000]:  # Limit URLs from sitemap
                await self.frontier.add(
                    url_item.loc,
                    priority=Priority.HIGH,
                    depth=1
                )
    
    async def _fetch_page(self, item: URLItem) -> Optional[FetchResult | RenderResult]:
        """Fetch a page using appropriate method."""
        # Check robots.txt
        if not await self._check_robots(item.url):
            self.stats.pages_skipped += 1
            return None
        
        # Decide whether to render
        if self.config.enable_rendering and self.renderer:
            result = await self.renderer.render(item.url)
            return result
        else:
            result = await self.fetcher.fetch(item.url, item.parent_url)
            return result
    
    async def _process_page(
        self,
        url: str,
        html: str,
        depth: int
    ) -> ParsedPage:
        """Process page content and extract links."""
        parser = HTMLParser(url)
        parsed = parser.parse(html)
        
        # Get crawlable links
        if depth < self.config.max_depth:
            links = parser.get_crawlable_links(
                parsed,
                respect_nofollow=self.config.respect_nofollow,
                internal_only=True
            )
            
            # Add to frontier
            await self.frontier.add_many(
                links,
                priority=Priority.NORMAL,
                depth=depth + 1,
                parent_url=url
            )
        
        return parsed
    
    async def _worker(self, worker_id: int) -> None:
        """Worker coroutine that processes URLs."""
        while self._running and not self._shutdown_event.is_set():
            # Check max pages
            if self.stats.total >= self.config.max_pages:
                break
            
            # Get next URL
            item = await self.frontier.get(timeout=2.0)
            if not item:
                # Check if we're done
                if self.frontier.is_empty():
                    break
                continue
            
            try:
                # Fetch the page
                result = await self._fetch_page(item)
                
                if result is None:
                    await self.frontier.complete(item.url, success=False)
                    continue
                
                # Handle fetch result
                if isinstance(result, FetchResult):
                    if not result.success:
                        self.stats.pages_failed += 1
                        await self.storage.db.mark_url_crawled(
                            self.storage.session_id,
                            item.url,
                            result.status,
                            error=result.error
                        )
                        await self.frontier.complete(item.url, success=False)
                        continue
                    
                    html = result.html
                    status = result.status
                    content_type = result.content_type
                else:
                    # RenderResult
                    if not result.success:
                        self.stats.pages_failed += 1
                        await self.storage.db.mark_url_crawled(
                            self.storage.session_id,
                            item.url,
                            result.status,
                            error=result.error
                        )
                        await self.frontier.complete(item.url, success=False)
                        continue
                    
                    html = result.html
                    status = result.status
                    content_type = 'text/html'
                
                # Process the page
                if html:
                    parsed = await self._process_page(item.url, html, item.depth)
                    
                    # Save content
                    content = {
                        'title': parsed.metadata.title,
                        'description': parsed.metadata.description,
                        'text': parsed.text_content[:10000],  # Limit text
                        'links_count': len(parsed.links),
                        'depth': item.depth
                    }
                    
                    if self.config.save_html:
                        content['html'] = html[:100000]  # Limit HTML
                    
                    await self.storage.content.save_page(item.url, content)
                
                # Update stats
                self.stats.pages_crawled += 1
                await self.storage.db.mark_url_crawled(
                    self.storage.session_id,
                    item.url,
                    status,
                    content_type
                )
                await self.frontier.complete(item.url, success=True)
            
            except Exception as e:
                self.stats.pages_failed += 1
                await self.frontier.complete(item.url, success=False)
                self.console.print(f"[red]Error processing {item.url}: {e}[/red]")
    
    def _create_progress_table(self) -> Table:
        """Create progress display table."""
        table = Table(show_header=False, box=None)
        table.add_column(style="cyan")
        table.add_column(style="green")
        
        stats = self.stats
        frontier_stats = {
            'queued': self.frontier.size,
            'in_progress': self.frontier.in_progress_count,
            'seen': self.frontier.seen_count
        }
        
        table.add_row("Pages Crawled", str(stats.pages_crawled))
        table.add_row("Pages Failed", str(stats.pages_failed))
        table.add_row("Pages Skipped", str(stats.pages_skipped))
        table.add_row("Queue Size", str(frontier_stats['queued']))
        table.add_row("In Progress", str(frontier_stats['in_progress']))
        table.add_row("URLs Seen", str(frontier_stats['seen']))
        
        return table
    
    async def crawl(
        self,
        seed_url: str,
        resume: bool = False
    ) -> CrawlStats:
        """
        Start crawling from seed URL.
        
        Args:
            seed_url: Starting URL
            resume: Resume previous crawl session
        
        Returns:
            CrawlStats with final statistics
        """
        self.console.print(f"\n[bold blue]🕷️  Starting crawl: {seed_url}[/bold blue]\n")
        
        # Initialize components
        await self._init_components()
        
        # Setup signal handlers
        def handle_shutdown(sig, frame):
            self.console.print("\n[yellow]Shutting down gracefully...[/yellow]")
            self._shutdown_event.set()
        
        if sys.platform != 'win32':
            signal.signal(signal.SIGINT, handle_shutdown)
            signal.signal(signal.SIGTERM, handle_shutdown)
        
        try:
            # Start or resume session
            if resume:
                session_id = await self.storage.resume_session()
                if not session_id:
                    self.console.print("[yellow]No session to resume, starting new crawl[/yellow]")
                    session_id = await self.storage.start_session(seed_url)
                    await self.frontier.add(seed_url, Priority.HIGHEST, depth=0)
                else:
                    # Load pending URLs
                    pending = await self.storage.db.get_pending_urls(session_id)
                    for p in pending:
                        await self.frontier.add(p['url'], Priority.NORMAL, p['depth'])
            else:
                session_id = await self.storage.start_session(seed_url)
                await self.frontier.add(seed_url, Priority.HIGHEST, depth=0)
            
            # Add seed URL to database
            await self.storage.db.add_url(session_id, seed_url, depth=0)
            
            # Process sitemaps
            domain = get_domain(seed_url)
            await self._process_sitemaps(domain)
            
            self._running = True
            
            # Start workers with progress display
            with Live(
                Panel(self._create_progress_table(), title="Crawl Progress"),
                refresh_per_second=2,
                console=self.console
            ) as live:
                # Create worker tasks
                workers = [
                    asyncio.create_task(self._worker(i))
                    for i in range(self.config.concurrent_requests)
                ]
                
                # Update display while workers run
                while self._running and not self._shutdown_event.is_set():
                    # Check if all workers are done
                    if all(w.done() for w in workers):
                        break
                    
                    # Check max pages
                    if self.stats.total >= self.config.max_pages:
                        self._running = False
                        break
                    
                    live.update(Panel(self._create_progress_table(), title="Crawl Progress"))
                    await asyncio.sleep(0.5)
                
                # Cancel remaining workers
                for w in workers:
                    if not w.done():
                        w.cancel()
                
                await asyncio.gather(*workers, return_exceptions=True)
            
            # Update session
            await self.storage.db.update_session(
                session_id,
                status='completed',
                pages_crawled=self.stats.pages_crawled,
                pages_failed=self.stats.pages_failed
            )
            
        finally:
            await self._cleanup()
        
        # Print summary
        self.console.print("\n[bold green]✓ Crawl completed![/bold green]")
        self.console.print(f"  Pages crawled: {self.stats.pages_crawled}")
        self.console.print(f"  Pages failed: {self.stats.pages_failed}")
        self.console.print(f"  Output: {self.config.output_dir}")
        
        return self.stats


async def run_crawler(
    seed_url: str,
    config: Optional[CrawlerConfig] = None,
    **kwargs
) -> CrawlStats:
    """
    Convenience function to run crawler.
    
    Args:
        seed_url: Starting URL
        config: Crawler configuration
        **kwargs: Config overrides
    
    Returns:
        CrawlStats
    """
    if config is None:
        config = CrawlerConfig(**kwargs)
    
    crawler = Crawler(config)
    return await crawler.crawl(seed_url)
