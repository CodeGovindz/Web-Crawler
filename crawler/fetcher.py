"""
HTTP Fetcher - Async HTTP client with retry logic and rate limiting.

Features:
- aiohttp-based async requests
- Automatic retry with exponential backoff
- Per-domain rate limiting
- Content-type detection
- Response caching support
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp
from aiohttp import ClientTimeout, TCPConnector

from .config import CrawlerConfig
from .frontier import get_domain
from .stealth import StealthManager


@dataclass
class FetchResult:
    """Result from fetching a URL."""
    url: str
    status: int
    content_type: Optional[str]
    text: Optional[str]
    html: Optional[str]
    headers: dict
    elapsed: float
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return 200 <= self.status < 400 and self.error is None
    
    @property
    def is_html(self) -> bool:
        if not self.content_type:
            return False
        return 'text/html' in self.content_type or 'xhtml' in self.content_type


class RateLimiter:
    """Per-domain rate limiter."""
    
    def __init__(self, requests_per_second: float = 2.0):
        self.min_interval = 1.0 / requests_per_second
        self._last_request: dict[str, float] = {}
        self._lock = asyncio.Lock()
    
    async def acquire(self, domain: str) -> None:
        """Wait until we can make a request to this domain."""
        async with self._lock:
            now = time.time()
            last = self._last_request.get(domain, 0)
            wait_time = self.min_interval - (now - last)
            
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            
            self._last_request[domain] = time.time()
    
    def update_delay(self, domain: str, delay: float) -> None:
        """Update minimum delay for a domain (e.g., from robots.txt)."""
        self.min_interval = max(self.min_interval, delay)


class HTTPFetcher:
    """
    Async HTTP fetcher with retry logic and stealth features.
    """
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.stealth = StealthManager(
            custom_user_agent=config.user_agent,
            rotate_agents=config.rotate_user_agents,
            proxy_list=config.proxy_list if config.proxy_rotation else None
        )
        self.rate_limiter = RateLimiter(config.requests_per_second)
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[TCPConnector] = None
    
    async def start(self) -> None:
        """Initialize the HTTP session."""
        self._connector = TCPConnector(
            limit=self.config.concurrent_requests,
            limit_per_host=5,
            ttl_dns_cache=300,
            enable_cleanup_closed=True
        )
        
        timeout = ClientTimeout(total=self.config.request_timeout)
        
        self._session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout,
            raise_for_status=False
        )
    
    async def stop(self) -> None:
        """Close the HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._connector:
            await self._connector.close()
            self._connector = None
    
    async def fetch(
        self,
        url: str,
        referer: Optional[str] = None,
        retry_count: int = 0
    ) -> FetchResult:
        """
        Fetch a URL with retry logic.
        
        Args:
            url: URL to fetch
            referer: Optional referer URL
            retry_count: Current retry attempt
        
        Returns:
            FetchResult with response data or error
        """
        if not self._session:
            await self.start()
        
        domain = get_domain(url)
        
        # Rate limit
        await self.rate_limiter.acquire(domain)
        
        # Add human-like delay
        delay = self.stealth.get_delay(
            self.config.delay_min,
            self.config.delay_max
        )
        await asyncio.sleep(delay)
        
        # Get headers
        headers = self.stealth.get_headers(referer)
        
        # Get proxy if configured
        proxy = self.stealth.get_proxy() if self.config.proxy_rotation else self.config.proxy_url
        
        start_time = time.time()
        
        try:
            async with self._session.get(
                url,
                headers=headers,
                proxy=proxy,
                allow_redirects=True,
                max_redirects=5
            ) as response:
                elapsed = time.time() - start_time
                
                content_type = response.headers.get('Content-Type', '')
                
                # Check content length
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > self.config.max_content_length:
                    return FetchResult(
                        url=str(response.url),
                        status=response.status,
                        content_type=content_type,
                        text=None,
                        html=None,
                        headers=dict(response.headers),
                        elapsed=elapsed,
                        error="Content too large"
                    )
                
                # Read content for HTML
                text = None
                html = None
                
                if 'text/html' in content_type or 'xhtml' in content_type:
                    try:
                        html = await response.text()
                        text = html
                    except Exception as e:
                        return FetchResult(
                            url=str(response.url),
                            status=response.status,
                            content_type=content_type,
                            text=None,
                            html=None,
                            headers=dict(response.headers),
                            elapsed=elapsed,
                            error=f"Failed to read content: {e}"
                        )
                elif 'text/' in content_type:
                    text = await response.text()
                
                return FetchResult(
                    url=str(response.url),
                    status=response.status,
                    content_type=content_type,
                    text=text,
                    html=html,
                    headers=dict(response.headers),
                    elapsed=elapsed
                )
        
        except asyncio.TimeoutError:
            elapsed = time.time() - start_time
            if retry_count < self.config.max_retries:
                # Exponential backoff
                await asyncio.sleep(2 ** retry_count)
                return await self.fetch(url, referer, retry_count + 1)
            return FetchResult(
                url=url,
                status=0,
                content_type=None,
                text=None,
                html=None,
                headers={},
                elapsed=elapsed,
                error="Request timeout"
            )
        
        except aiohttp.ClientError as e:
            elapsed = time.time() - start_time
            if retry_count < self.config.max_retries:
                await asyncio.sleep(2 ** retry_count)
                return await self.fetch(url, referer, retry_count + 1)
            return FetchResult(
                url=url,
                status=0,
                content_type=None,
                text=None,
                html=None,
                headers={},
                elapsed=elapsed,
                error=f"Client error: {e}"
            )
        
        except Exception as e:
            elapsed = time.time() - start_time
            return FetchResult(
                url=url,
                status=0,
                content_type=None,
                text=None,
                html=None,
                headers={},
                elapsed=elapsed,
                error=f"Unexpected error: {e}"
            )
    
    async def fetch_simple(self, url: str) -> dict:
        """
        Simple fetch returning dict (for robots.txt fetching).
        """
        result = await self.fetch(url)
        return {
            'status': result.status,
            'text': result.text,
            'error': result.error
        }


async def create_fetcher(config: CrawlerConfig) -> HTTPFetcher:
    """Create and initialize an HTTP fetcher."""
    fetcher = HTTPFetcher(config)
    await fetcher.start()
    return fetcher
