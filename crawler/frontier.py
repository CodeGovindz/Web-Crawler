"""
URL Frontier - Priority queue with deduplication for URL management.

Features:
- Priority-based URL scheduling
- Bloom filter for seen URL deduplication
- Per-domain politeness tracking
- Async-safe operations
"""

import asyncio
import hashlib
import heapq
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Optional, Set
from urllib.parse import urlparse, urlunparse, urljoin


class Priority(IntEnum):
    """URL priority levels."""
    HIGHEST = 0  # Seed URLs, sitemaps
    HIGH = 1     # Same-domain links from high-priority pages
    NORMAL = 2   # Regular discovered links
    LOW = 3      # External links, low-value pages


@dataclass(order=True)
class URLItem:
    """A URL item in the frontier queue."""
    priority: int
    timestamp: float = field(compare=False)
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    parent_url: Optional[str] = field(compare=False, default=None)
    retry_count: int = field(compare=False, default=0)
    
    def __hash__(self):
        return hash(self.url)


class BloomFilter:
    """Simple bloom filter for URL deduplication."""
    
    def __init__(self, size: int = 1_000_000, hash_count: int = 5):
        self.size = size
        self.hash_count = hash_count
        self.bit_array = [False] * size
        self._count = 0
    
    def _get_hashes(self, item: str) -> list[int]:
        """Generate hash values for an item."""
        hashes = []
        for i in range(self.hash_count):
            h = hashlib.md5(f"{item}{i}".encode()).hexdigest()
            hashes.append(int(h, 16) % self.size)
        return hashes
    
    def add(self, item: str) -> None:
        """Add an item to the filter."""
        for h in self._get_hashes(item):
            self.bit_array[h] = True
        self._count += 1
    
    def __contains__(self, item: str) -> bool:
        """Check if item might be in the filter."""
        return all(self.bit_array[h] for h in self._get_hashes(item))
    
    def __len__(self) -> int:
        return self._count


def normalize_url(url: str, base_url: Optional[str] = None) -> Optional[str]:
    """
    Normalize a URL for consistent comparison.
    
    - Resolves relative URLs against base
    - Lowercases scheme and host
    - Removes fragments
    - Removes trailing slashes (except root)
    - Sorts query parameters
    """
    try:
        # Resolve relative URLs
        if base_url:
            url = urljoin(base_url, url)
        
        parsed = urlparse(url)
        
        # Skip non-HTTP(S) URLs
        if parsed.scheme not in ('http', 'https'):
            return None
        
        # Lowercase scheme and host
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        
        # Remove default ports
        if netloc.endswith(':80') and scheme == 'http':
            netloc = netloc[:-3]
        elif netloc.endswith(':443') and scheme == 'https':
            netloc = netloc[:-4]
        
        # Normalize path
        path = parsed.path or '/'
        if path != '/' and path.endswith('/'):
            path = path.rstrip('/')
        
        # Sort query parameters
        query = parsed.query
        if query:
            params = sorted(query.split('&'))
            query = '&'.join(params)
        
        # Reconstruct without fragment
        normalized = urlunparse((scheme, netloc, path, parsed.params, query, ''))
        return normalized
    
    except Exception:
        return None


def get_domain(url: str) -> str:
    """Extract domain from URL."""
    parsed = urlparse(url)
    return parsed.netloc.lower()


class URLFrontier:
    """
    URL Frontier with priority queue and deduplication.
    
    Thread-safe async implementation for concurrent crawling.
    """
    
    def __init__(self, max_size: int = 100_000):
        self._queue: list[URLItem] = []
        self._seen = BloomFilter(size=max_size * 10)
        self._in_progress: Set[str] = set()
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition()
        self._domain_queues: dict[str, list[URLItem]] = {}
        self._completed_count = 0
        self._error_count = 0
    
    async def add(
        self,
        url: str,
        priority: Priority = Priority.NORMAL,
        depth: int = 0,
        parent_url: Optional[str] = None
    ) -> bool:
        """
        Add a URL to the frontier.
        
        Returns True if URL was added, False if already seen.
        """
        normalized = normalize_url(url, parent_url)
        if not normalized:
            return False
        
        async with self._lock:
            if normalized in self._seen or normalized in self._in_progress:
                return False
            
            self._seen.add(normalized)
            
            item = URLItem(
                priority=priority,
                timestamp=time.time(),
                url=normalized,
                depth=depth,
                parent_url=parent_url
            )
            
            heapq.heappush(self._queue, item)
            
            # Signal waiting consumers
            async with self._not_empty:
                self._not_empty.notify()
            
            return True
    
    async def add_many(
        self,
        urls: list[str],
        priority: Priority = Priority.NORMAL,
        depth: int = 0,
        parent_url: Optional[str] = None
    ) -> int:
        """Add multiple URLs. Returns count of URLs actually added."""
        added = 0
        for url in urls:
            if await self.add(url, priority, depth, parent_url):
                added += 1
        return added
    
    async def get(self, timeout: Optional[float] = None) -> Optional[URLItem]:
        """
        Get the next URL to crawl.
        
        Returns None if timeout expires with no URL available.
        """
        async with self._not_empty:
            while not self._queue:
                try:
                    await asyncio.wait_for(
                        self._not_empty.wait(),
                        timeout=timeout
                    )
                except asyncio.TimeoutError:
                    return None
        
        async with self._lock:
            if not self._queue:
                return None
            
            item = heapq.heappop(self._queue)
            self._in_progress.add(item.url)
            return item
    
    async def complete(self, url: str, success: bool = True) -> None:
        """Mark a URL as completed."""
        async with self._lock:
            self._in_progress.discard(url)
            if success:
                self._completed_count += 1
            else:
                self._error_count += 1
    
    async def retry(self, item: URLItem, max_retries: int = 3) -> bool:
        """
        Re-queue a URL for retry with increased priority penalty.
        
        Returns True if requeued, False if max retries exceeded.
        """
        async with self._lock:
            self._in_progress.discard(item.url)
            
            if item.retry_count >= max_retries:
                self._error_count += 1
                return False
            
            # Create retry item with lower priority
            retry_item = URLItem(
                priority=min(item.priority + 1, Priority.LOW),
                timestamp=time.time(),
                url=item.url,
                depth=item.depth,
                parent_url=item.parent_url,
                retry_count=item.retry_count + 1
            )
            
            heapq.heappush(self._queue, retry_item)
            return True
    
    @property
    def size(self) -> int:
        """Number of URLs in queue."""
        return len(self._queue)
    
    @property
    def in_progress_count(self) -> int:
        """Number of URLs being processed."""
        return len(self._in_progress)
    
    @property
    def seen_count(self) -> int:
        """Total URLs seen (including completed)."""
        return len(self._seen)
    
    @property
    def completed_count(self) -> int:
        """Total URLs successfully completed."""
        return self._completed_count
    
    @property
    def error_count(self) -> int:
        """Total URLs that errored."""
        return self._error_count
    
    def is_empty(self) -> bool:
        """Check if frontier is empty and nothing in progress."""
        return len(self._queue) == 0 and len(self._in_progress) == 0
    
    async def get_stats(self) -> dict:
        """Get frontier statistics."""
        async with self._lock:
            return {
                "queued": len(self._queue),
                "in_progress": len(self._in_progress),
                "seen": len(self._seen),
                "completed": self._completed_count,
                "errors": self._error_count
            }
