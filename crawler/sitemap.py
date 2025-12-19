"""
Sitemap Parser - XML sitemap and sitemap index parsing.

Features:
- Standard XML sitemap support
- Sitemap index handling
- Gzip compressed sitemaps
- Priority and change frequency parsing
"""

import asyncio
import gzip
import io
import re
from dataclasses import dataclass
from typing import Optional
from xml.etree import ElementTree as ET


@dataclass
class SitemapURL:
    """A URL entry from a sitemap."""
    loc: str
    lastmod: Optional[str] = None
    changefreq: Optional[str] = None
    priority: Optional[float] = None


class SitemapParser:
    """
    Parses XML sitemaps and sitemap indexes.
    """
    
    # XML namespaces
    SITEMAP_NS = {
        'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'
    }
    
    def __init__(self):
        self._cache: dict[str, list[SitemapURL]] = {}
    
    def parse(self, content: str) -> tuple[list[SitemapURL], list[str]]:
        """
        Parse sitemap content.
        
        Args:
            content: XML sitemap content
        
        Returns:
            Tuple of (list of URLs, list of sub-sitemap URLs)
        """
        urls: list[SitemapURL] = []
        sitemaps: list[str] = []
        
        try:
            # Handle potential gzip content
            if content.startswith('\x1f\x8b'):
                content = gzip.decompress(content.encode('latin-1')).decode('utf-8')
            
            root = ET.fromstring(content)
            
            # Remove namespace prefixes for easier parsing
            for elem in root.iter():
                if '}' in elem.tag:
                    elem.tag = elem.tag.split('}')[1]
            
            # Check if this is a sitemap index
            if root.tag == 'sitemapindex':
                for sitemap in root.findall('.//sitemap'):
                    loc = sitemap.find('loc')
                    if loc is not None and loc.text:
                        sitemaps.append(loc.text.strip())
            
            # Parse URL entries
            for url_elem in root.findall('.//url'):
                loc = url_elem.find('loc')
                if loc is None or not loc.text:
                    continue
                
                lastmod = url_elem.find('lastmod')
                changefreq = url_elem.find('changefreq')
                priority = url_elem.find('priority')
                
                url = SitemapURL(
                    loc=loc.text.strip(),
                    lastmod=lastmod.text.strip() if lastmod is not None and lastmod.text else None,
                    changefreq=changefreq.text.strip() if changefreq is not None and changefreq.text else None,
                    priority=float(priority.text) if priority is not None and priority.text else None
                )
                urls.append(url)
        
        except ET.ParseError:
            # Try regex fallback for malformed XML
            urls = self._parse_with_regex(content)
        except Exception:
            pass
        
        return urls, sitemaps
    
    def _parse_with_regex(self, content: str) -> list[SitemapURL]:
        """Fallback regex parser for malformed XML."""
        urls = []
        
        # Find all <loc>...</loc> entries
        loc_pattern = re.compile(r'<loc>\s*([^<]+)\s*</loc>', re.IGNORECASE)
        
        for match in loc_pattern.finditer(content):
            url = match.group(1).strip()
            if url.startswith('http'):
                urls.append(SitemapURL(loc=url))
        
        return urls


class SitemapManager:
    """
    Manages sitemap discovery and parsing.
    """
    
    def __init__(self):
        self.parser = SitemapParser()
        self._discovered_urls: list[SitemapURL] = []
        self._processed_sitemaps: set[str] = set()
    
    async def process_sitemap(
        self,
        sitemap_url: str,
        fetch_func,
        max_depth: int = 3
    ) -> list[SitemapURL]:
        """
        Process a sitemap URL recursively.
        
        Args:
            sitemap_url: URL of the sitemap
            fetch_func: Async function to fetch URL content
            max_depth: Maximum recursion depth for sitemap indexes
        
        Returns:
            List of discovered URLs
        """
        if sitemap_url in self._processed_sitemaps:
            return []
        
        if max_depth <= 0:
            return []
        
        self._processed_sitemaps.add(sitemap_url)
        
        try:
            response = await fetch_func(sitemap_url)
            if not response or response.get('status') != 200:
                return []
            
            content = response.get('text', '')
            if not content:
                return []
            
            urls, sub_sitemaps = self.parser.parse(content)
            
            # Process sub-sitemaps
            for sub_sitemap in sub_sitemaps:
                sub_urls = await self.process_sitemap(
                    sub_sitemap,
                    fetch_func,
                    max_depth - 1
                )
                urls.extend(sub_urls)
            
            self._discovered_urls.extend(urls)
            return urls
        
        except Exception:
            return []
    
    async def discover_sitemaps(
        self,
        domain: str,
        robots_sitemaps: list[str],
        fetch_func
    ) -> list[str]:
        """
        Discover all sitemaps for a domain.
        
        Checks:
        1. Sitemaps from robots.txt
        2. Common sitemap locations
        """
        sitemaps = list(robots_sitemaps)
        
        # Common sitemap locations
        common_locations = [
            f"https://{domain}/sitemap.xml",
            f"https://{domain}/sitemap_index.xml",
            f"https://{domain}/sitemap/sitemap.xml",
            f"https://{domain}/sitemaps/sitemap.xml",
        ]
        
        for location in common_locations:
            if location not in sitemaps:
                try:
                    response = await fetch_func(location)
                    if response and response.get('status') == 200:
                        sitemaps.append(location)
                except Exception:
                    pass
        
        return sitemaps
    
    def get_all_urls(self) -> list[SitemapURL]:
        """Get all discovered URLs."""
        return self._discovered_urls.copy()
    
    def clear(self) -> None:
        """Clear discovered URLs and processed sitemaps."""
        self._discovered_urls.clear()
        self._processed_sitemaps.clear()
