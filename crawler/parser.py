"""
HTML Parser - Content extraction and link discovery.

Features:
- BeautifulSoup + lxml parsing
- Link extraction with URL normalization
- Text content extraction
- Metadata extraction (title, description, etc.)
- Structured data detection (JSON-LD, microdata)
"""

import json
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup, Comment


@dataclass
class PageMetadata:
    """Extracted page metadata."""
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[list[str]] = None
    author: Optional[str] = None
    canonical_url: Optional[str] = None
    language: Optional[str] = None
    og_title: Optional[str] = None
    og_description: Optional[str] = None
    og_image: Optional[str] = None
    og_type: Optional[str] = None
    twitter_card: Optional[str] = None
    robots: Optional[str] = None


@dataclass
class ExtractedLink:
    """An extracted link from a page."""
    url: str
    text: str
    is_internal: bool
    nofollow: bool
    link_type: str  # 'anchor', 'form', 'frame', etc.


@dataclass
class ParsedPage:
    """Complete parsed page data."""
    url: str
    metadata: PageMetadata
    text_content: str
    links: list[ExtractedLink] = field(default_factory=list)
    structured_data: list[dict] = field(default_factory=list)
    headings: dict[str, list[str]] = field(default_factory=dict)
    images: list[dict] = field(default_factory=list)


class HTMLParser:
    """
    Parses HTML content and extracts structured data.
    """
    
    # Tags to ignore for text extraction
    IGNORE_TAGS = {
        'script', 'style', 'noscript', 'header', 'footer',
        'nav', 'aside', 'form', 'button', 'input', 'select',
        'textarea', 'iframe', 'svg', 'canvas'
    }
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc.lower()
    
    def parse(self, html: str) -> ParsedPage:
        """
        Parse HTML content and extract all data.
        
        Args:
            html: HTML content string
        
        Returns:
            ParsedPage with extracted data
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract metadata
        metadata = self._extract_metadata(soup)
        
        # Extract structured data BEFORE text extraction (which destroys scripts)
        structured_data = self._extract_structured_data(soup)
        
        # Extract links BEFORE text extraction
        links = self._extract_links(soup)
        
        # Extract headings BEFORE text extraction
        headings = self._extract_headings(soup)
        
        # Extract images
        images = self._extract_images(soup)
        
        # Extract text content (destroys unwanted elements)
        text_content = self._extract_text(soup)
        
        return ParsedPage(
            url=self.base_url,
            metadata=metadata,
            text_content=text_content,
            links=links,
            structured_data=structured_data,
            headings=headings,
            images=images
        )
    
    def _extract_metadata(self, soup: BeautifulSoup) -> PageMetadata:
        """Extract page metadata from head section."""
        metadata = PageMetadata()
        
        # Title
        title_tag = soup.find('title')
        if title_tag:
            metadata.title = title_tag.get_text(strip=True)
        
        # Meta tags
        for meta in soup.find_all('meta'):
            name = (meta.get('name') or meta.get('property') or '').lower()
            content = meta.get('content', '')
            
            if name == 'description':
                metadata.description = content
            elif name == 'keywords':
                metadata.keywords = [k.strip() for k in content.split(',')]
            elif name == 'author':
                metadata.author = content
            elif name == 'robots':
                metadata.robots = content
            elif name == 'og:title':
                metadata.og_title = content
            elif name == 'og:description':
                metadata.og_description = content
            elif name == 'og:image':
                metadata.og_image = content
            elif name == 'og:type':
                metadata.og_type = content
            elif name == 'twitter:card':
                metadata.twitter_card = content
        
        # Canonical URL
        canonical = soup.find('link', rel='canonical')
        if canonical:
            metadata.canonical_url = canonical.get('href')
        
        # Language
        html_tag = soup.find('html')
        if html_tag:
            metadata.language = html_tag.get('lang')
        
        return metadata
    
    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract clean text content from page."""
        # Remove unwanted elements
        for tag in soup.find_all(self.IGNORE_TAGS):
            tag.decompose()
        
        # Remove comments
        for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
            comment.extract()
        
        # Get text
        text = soup.get_text(separator=' ', strip=True)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def _extract_links(self, soup: BeautifulSoup) -> list[ExtractedLink]:
        """Extract all links from page."""
        links = []
        
        # Anchor links
        for anchor in soup.find_all('a', href=True):
            href = anchor.get('href', '')
            
            # Skip javascript and mailto links
            if href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                continue
            
            # Normalize URL
            absolute_url = urljoin(self.base_url, href)
            
            # Check if internal
            link_domain = urlparse(absolute_url).netloc.lower()
            is_internal = link_domain == self.base_domain
            
            # Check nofollow
            rel = anchor.get('rel', [])
            if isinstance(rel, str):
                rel = rel.split()
            nofollow = 'nofollow' in rel
            
            # Get link text
            text = anchor.get_text(strip=True)
            
            links.append(ExtractedLink(
                url=absolute_url,
                text=text[:200],  # Limit text length
                is_internal=is_internal,
                nofollow=nofollow,
                link_type='anchor'
            ))
        
        # Frame/iframe sources
        for frame in soup.find_all(['frame', 'iframe'], src=True):
            src = frame.get('src', '')
            if src and not src.startswith('javascript:'):
                absolute_url = urljoin(self.base_url, src)
                link_domain = urlparse(absolute_url).netloc.lower()
                
                links.append(ExtractedLink(
                    url=absolute_url,
                    text='',
                    is_internal=link_domain == self.base_domain,
                    nofollow=False,
                    link_type='frame'
                ))
        
        return links
    
    def _extract_structured_data(self, soup: BeautifulSoup) -> list[dict]:
        """Extract JSON-LD and other structured data."""
        structured_data = []
        
        # JSON-LD
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    structured_data.extend(data)
                else:
                    structured_data.append(data)
            except (json.JSONDecodeError, TypeError):
                pass
        
        return structured_data
    
    def _extract_headings(self, soup: BeautifulSoup) -> dict[str, list[str]]:
        """Extract heading structure."""
        headings = {f'h{i}': [] for i in range(1, 7)}
        
        for i in range(1, 7):
            for heading in soup.find_all(f'h{i}'):
                text = heading.get_text(strip=True)
                if text:
                    headings[f'h{i}'].append(text[:200])
        
        return headings
    
    def _extract_images(self, soup: BeautifulSoup) -> list[dict]:
        """Extract image information."""
        images = []
        
        for img in soup.find_all('img'):
            src = img.get('src') or img.get('data-src')
            if not src:
                continue
            
            images.append({
                'src': urljoin(self.base_url, src),
                'alt': img.get('alt', ''),
                'title': img.get('title', ''),
            })
        
        return images[:50]  # Limit to 50 images
    
    def get_crawlable_links(
        self,
        parsed: ParsedPage,
        respect_nofollow: bool = True,
        internal_only: bool = True
    ) -> list[str]:
        """
        Get list of URLs to crawl from parsed page.
        
        Args:
            parsed: ParsedPage object
            respect_nofollow: Skip nofollow links
            internal_only: Only return internal links
        
        Returns:
            List of URLs to crawl
        """
        # Check robots meta
        if parsed.metadata.robots:
            robots = parsed.metadata.robots.lower()
            if 'nofollow' in robots:
                return []  # Don't follow any links
        
        urls = []
        seen = set()
        
        for link in parsed.links:
            # Skip if configured
            if respect_nofollow and link.nofollow:
                continue
            if internal_only and not link.is_internal:
                continue
            
            # Deduplicate
            if link.url in seen:
                continue
            seen.add(link.url)
            
            urls.append(link.url)
        
        return urls
