"""
Tests for URL Frontier functionality.
"""

import asyncio
import pytest

from crawler.frontier import (
    URLFrontier, URLItem, Priority, 
    normalize_url, get_domain, BloomFilter
)


class TestNormalizeURL:
    """Tests for URL normalization."""
    
    def test_basic_normalization(self):
        """Test basic URL normalization."""
        url = normalize_url("https://Example.COM/path")
        assert url == "https://example.com/path"
    
    def test_removes_fragment(self):
        """Test fragment removal."""
        url = normalize_url("https://example.com/page#section")
        assert url == "https://example.com/page"
    
    def test_removes_trailing_slash(self):
        """Test trailing slash removal."""
        url = normalize_url("https://example.com/path/")
        assert url == "https://example.com/path"
    
    def test_keeps_root_slash(self):
        """Test root path keeps slash."""
        url = normalize_url("https://example.com/")
        assert url == "https://example.com/"
    
    def test_resolves_relative(self):
        """Test relative URL resolution."""
        url = normalize_url("/page", "https://example.com/base")
        assert url == "https://example.com/page"
    
    def test_removes_default_port(self):
        """Test default port removal."""
        url = normalize_url("https://example.com:443/path")
        assert url == "https://example.com/path"
        
        url = normalize_url("http://example.com:80/path")
        assert url == "http://example.com/path"
    
    def test_sorts_query_params(self):
        """Test query parameter sorting."""
        url = normalize_url("https://example.com/page?z=1&a=2")
        assert url == "https://example.com/page?a=2&z=1"
    
    def test_rejects_non_http(self):
        """Test rejection of non-HTTP URLs."""
        assert normalize_url("ftp://example.com") is None
        assert normalize_url("javascript:void(0)") is None
        assert normalize_url("mailto:test@example.com") is None


class TestGetDomain:
    """Tests for domain extraction."""
    
    def test_basic_domain(self):
        """Test basic domain extraction."""
        assert get_domain("https://example.com/path") == "example.com"
    
    def test_subdomain(self):
        """Test subdomain handling."""
        assert get_domain("https://www.example.com/path") == "www.example.com"
    
    def test_port(self):
        """Test domain with port."""
        assert get_domain("https://example.com:8080/path") == "example.com:8080"


class TestBloomFilter:
    """Tests for bloom filter."""
    
    def test_add_and_check(self):
        """Test adding and checking items."""
        bf = BloomFilter(size=1000)
        
        bf.add("test1")
        assert "test1" in bf
        assert "test2" not in bf
    
    def test_count(self):
        """Test item count."""
        bf = BloomFilter(size=1000)
        
        bf.add("item1")
        bf.add("item2")
        assert len(bf) == 2


class TestURLFrontier:
    """Tests for URL frontier."""
    
    @pytest.fixture
    def frontier(self):
        return URLFrontier()
    
    @pytest.mark.asyncio
    async def test_add_url(self, frontier):
        """Test adding a URL."""
        added = await frontier.add("https://example.com/page1")
        assert added is True
        assert frontier.size == 1
    
    @pytest.mark.asyncio
    async def test_add_duplicate(self, frontier):
        """Test duplicate URL handling."""
        await frontier.add("https://example.com/page1")
        added = await frontier.add("https://example.com/page1")
        assert added is False
        assert frontier.size == 1
    
    @pytest.mark.asyncio
    async def test_priority_ordering(self, frontier):
        """Test priority queue ordering."""
        await frontier.add("https://example.com/low", Priority.LOW)
        await frontier.add("https://example.com/high", Priority.HIGH)
        await frontier.add("https://example.com/highest", Priority.HIGHEST)
        
        item1 = await frontier.get()
        item2 = await frontier.get()
        item3 = await frontier.get()
        
        assert "highest" in item1.url
        assert "high" in item2.url
        assert "low" in item3.url
    
    @pytest.mark.asyncio
    async def test_get_timeout(self, frontier):
        """Test get with timeout on empty queue."""
        item = await frontier.get(timeout=0.1)
        assert item is None
    
    @pytest.mark.asyncio
    async def test_complete(self, frontier):
        """Test completing a URL."""
        await frontier.add("https://example.com/page")
        item = await frontier.get()
        
        assert frontier.in_progress_count == 1
        
        await frontier.complete(item.url, success=True)
        
        assert frontier.in_progress_count == 0
        assert frontier.completed_count == 1
    
    @pytest.mark.asyncio
    async def test_stats(self, frontier):
        """Test getting stats."""
        await frontier.add("https://example.com/page1")
        await frontier.add("https://example.com/page2")
        
        item = await frontier.get()
        await frontier.complete(item.url)
        
        stats = await frontier.get_stats()
        
        assert stats['queued'] == 1
        assert stats['completed'] == 1
        assert stats['seen'] == 2
