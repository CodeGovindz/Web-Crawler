"""
Tests for robots.txt parser.
"""

import pytest

from crawler.robots import RobotsParser, RobotsRules


class TestRobotsParser:
    """Tests for robots.txt parsing."""
    
    @pytest.fixture
    def parser(self):
        return RobotsParser(user_agent="*")
    
    def test_basic_disallow(self, parser):
        """Test basic disallow rule."""
        content = """
User-agent: *
Disallow: /private
"""
        rules = parser.parse(content, "https://example.com/")
        
        assert parser.is_allowed(rules, "/public") is True
        assert parser.is_allowed(rules, "/private") is False
        assert parser.is_allowed(rules, "/private/page") is False
    
    def test_allow_override(self, parser):
        """Test allow overriding disallow."""
        content = """
User-agent: *
Disallow: /private
Allow: /private/public
"""
        rules = parser.parse(content, "https://example.com/")
        
        assert parser.is_allowed(rules, "/private") is False
        assert parser.is_allowed(rules, "/private/public") is True
    
    def test_crawl_delay(self, parser):
        """Test crawl-delay extraction."""
        content = """
User-agent: *
Crawl-delay: 5
"""
        rules = parser.parse(content, "https://example.com/")
        assert rules.crawl_delay == 5.0
    
    def test_sitemap_extraction(self, parser):
        """Test sitemap URL extraction."""
        content = """
User-agent: *
Disallow:

Sitemap: https://example.com/sitemap.xml
Sitemap: https://example.com/sitemap2.xml
"""
        rules = parser.parse(content, "https://example.com/")
        
        assert len(rules.sitemaps) == 2
        assert "https://example.com/sitemap.xml" in rules.sitemaps
    
    def test_wildcard_pattern(self, parser):
        """Test wildcard pattern matching."""
        content = """
User-agent: *
Disallow: /*.pdf
"""
        rules = parser.parse(content, "https://example.com/")
        
        assert parser.is_allowed(rules, "/document.pdf") is False
        assert parser.is_allowed(rules, "/path/file.pdf") is False
        assert parser.is_allowed(rules, "/document.html") is True
    
    def test_end_anchor(self, parser):
        """Test $ end anchor."""
        content = """
User-agent: *
Disallow: /exact$
"""
        rules = parser.parse(content, "https://example.com/")
        
        assert parser.is_allowed(rules, "/exact") is False
        assert parser.is_allowed(rules, "/exact/more") is True
    
    def test_empty_disallow(self, parser):
        """Test empty disallow (allow all)."""
        content = """
User-agent: *
Disallow:
"""
        rules = parser.parse(content, "https://example.com/")
        
        assert parser.is_allowed(rules, "/anything") is True
    
    def test_user_agent_matching(self):
        """Test specific user-agent matching."""
        content = """
User-agent: googlebot
Disallow: /nogoogle

User-agent: *
Disallow: /private
"""
        # Test with googlebot
        parser_google = RobotsParser(user_agent="googlebot")
        rules = parser_google.parse(content, "https://example.com/")
        
        assert parser_google.is_allowed(rules, "/nogoogle") is False
        
        # Test with generic agent - should use * rules
        parser_generic = RobotsParser(user_agent="mybot")
        rules = parser_generic.parse(content, "https://example.com/")
        
        assert parser_generic.is_allowed(rules, "/private") is False


class TestRobotsRules:
    """Tests for RobotsRules dataclass."""
    
    def test_default_values(self):
        """Test default values."""
        rules = RobotsRules(user_agent="*")
        
        assert rules.allowed == []
        assert rules.disallowed == []
        assert rules.crawl_delay is None
        assert rules.sitemaps == []
