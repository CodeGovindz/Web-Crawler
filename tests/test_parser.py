"""
Tests for HTML parser.
"""

import pytest

from crawler.parser import HTMLParser, ParsedPage


class TestHTMLParser:
    """Tests for HTML parsing."""
    
    @pytest.fixture
    def parser(self):
        return HTMLParser("https://example.com/page")
    
    def test_extract_title(self, parser):
        """Test title extraction."""
        html = "<html><head><title>Test Title</title></head><body></body></html>"
        parsed = parser.parse(html)
        
        assert parsed.metadata.title == "Test Title"
    
    def test_extract_meta_description(self, parser):
        """Test meta description extraction."""
        html = '''
        <html>
        <head>
            <meta name="description" content="Test description">
        </head>
        <body></body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert parsed.metadata.description == "Test description"
    
    def test_extract_og_tags(self, parser):
        """Test Open Graph tag extraction."""
        html = '''
        <html>
        <head>
            <meta property="og:title" content="OG Title">
            <meta property="og:description" content="OG Description">
            <meta property="og:image" content="https://example.com/image.jpg">
        </head>
        <body></body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert parsed.metadata.og_title == "OG Title"
        assert parsed.metadata.og_description == "OG Description"
        assert parsed.metadata.og_image == "https://example.com/image.jpg"
    
    def test_extract_canonical(self, parser):
        """Test canonical URL extraction."""
        html = '''
        <html>
        <head>
            <link rel="canonical" href="https://example.com/canonical">
        </head>
        <body></body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert parsed.metadata.canonical_url == "https://example.com/canonical"
    
    def test_extract_links(self, parser):
        """Test link extraction."""
        html = '''
        <html>
        <body>
            <a href="/internal">Internal Link</a>
            <a href="https://other.com/external">External Link</a>
            <a href="/nofollow" rel="nofollow">Nofollow Link</a>
        </body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert len(parsed.links) == 3
        
        internal = next(l for l in parsed.links if "internal" in l.url)
        assert internal.is_internal is True
        assert internal.nofollow is False
        
        external = next(l for l in parsed.links if "external" in l.url)
        assert external.is_internal is False
        
        nofollow = next(l for l in parsed.links if "nofollow" in l.url)
        assert nofollow.nofollow is True
    
    def test_skip_javascript_links(self, parser):
        """Test JavaScript link skipping."""
        html = '''
        <html>
        <body>
            <a href="javascript:void(0)">JS Link</a>
            <a href="/real">Real Link</a>
        </body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert len(parsed.links) == 1
        assert "real" in parsed.links[0].url
    
    def test_extract_text(self, parser):
        """Test text content extraction."""
        html = '''
        <html>
        <body>
            <h1>Main Title</h1>
            <p>This is paragraph text.</p>
            <script>var x = 1;</script>
            <div>More content</div>
        </body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert "Main Title" in parsed.text_content
        assert "paragraph text" in parsed.text_content
        assert "var x" not in parsed.text_content  # Script should be removed
    
    def test_extract_headings(self, parser):
        """Test heading extraction."""
        html = '''
        <html>
        <body>
            <h1>H1 Title</h1>
            <h2>H2 Subtitle</h2>
            <h3>H3 Section</h3>
        </body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert "H1 Title" in parsed.headings['h1']
        assert "H2 Subtitle" in parsed.headings['h2']
        assert "H3 Section" in parsed.headings['h3']
    
    def test_extract_json_ld(self, parser):
        """Test JSON-LD extraction."""
        html = '''
        <html>
        <head>
            <script type="application/ld+json">
            {"@type": "Organization", "name": "Example Corp"}
            </script>
        </head>
        <body></body>
        </html>
        '''
        parsed = parser.parse(html)
        
        assert len(parsed.structured_data) == 1
        assert parsed.structured_data[0]['name'] == "Example Corp"
    
    def test_get_crawlable_links(self, parser):
        """Test crawlable links extraction."""
        html = '''
        <html>
        <body>
            <a href="/page1">Page 1</a>
            <a href="/page2" rel="nofollow">Page 2</a>
            <a href="https://other.com/page3">Page 3</a>
        </body>
        </html>
        '''
        parsed = parser.parse(html)
        
        # Respect nofollow, internal only
        links = parser.get_crawlable_links(parsed, respect_nofollow=True, internal_only=True)
        
        assert len(links) == 1
        assert "page1" in links[0]
    
    def test_robots_nofollow(self, parser):
        """Test robots meta nofollow handling."""
        html = '''
        <html>
        <head>
            <meta name="robots" content="nofollow">
        </head>
        <body>
            <a href="/page1">Page 1</a>
        </body>
        </html>
        '''
        parsed = parser.parse(html)
        
        links = parser.get_crawlable_links(parsed)
        assert len(links) == 0
