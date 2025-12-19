"""
Configuration management using Pydantic settings.

Provides centralized configuration for all crawler components.
"""

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class CrawlerConfig(BaseSettings):
    """Main configuration for the web crawler."""
    
    # Crawl limits
    max_pages: int = Field(default=1000, description="Maximum pages to crawl")
    max_depth: int = Field(default=10, description="Maximum crawl depth from seed URL")
    max_retries: int = Field(default=3, description="Maximum retry attempts per URL")
    
    # Rate limiting
    requests_per_second: float = Field(default=2.0, description="Max requests per second per domain")
    concurrent_requests: int = Field(default=10, description="Max concurrent requests")
    delay_min: float = Field(default=0.5, description="Minimum delay between requests (seconds)")
    delay_max: float = Field(default=2.0, description="Maximum delay between requests (seconds)")
    
    # Timeouts (in seconds)
    request_timeout: float = Field(default=30.0, description="HTTP request timeout")
    render_timeout: float = Field(default=60.0, description="Page render timeout for JS sites")
    
    # Content settings
    max_content_length: int = Field(default=10 * 1024 * 1024, description="Max content size (10MB)")
    allowed_content_types: list[str] = Field(
        default=["text/html", "application/xhtml+xml"],
        description="Content types to process"
    )
    
    # Rendering
    enable_rendering: bool = Field(default=False, description="Enable JavaScript rendering")
    render_wait_time: float = Field(default=2.0, description="Wait time after page load")
    block_resources: list[str] = Field(
        default=["image", "media", "font", "stylesheet"],
        description="Resource types to block during rendering"
    )
    
    # Compliance
    respect_robots_txt: bool = Field(default=True, description="Respect robots.txt rules")
    respect_nofollow: bool = Field(default=True, description="Respect nofollow attributes")
    parse_sitemaps: bool = Field(default=True, description="Parse and use sitemaps")
    
    # User-Agent
    user_agent: Optional[str] = Field(
        default=None,
        description="Custom User-Agent (None = rotate fake agents)"
    )
    rotate_user_agents: bool = Field(default=True, description="Rotate User-Agents")
    
    # Proxy settings
    proxy_url: Optional[str] = Field(default=None, description="Proxy URL")
    proxy_rotation: bool = Field(default=False, description="Enable proxy rotation")
    proxy_list: list[str] = Field(default=[], description="List of proxy URLs for rotation")
    
    # Output settings
    output_dir: Path = Field(default=Path("./data"), description="Output directory")
    save_html: bool = Field(default=True, description="Save raw HTML content")
    save_text: bool = Field(default=True, description="Save extracted text")
    save_links: bool = Field(default=True, description="Save discovered links")
    
    # Database
    db_path: Path = Field(default=Path("./data/crawler.db"), description="SQLite database path")
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_file: Optional[Path] = Field(default=None, description="Log file path")
    
    class Config:
        env_prefix = "CRAWLER_"
        env_file = ".env"


class DomainConfig:
    """Per-domain configuration tracking."""
    
    def __init__(self, domain: str):
        self.domain = domain
        self.crawl_delay: float = 1.0
        self.last_request_time: float = 0.0
        self.request_count: int = 0
        self.error_count: int = 0
        self.robots_rules: Optional[dict] = None
        self.sitemap_urls: list[str] = []
    
    def update_crawl_delay(self, delay: float) -> None:
        """Update crawl delay from robots.txt."""
        self.crawl_delay = max(delay, 0.5)  # Minimum 0.5s
    
    def record_request(self, timestamp: float) -> None:
        """Record a request timestamp."""
        self.last_request_time = timestamp
        self.request_count += 1
    
    def record_error(self) -> None:
        """Record an error for this domain."""
        self.error_count += 1


def get_config(**overrides) -> CrawlerConfig:
    """Get configuration with optional overrides."""
    return CrawlerConfig(**overrides)
