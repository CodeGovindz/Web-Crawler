"""
robots.txt parser - RFC 9309 compliant.

Features:
- Parses robots.txt files
- Checks URL access permissions
- Extracts crawl-delay
- Extracts sitemap URLs
"""

import asyncio
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urljoin, urlparse


@dataclass
class RobotsRules:
    """Parsed robots.txt rules for a specific user-agent."""
    user_agent: str
    allowed: list[str] = field(default_factory=list)
    disallowed: list[str] = field(default_factory=list)
    crawl_delay: Optional[float] = None
    sitemaps: list[str] = field(default_factory=list)


class RobotsParser:
    """
    RFC 9309 compliant robots.txt parser.
    
    Caches parsed rules per domain.
    """
    
    def __init__(self, user_agent: str = "*"):
        self.user_agent = user_agent
        self._cache: dict[str, RobotsRules] = {}
        self._lock = asyncio.Lock()
    
    def parse(self, content: str, base_url: str) -> RobotsRules:
        """
        Parse robots.txt content.
        
        Args:
            content: robots.txt file content
            base_url: Base URL for resolving sitemap URLs
        
        Returns:
            RobotsRules object with parsed rules
        """
        rules = RobotsRules(user_agent=self.user_agent)
        current_agents: list[str] = []
        applies_to_us = False
        
        # Track sitemaps (global, not per user-agent)
        sitemaps: list[str] = []
        
        for line in content.split('\n'):
            # Remove comments and strip whitespace
            line = line.split('#')[0].strip()
            if not line:
                continue
            
            # Parse directive
            if ':' not in line:
                continue
            
            directive, value = line.split(':', 1)
            directive = directive.strip().lower()
            value = value.strip()
            
            if directive == 'user-agent':
                # New user-agent block
                if current_agents and not applies_to_us:
                    current_agents = []
                
                current_agents.append(value.lower())
                applies_to_us = self._matches_user_agent(value)
            
            elif directive == 'disallow' and applies_to_us:
                if value:
                    rules.disallowed.append(value)
            
            elif directive == 'allow' and applies_to_us:
                if value:
                    rules.allowed.append(value)
            
            elif directive == 'crawl-delay' and applies_to_us:
                try:
                    rules.crawl_delay = float(value)
                except ValueError:
                    pass
            
            elif directive == 'sitemap':
                # Sitemap URLs are global
                sitemap_url = urljoin(base_url, value)
                sitemaps.append(sitemap_url)
        
        rules.sitemaps = sitemaps
        return rules
    
    def _matches_user_agent(self, pattern: str) -> bool:
        """Check if a user-agent pattern matches our agent."""
        pattern = pattern.lower()
        agent = self.user_agent.lower()
        
        if pattern == '*':
            return True
        
        # Check if pattern is a prefix of our agent
        return agent.startswith(pattern) or pattern in agent
    
    def is_allowed(self, rules: RobotsRules, path: str) -> bool:
        """
        Check if a path is allowed according to rules.
        
        Uses the most specific matching rule (longest match wins).
        """
        # Empty rules = everything allowed
        if not rules.allowed and not rules.disallowed:
            return True
        
        # Find matching rules
        allow_match = -1
        disallow_match = -1
        
        for pattern in rules.allowed:
            if self._path_matches(path, pattern):
                allow_match = max(allow_match, len(pattern))
        
        for pattern in rules.disallowed:
            if self._path_matches(path, pattern):
                disallow_match = max(disallow_match, len(pattern))
        
        # Longest match wins; allow wins ties
        if allow_match >= disallow_match:
            return True
        return disallow_match == -1
    
    def _path_matches(self, path: str, pattern: str) -> bool:
        """
        Check if a path matches a robots.txt pattern.
        
        Supports * and $ wildcards.
        """
        # Convert pattern to regex
        regex = ''
        i = 0
        while i < len(pattern):
            char = pattern[i]
            if char == '*':
                regex += '.*'
            elif char == '$' and i == len(pattern) - 1:
                regex += '$'
            else:
                regex += re.escape(char)
            i += 1
        
        try:
            return bool(re.match(regex, path))
        except re.error:
            return False
    
    async def can_fetch(self, url: str, rules: RobotsRules) -> bool:
        """
        Check if a URL can be fetched according to cached rules.
        
        Args:
            url: Full URL to check
            rules: Pre-fetched rules for the domain
        
        Returns:
            True if allowed, False otherwise
        """
        parsed = urlparse(url)
        path = parsed.path or '/'
        if parsed.query:
            path += '?' + parsed.query
        
        return self.is_allowed(rules, path)
    
    def get_crawl_delay(self, rules: RobotsRules) -> Optional[float]:
        """Get crawl-delay from rules."""
        return rules.crawl_delay
    
    def get_sitemaps(self, rules: RobotsRules) -> list[str]:
        """Get sitemap URLs from rules."""
        return rules.sitemaps


class RobotsManager:
    """
    Manages robots.txt fetching, parsing, and caching per domain.
    """
    
    def __init__(self, user_agent: str = "*"):
        self.parser = RobotsParser(user_agent)
        self._cache: dict[str, RobotsRules] = {}
        self._lock = asyncio.Lock()
    
    async def get_rules(
        self, 
        domain: str, 
        fetch_func
    ) -> RobotsRules:
        """
        Get robots.txt rules for a domain.
        
        Args:
            domain: Domain to get rules for
            fetch_func: Async function to fetch URL content
        
        Returns:
            RobotsRules for the domain
        """
        async with self._lock:
            if domain in self._cache:
                return self._cache[domain]
        
        # Construct robots.txt URL
        robots_url = f"https://{domain}/robots.txt"
        
        try:
            # Fetch robots.txt
            response = await fetch_func(robots_url)
            if response and response.get('status') == 200:
                content = response.get('text', '')
                rules = self.parser.parse(content, f"https://{domain}/")
            else:
                # No robots.txt = allow all
                rules = RobotsRules(user_agent=self.parser.user_agent)
        except Exception:
            # Error fetching = allow all
            rules = RobotsRules(user_agent=self.parser.user_agent)
        
        async with self._lock:
            self._cache[domain] = rules
        
        return rules
    
    def clear_cache(self) -> None:
        """Clear the robots.txt cache."""
        self._cache.clear()
