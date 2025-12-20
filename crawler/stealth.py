"""
Stealth module - Anti-detection and human-like behavior simulation.

Features:
- User-Agent rotation
- Request header randomization
- Human-like timing jitter
- Proxy rotation support
"""

import random
import time
import warnings
from typing import Optional

# Suppress fake_useragent warnings
warnings.filterwarnings('ignore', message='.*Error occurred during getting browser.*')

# Static user agents (no network dependency)
STATIC_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]


class StealthManager:
    """Manages anti-detection techniques."""
    
    # Common browser headers
    ACCEPT_HEADERS = [
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    ]
    
    ACCEPT_LANGUAGE = [
        "en-US,en;q=0.9",
        "en-GB,en;q=0.9,en-US;q=0.8",
        "en-US,en;q=0.9,es;q=0.8",
        "en,en-US;q=0.9",
    ]
    
    ACCEPT_ENCODING = "gzip, deflate, br"
    
    SEC_FETCH_MODES = ["navigate", "same-origin", "cors"]
    SEC_FETCH_SITES = ["none", "same-origin", "same-site", "cross-site"]
    SEC_FETCH_DESTS = ["document", "empty"]
    
    def __init__(
        self,
        custom_user_agent: Optional[str] = None,
        rotate_agents: bool = True,
        proxy_list: Optional[list[str]] = None
    ):
        self.custom_user_agent = custom_user_agent
        self.rotate_agents = rotate_agents
        self.proxy_list = proxy_list or []
        self._proxy_index = 0
        self._last_user_agent: Optional[str] = None
    
    def get_user_agent(self) -> str:
        """Get a User-Agent string."""
        if self.custom_user_agent:
            return self.custom_user_agent
        
        if self.rotate_agents:
            return random.choice(STATIC_USER_AGENTS)
        
        # Default Chrome UA
        return STATIC_USER_AGENTS[0]
    
    def get_headers(self, referer: Optional[str] = None) -> dict[str, str]:
        """
        Generate randomized but realistic HTTP headers.
        
        Args:
            referer: Optional referer URL
        
        Returns:
            Dictionary of HTTP headers
        """
        user_agent = self.get_user_agent()
        self._last_user_agent = user_agent
        
        headers = {
            "User-Agent": user_agent,
            "Accept": random.choice(self.ACCEPT_HEADERS),
            "Accept-Language": random.choice(self.ACCEPT_LANGUAGE),
            "Accept-Encoding": self.ACCEPT_ENCODING,
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
        
        # Add referer if provided
        if referer:
            headers["Referer"] = referer
        
        # Add Sec-Fetch headers (modern browsers)
        if "Chrome" in user_agent or "Edge" in user_agent:
            headers.update({
                "Sec-Fetch-Mode": random.choice(self.SEC_FETCH_MODES),
                "Sec-Fetch-Site": random.choice(self.SEC_FETCH_SITES),
                "Sec-Fetch-Dest": random.choice(self.SEC_FETCH_DESTS),
                "Sec-Fetch-User": "?1",
                "Sec-Ch-Ua": self._get_sec_ch_ua(user_agent),
                "Sec-Ch-Ua-Mobile": "?0",
                "Sec-Ch-Ua-Platform": '"Windows"',
            })
        
        # Randomly add DNT header
        if random.random() < 0.3:
            headers["DNT"] = "1"
        
        return headers
    
    def _get_sec_ch_ua(self, user_agent: str) -> str:
        """Generate Sec-Ch-Ua header based on User-Agent."""
        if "Chrome/120" in user_agent:
            return '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
        elif "Chrome/119" in user_agent:
            return '"Not_A Brand";v="8", "Chromium";v="119", "Google Chrome";v="119"'
        elif "Edge" in user_agent:
            return '"Not_A Brand";v="8", "Chromium";v="120", "Microsoft Edge";v="120"'
        else:
            return '"Not_A Brand";v="8", "Chromium";v="120"'
    
    def get_delay(self, min_delay: float = 0.5, max_delay: float = 2.0) -> float:
        """
        Get a human-like random delay.
        
        Uses a distribution that favors shorter delays but occasionally
        includes longer pauses (mimicking human reading behavior).
        """
        # Use exponential distribution for more natural timing
        base_delay = random.expovariate(1 / ((min_delay + max_delay) / 2))
        
        # Clamp to range
        delay = max(min_delay, min(max_delay, base_delay))
        
        # Occasionally add "reading time" (simulating user pausing to read)
        if random.random() < 0.1:  # 10% chance
            delay += random.uniform(1.0, 3.0)
        
        return delay
    
    def get_proxy(self) -> Optional[str]:
        """Get next proxy from rotation."""
        if not self.proxy_list:
            return None
        
        proxy = self.proxy_list[self._proxy_index]
        self._proxy_index = (self._proxy_index + 1) % len(self.proxy_list)
        return proxy
    
    def get_playwright_stealth_config(self) -> dict:
        """
        Get Playwright browser context options for stealth.
        
        Returns configuration to make headless browser less detectable.
        """
        return {
            "user_agent": self.get_user_agent(),
            "viewport": {"width": random.choice([1920, 1366, 1536, 1440]),
                        "height": random.choice([1080, 768, 864, 900])},
            "locale": random.choice(["en-US", "en-GB", "en"]),
            "timezone_id": random.choice([
                "America/New_York",
                "America/Los_Angeles", 
                "Europe/London",
                "Asia/Tokyo"
            ]),
            "color_scheme": random.choice(["light", "dark"]),
            "device_scale_factor": random.choice([1, 1.25, 1.5, 2]),
            "is_mobile": False,
            "has_touch": False,
            "java_script_enabled": True,
            "extra_http_headers": {
                "Accept-Language": random.choice(self.ACCEPT_LANGUAGE)
            }
        }
    
    async def simulate_human_behavior(self, page) -> None:
        """
        Simulate human-like behavior on a Playwright page.
        
        Scrolls, moves mouse, and waits like a human would.
        """
        try:
            # Random scroll
            scroll_amount = random.randint(100, 500)
            await page.evaluate(f"window.scrollBy(0, {scroll_amount})")
            await page.wait_for_timeout(random.randint(200, 500))
            
            # Random mouse movement
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            await page.mouse.move(x, y)
            
            # Brief pause
            await page.wait_for_timeout(random.randint(100, 300))
            
        except Exception:
            pass  # Ignore errors in human simulation
