"""
Browser Renderer - Playwright-based JavaScript rendering.

Features:
- Headless browser for JavaScript-heavy sites
- Stealth mode to evade detection
- Resource blocking for performance
- Screenshot and DOM snapshot
- Cookie and session management
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

from playwright.async_api import async_playwright, Browser, Page, BrowserContext

from .config import CrawlerConfig
from .stealth import StealthManager


@dataclass
class RenderResult:
    """Result from rendering a page."""
    url: str
    html: str
    title: Optional[str]
    status: int
    screenshot: Optional[bytes] = None
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return self.error is None and self.html is not None


class BrowserRenderer:
    """
    Playwright-based browser for JavaScript rendering.
    """
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.stealth = StealthManager(
            custom_user_agent=config.user_agent,
            rotate_agents=config.rotate_user_agents
        )
        
        self._playwright = None
        self._browser: Optional[Browser] = None
        self._context: Optional[BrowserContext] = None
    
    async def start(self) -> None:
        """Initialize the browser."""
        self._playwright = await async_playwright().start()
        
        # Launch browser with stealth options
        self._browser = await self._playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--window-position=0,0',
                '--ignore-certificate-errors',
                '--ignore-certificate-errors-spki-list',
            ]
        )
        
        # Create stealth context
        stealth_config = self.stealth.get_playwright_stealth_config()
        self._context = await self._browser.new_context(**stealth_config)
        
        # Add stealth scripts
        await self._context.add_init_script("""
            // Override navigator.webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override chrome
            window.chrome = {
                runtime: {}
            };
            
            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
        """)
    
    async def stop(self) -> None:
        """Close the browser."""
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None
    
    async def render(
        self,
        url: str,
        wait_for_selector: Optional[str] = None,
        take_screenshot: bool = False
    ) -> RenderResult:
        """
        Render a page with JavaScript execution.
        
        Args:
            url: URL to render
            wait_for_selector: CSS selector to wait for
            take_screenshot: Whether to capture a screenshot
        
        Returns:
            RenderResult with rendered HTML
        """
        if not self._context:
            await self.start()
        
        page: Optional[Page] = None
        
        try:
            page = await self._context.new_page()
            
            # Block unnecessary resources for faster loading
            if self.config.block_resources:
                await page.route(
                    "**/*",
                    lambda route: self._handle_route(route)
                )
            
            # Navigate to page
            response = await page.goto(
                url,
                wait_until='networkidle',
                timeout=int(self.config.render_timeout * 1000)
            )
            
            status = response.status if response else 0
            
            # Wait for additional time or selector
            if wait_for_selector:
                try:
                    await page.wait_for_selector(
                        wait_for_selector,
                        timeout=5000
                    )
                except Exception:
                    pass  # Continue even if selector not found
            else:
                await page.wait_for_timeout(int(self.config.render_wait_time * 1000))
            
            # Simulate human behavior
            await self.stealth.simulate_human_behavior(page)
            
            # Get page content
            html = await page.content()
            title = await page.title()
            
            # Take screenshot if requested
            screenshot = None
            if take_screenshot:
                screenshot = await page.screenshot(full_page=True)
            
            return RenderResult(
                url=url,
                html=html,
                title=title,
                status=status,
                screenshot=screenshot
            )
        
        except asyncio.TimeoutError:
            return RenderResult(
                url=url,
                html='',
                title=None,
                status=0,
                error='Render timeout'
            )
        except Exception as e:
            return RenderResult(
                url=url,
                html='',
                title=None,
                status=0,
                error=str(e)
            )
        finally:
            if page:
                await page.close()
    
    async def _handle_route(self, route) -> None:
        """Handle route blocking for performance."""
        request = route.request
        resource_type = request.resource_type
        
        if resource_type in self.config.block_resources:
            await route.abort()
        else:
            await route.continue_()
    
    async def render_and_interact(
        self,
        url: str,
        interactions: list[dict]
    ) -> RenderResult:
        """
        Render page and perform interactions.
        
        Args:
            url: URL to render
            interactions: List of interactions like:
                [{'type': 'click', 'selector': '.button'},
                 {'type': 'fill', 'selector': 'input', 'value': 'text'}]
        
        Returns:
            RenderResult after interactions
        """
        if not self._context:
            await self.start()
        
        page: Optional[Page] = None
        
        try:
            page = await self._context.new_page()
            
            response = await page.goto(
                url,
                wait_until='networkidle',
                timeout=int(self.config.render_timeout * 1000)
            )
            
            status = response.status if response else 0
            
            # Perform interactions
            for interaction in interactions:
                interaction_type = interaction.get('type')
                selector = interaction.get('selector')
                
                if interaction_type == 'click':
                    await page.click(selector)
                elif interaction_type == 'fill':
                    value = interaction.get('value', '')
                    await page.fill(selector, value)
                elif interaction_type == 'wait':
                    timeout = interaction.get('timeout', 1000)
                    await page.wait_for_timeout(timeout)
                elif interaction_type == 'scroll':
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                
                # Brief wait between interactions
                await page.wait_for_timeout(500)
            
            # Wait for network to settle
            await page.wait_for_load_state('networkidle')
            
            html = await page.content()
            title = await page.title()
            
            return RenderResult(
                url=url,
                html=html,
                title=title,
                status=status
            )
        
        except Exception as e:
            return RenderResult(
                url=url,
                html='',
                title=None,
                status=0,
                error=str(e)
            )
        finally:
            if page:
                await page.close()


async def create_renderer(config: CrawlerConfig) -> BrowserRenderer:
    """Create and initialize a browser renderer."""
    renderer = BrowserRenderer(config)
    await renderer.start()
    return renderer
