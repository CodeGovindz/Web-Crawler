"""
Web API for the Web Crawler - FastAPI backend with WebSocket support.

Provides:
- REST API for starting/stopping crawls
- WebSocket for real-time progress updates
- Crawl history and data viewing
"""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel

from crawler.config import CrawlerConfig
from crawler.crawler import Crawler
from crawler.storage import CrawlDatabase, ContentStorage


app = FastAPI(
    title="SOTA Web Crawler",
    description="State-of-the-art web crawling system with real-time monitoring",
    version="1.0.0"
)

# Global state
active_crawlers: dict[str, dict] = {}
websocket_connections: list[WebSocket] = []


class CrawlRequest(BaseModel):
    """Request to start a crawl."""
    url: str
    max_pages: int = 100
    max_depth: int = 10
    delay: float = 1.0
    render: bool = False
    respect_robots: bool = True
    concurrent: int = 5


class CrawlResponse(BaseModel):
    """Response from crawl operations."""
    crawl_id: str
    status: str
    message: str


# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass


manager = ConnectionManager()


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main UI."""
    ui_path = Path(__file__).parent / "ui" / "index.html"
    if ui_path.exists():
        return FileResponse(ui_path)
    return HTMLResponse("<h1>SOTA Crawler API</h1><p>UI not found. Use API endpoints.</p>")


@app.post("/api/crawl", response_model=CrawlResponse)
async def start_crawl(request: CrawlRequest):
    """Start a new crawl job."""
    crawl_id = f"crawl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Create config
    config = CrawlerConfig(
        max_pages=request.max_pages,
        max_depth=request.max_depth,
        delay_min=request.delay * 0.5,
        delay_max=request.delay * 1.5,
        enable_rendering=request.render,
        respect_robots_txt=request.respect_robots,
        concurrent_requests=request.concurrent,
        output_dir=Path("./data")
    )
    config.db_path = config.output_dir / "crawler.db"
    config.output_dir.mkdir(parents=True, exist_ok=True)
    
    # Start crawl in background
    crawler = Crawler(config)
    
    async def run_crawl():
        try:
            active_crawlers[crawl_id] = {
                "status": "running",
                "url": request.url,
                "started": datetime.now().isoformat(),
                "crawler": crawler
            }
            
            # Broadcast start
            await manager.broadcast({
                "type": "crawl_started",
                "crawl_id": crawl_id,
                "url": request.url
            })
            
            # Run crawl with progress updates
            await crawl_with_updates(crawler, request.url, crawl_id)
            
            active_crawlers[crawl_id]["status"] = "completed"
            
            # Broadcast completion
            await manager.broadcast({
                "type": "crawl_completed",
                "crawl_id": crawl_id,
                "stats": {
                    "pages_crawled": crawler.stats.pages_crawled,
                    "pages_failed": crawler.stats.pages_failed
                }
            })
        except Exception as e:
            active_crawlers[crawl_id]["status"] = "failed"
            active_crawlers[crawl_id]["error"] = str(e)
            await manager.broadcast({
                "type": "crawl_error",
                "crawl_id": crawl_id,
                "error": str(e)
            })
    
    asyncio.create_task(run_crawl())
    
    return CrawlResponse(
        crawl_id=crawl_id,
        status="started",
        message=f"Crawl started for {request.url}"
    )


async def crawl_with_updates(crawler: Crawler, url: str, crawl_id: str):
    """Run crawl and send progress updates via WebSocket."""
    # Initialize components
    await crawler._init_components()
    
    try:
        # Start session
        session_id = await crawler.storage.start_session(url)
        await crawler.frontier.add(url, depth=0)
        await crawler.storage.db.add_url(session_id, url, depth=0)
        
        crawler._running = True
        
        # Create workers
        workers = [
            asyncio.create_task(crawler._worker(i))
            for i in range(crawler.config.concurrent_requests)
        ]
        
        # Send progress updates
        while crawler._running:
            if all(w.done() for w in workers):
                break
            
            if crawler.stats.total >= crawler.config.max_pages:
                crawler._running = False
                break
            
            # Send progress
            await manager.broadcast({
                "type": "progress",
                "crawl_id": crawl_id,
                "stats": {
                    "pages_crawled": crawler.stats.pages_crawled,
                    "pages_failed": crawler.stats.pages_failed,
                    "pages_skipped": crawler.stats.pages_skipped,
                    "queue_size": crawler.frontier.size,
                    "in_progress": crawler.frontier.in_progress_count,
                    "urls_seen": crawler.frontier.seen_count
                }
            })
            
            await asyncio.sleep(0.5)
        
        # Cancel remaining workers
        for w in workers:
            if not w.done():
                w.cancel()
        
        await asyncio.gather(*workers, return_exceptions=True)
        
        # Update session
        await crawler.storage.db.update_session(
            session_id,
            status='completed',
            pages_crawled=crawler.stats.pages_crawled,
            pages_failed=crawler.stats.pages_failed
        )
    finally:
        await crawler._cleanup()


@app.get("/api/crawl/{crawl_id}")
async def get_crawl_status(crawl_id: str):
    """Get status of a specific crawl."""
    if crawl_id in active_crawlers:
        crawl = active_crawlers[crawl_id]
        return {
            "crawl_id": crawl_id,
            "status": crawl["status"],
            "url": crawl["url"],
            "started": crawl["started"],
            "error": crawl.get("error")
        }
    raise HTTPException(status_code=404, detail="Crawl not found")


@app.delete("/api/crawl/{crawl_id}")
async def stop_crawl(crawl_id: str):
    """Stop an active crawl."""
    if crawl_id in active_crawlers:
        crawl = active_crawlers[crawl_id]
        if crawl["status"] == "running" and "crawler" in crawl:
            crawl["crawler"]._running = False
            crawl["crawler"]._shutdown_event.set()
            return {"message": "Crawl stop requested"}
    raise HTTPException(status_code=404, detail="Crawl not found")


@app.get("/api/history")
async def get_crawl_history():
    """Get crawl history from database."""
    db_path = Path("./data/crawler.db")
    if not db_path.exists():
        return {"sessions": []}
    
    db = CrawlDatabase(db_path)
    db.connect()
    
    try:
        cursor = db._conn.execute("""
            SELECT id, seed_url, started_at, completed_at, status, pages_crawled, pages_failed
            FROM crawl_sessions ORDER BY id DESC LIMIT 20
        """)
        sessions = [dict(row) for row in cursor.fetchall()]
        return {"sessions": sessions}
    finally:
        db.close()


@app.get("/api/data/{session_id}")
async def get_crawl_data(session_id: int, limit: int = 100):
    """Get crawled data for a session."""
    content_file = Path(f"./data/content_{session_id}.jsonl")
    if not content_file.exists():
        raise HTTPException(status_code=404, detail="Data not found")
    
    records = []
    with open(content_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            if line.strip():
                try:
                    record = json.loads(line)
                    # Remove large HTML field for API response
                    if 'html' in record:
                        record['html_length'] = len(record['html'])
                        del record['html']
                    records.append(record)
                except json.JSONDecodeError:
                    pass
    
    return {"session_id": session_id, "records": records, "count": len(records)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time updates."""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive
            data = await websocket.receive_text()
            # Echo back any messages
            await websocket.send_json({"type": "pong", "data": data})
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# Mount static files for UI
ui_dir = Path(__file__).parent / "ui"
if ui_dir.exists():
    app.mount("/static", StaticFiles(directory=str(ui_dir)), name="static")
