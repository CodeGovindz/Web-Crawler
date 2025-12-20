"""
Web API for the Web Crawler - FastAPI backend with WebSocket support.

Provides:
- REST API for starting/stopping crawls
- WebSocket for real-time progress updates
- Crawl history and data viewing
- Schedule management for recurring crawls
- Change detection and monitoring
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
from crawler.scheduler import (
    CrawlScheduler, ScheduleConfig, ScheduleType, ScheduleStatus, get_scheduler
)
from crawler.changes import ChangeDatabase, ChangeDetector, get_change_db
from crawler.classifier import classify_content, get_classifier


app = FastAPI(
    title="SOTA Web Crawler",
    description="State-of-the-art web crawling system with real-time monitoring",
    version="1.2.0"
)

# Global instances
scheduler: Optional[CrawlScheduler] = None
change_db: Optional[ChangeDatabase] = None


@app.on_event("startup")
async def startup_event():
    """Initialize scheduler and change detection on startup."""
    global scheduler, change_db
    scheduler = get_scheduler(
        db_path=Path("./data/scheduler.db"),
        output_dir=Path("./data")
    )
    await scheduler.start()
    
    change_db = get_change_db(Path("./data/changes.db"))


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global scheduler, change_db
    if scheduler:
        await scheduler.stop()
    if change_db:
        change_db.close()

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


# ============== Schedule API Endpoints ==============

class ScheduleRequest(BaseModel):
    """Request to create/update a schedule."""
    name: str
    url: str
    schedule_type: str = "interval"  # cron, interval, once
    cron_expression: Optional[str] = None
    interval_hours: Optional[int] = None
    run_at: Optional[str] = None  # ISO datetime string
    max_pages: int = 100
    max_depth: int = 10
    delay: float = 1.0
    concurrent: int = 5
    respect_robots: bool = True
    render: bool = False


@app.get("/api/schedules")
async def get_schedules():
    """Get all schedules."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    schedules = scheduler.get_all_schedules()
    return {
        "schedules": [
            {
                "id": s.id,
                "name": s.name,
                "url": s.url,
                "schedule_type": s.schedule_type.value,
                "cron_expression": s.cron_expression,
                "interval_seconds": s.interval_seconds,
                "status": s.status.value,
                "last_run": s.last_run.isoformat() if s.last_run else None,
                "next_run": s.next_run.isoformat() if s.next_run else None,
                "run_count": s.run_count
            }
            for s in schedules
        ]
    }


@app.post("/api/schedules")
async def create_schedule(request: ScheduleRequest):
    """Create a new schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    # Convert interval hours to seconds
    interval_seconds = None
    if request.interval_hours:
        interval_seconds = request.interval_hours * 3600
    
    # Parse run_at datetime
    run_at = None
    if request.run_at:
        try:
            run_at = datetime.fromisoformat(request.run_at)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid datetime format")
    
    config = ScheduleConfig(
        name=request.name,
        url=request.url,
        schedule_type=ScheduleType(request.schedule_type),
        cron_expression=request.cron_expression,
        interval_seconds=interval_seconds,
        run_at=run_at,
        max_pages=request.max_pages,
        max_depth=request.max_depth,
        delay=request.delay,
        concurrent=request.concurrent,
        respect_robots=request.respect_robots,
        render=request.render
    )
    
    created = scheduler.create_schedule(config)
    
    return {
        "message": "Schedule created",
        "schedule": {
            "id": created.id,
            "name": created.name,
            "status": created.status.value
        }
    }


@app.get("/api/schedules/{schedule_id}")
async def get_schedule(schedule_id: int):
    """Get a specific schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    schedule = scheduler.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    
    return {
        "id": schedule.id,
        "name": schedule.name,
        "url": schedule.url,
        "schedule_type": schedule.schedule_type.value,
        "cron_expression": schedule.cron_expression,
        "interval_seconds": schedule.interval_seconds,
        "max_pages": schedule.max_pages,
        "max_depth": schedule.max_depth,
        "delay": schedule.delay,
        "concurrent": schedule.concurrent,
        "respect_robots": schedule.respect_robots,
        "render": schedule.render,
        "status": schedule.status.value,
        "last_run": schedule.last_run.isoformat() if schedule.last_run else None,
        "next_run": schedule.next_run.isoformat() if schedule.next_run else None,
        "run_count": schedule.run_count
    }


@app.delete("/api/schedules/{schedule_id}")
async def delete_schedule(schedule_id: int):
    """Delete a schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    if scheduler.delete_schedule(schedule_id):
        return {"message": "Schedule deleted"}
    raise HTTPException(status_code=404, detail="Schedule not found")


@app.post("/api/schedules/{schedule_id}/pause")
async def pause_schedule(schedule_id: int):
    """Pause a schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    if scheduler.pause_schedule(schedule_id):
        return {"message": "Schedule paused"}
    raise HTTPException(status_code=404, detail="Schedule not found")


@app.post("/api/schedules/{schedule_id}/resume")
async def resume_schedule(schedule_id: int):
    """Resume a paused schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    if scheduler.resume_schedule(schedule_id):
        return {"message": "Schedule resumed"}
    raise HTTPException(status_code=404, detail="Schedule not found")


@app.get("/api/schedules/{schedule_id}/runs")
async def get_schedule_runs(schedule_id: int):
    """Get recent runs for a schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    runs = scheduler.get_schedule_runs(schedule_id)
    return {"runs": runs}


@app.post("/api/schedules/{schedule_id}/run-now")
async def run_schedule_now(schedule_id: int):
    """Trigger an immediate run of a schedule."""
    if not scheduler:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    
    schedule = scheduler.get_schedule(schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    
    # Trigger the crawl immediately
    asyncio.create_task(scheduler._run_crawl(schedule_id))
    
    return {"message": "Schedule triggered"}


# ============== Change Detection API Endpoints ==============

class MonitorUrlRequest(BaseModel):
    """Request to add a URL for monitoring."""
    url: str
    name: Optional[str] = None
    check_interval_hours: int = 24


class CheckUrlRequest(BaseModel):
    """Request to check a URL for changes."""
    url: str
    html: str


@app.get("/api/changes")
async def get_recent_changes():
    """Get recent content changes across all monitored URLs."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    changes = change_db.get_recent_changes(limit=50)
    return {"changes": changes}


@app.get("/api/changes/tracked")
async def get_tracked_urls():
    """Get all URLs being tracked for changes."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    urls = change_db.get_tracked_urls()
    monitored = change_db.get_monitored_urls()
    
    return {
        "tracked_urls": urls,
        "monitored_urls": monitored
    }


@app.post("/api/changes/monitor")
async def add_monitored_url(request: MonitorUrlRequest):
    """Add a URL to actively monitor for changes."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    url_id = change_db.add_monitored_url(
        request.url, 
        request.name, 
        request.check_interval_hours
    )
    
    return {"message": "URL added for monitoring", "id": url_id}


@app.delete("/api/changes/monitor/{url_id}")
async def remove_monitored_url(url_id: int):
    """Remove a URL from monitoring."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    if change_db.delete_monitored_url(url_id):
        return {"message": "URL removed from monitoring"}
    raise HTTPException(status_code=404, detail="Monitored URL not found")


@app.get("/api/changes/url/{url_path:path}")
async def get_url_changes(url_path: str):
    """Get change history for a specific URL."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    # Reconstruct URL (path param strips the protocol)
    url = url_path if url_path.startswith("http") else f"https://{url_path}"
    
    changes = change_db.get_changes_for_url(url)
    versions = change_db.get_version_history(url)
    
    return {
        "url": url,
        "changes": changes,
        "versions": [
            {
                "id": v.id,
                "title": v.title,
                "word_count": v.word_count,
                "captured_at": v.captured_at.isoformat() if v.captured_at else None
            }
            for v in versions
        ]
    }


@app.get("/api/changes/version/{version_id}")
async def get_version_content(version_id: int):
    """Get the content of a specific version."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    version = change_db.get_version(version_id)
    if not version:
        raise HTTPException(status_code=404, detail="Version not found")
    
    return {
        "id": version.id,
        "url": version.url,
        "title": version.title,
        "text_content": version.text_content[:5000],  # Limit content size
        "word_count": version.word_count,
        "captured_at": version.captured_at.isoformat() if version.captured_at else None
    }


@app.get("/api/changes/diff/{old_id}/{new_id}")
async def get_version_diff(old_id: int, new_id: int):
    """Get diff between two versions."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    old_version = change_db.get_version(old_id)
    new_version = change_db.get_version(new_id)
    
    if not old_version or not new_version:
        raise HTTPException(status_code=404, detail="Version(s) not found")
    
    detector = ChangeDetector(change_db)
    diff_result = detector.calculate_diff(
        old_version.text_content, 
        new_version.text_content
    )
    
    return {
        "old_version": {"id": old_id, "title": old_version.title},
        "new_version": {"id": new_id, "title": new_version.title},
        "change_percent": diff_result['change_percent'],
        "added_lines": diff_result['added'],
        "removed_lines": diff_result['removed'],
        "summary": diff_result['summary'],
        "diff_lines": diff_result['diff_lines'][:50]  # Limit for API response
    }


@app.post("/api/changes/check")
async def check_url_for_changes(request: CheckUrlRequest):
    """Check a URL for changes with provided HTML content."""
    if not change_db:
        raise HTTPException(status_code=503, detail="Change detection not initialized")
    
    detector = ChangeDetector(change_db)
    change = detector.check_for_changes(request.url, request.html)
    
    if change:
        return {
            "changed": True,
            "change_type": change.change_type,
            "change_percent": change.change_percent,
            "summary": change.diff_summary
        }
    
    return {"changed": False}


# ============== Classification API Endpoints ==============

class ClassifyRequest(BaseModel):
    """Request to classify content."""
    text: str
    url: Optional[str] = ""
    title: Optional[str] = ""


@app.post("/api/classify")
async def classify_text(request: ClassifyRequest):
    """Classify text content and extract insights."""
    result = classify_content(
        text=request.text,
        url=request.url or "",
        title=request.title or ""
    )
    return result


@app.get("/api/classify/session/{session_id}")
async def classify_session(session_id: int, limit: int = 20):
    """Classify all content from a crawl session."""
    content_file = Path(f"./data/content_{session_id}.jsonl")
    if not content_file.exists():
        raise HTTPException(status_code=404, detail="Session data not found")
    
    results = []
    classifier = get_classifier(use_ml=False)  # Use fast rule-based
    
    with open(content_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= limit:
                break
            if line.strip():
                try:
                    record = json.loads(line)
                    text = record.get('text', '')[:3000]
                    title = record.get('title', '')
                    url = record.get('url', '')
                    
                    classification = classifier.classify(text, url, title)
                    results.append({
                        "url": url,
                        "title": title,
                        "category": classification.category,
                        "confidence": classification.category_confidence,
                        "keywords": classification.keywords[:5],
                        "sentiment": classification.sentiment,
                        "word_count": classification.word_count
                    })
                except json.JSONDecodeError:
                    pass
    
    # Category summary
    categories = {}
    for r in results:
        cat = r["category"]
        categories[cat] = categories.get(cat, 0) + 1
    
    return {
        "session_id": session_id,
        "total_classified": len(results),
        "category_distribution": categories,
        "pages": results
    }


# Mount static files for UI
ui_dir = Path(__file__).parent / "ui"
if ui_dir.exists():
    app.mount("/static", StaticFiles(directory=str(ui_dir)), name="static")
