"""
Scheduler - Cron-like scheduling for recurring crawls.

Features:
- Create, update, delete scheduled crawls
- Cron expression support (e.g., "0 9 * * *" for daily at 9 AM)
- Interval-based scheduling (every N hours/days)
- One-time scheduled crawls
- Pause/resume schedules
"""

import asyncio
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional
import sqlite3
import json

from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger

from .config import CrawlerConfig
from .crawler import Crawler


class ScheduleType(str, Enum):
    """Type of schedule."""
    CRON = "cron"           # Cron expression
    INTERVAL = "interval"   # Every N seconds/minutes/hours/days
    ONCE = "once"           # One-time scheduled run


class ScheduleStatus(str, Enum):
    """Schedule status."""
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"  # For one-time schedules


class ScheduleConfig(BaseModel):
    """Configuration for a scheduled crawl."""
    id: Optional[int] = None
    name: str = Field(..., description="Schedule name")
    url: str = Field(..., description="URL to crawl")
    
    # Schedule settings
    schedule_type: ScheduleType = ScheduleType.INTERVAL
    cron_expression: Optional[str] = None  # For CRON type: "0 9 * * *"
    interval_seconds: Optional[int] = None  # For INTERVAL type
    run_at: Optional[datetime] = None       # For ONCE type
    
    # Crawl settings
    max_pages: int = 100
    max_depth: int = 10
    delay: float = 1.0
    concurrent: int = 5
    respect_robots: bool = True
    render: bool = False
    
    # Status
    status: ScheduleStatus = ScheduleStatus.ACTIVE
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    
    # Metadata
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class SchedulerDatabase:
    """SQLite database for schedule storage."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
    
    def connect(self) -> None:
        """Initialize database connection and schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS schedules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                url TEXT NOT NULL,
                schedule_type TEXT NOT NULL,
                cron_expression TEXT,
                interval_seconds INTEGER,
                run_at TEXT,
                max_pages INTEGER DEFAULT 100,
                max_depth INTEGER DEFAULT 10,
                delay REAL DEFAULT 1.0,
                concurrent INTEGER DEFAULT 5,
                respect_robots INTEGER DEFAULT 1,
                render INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                last_run TEXT,
                next_run TEXT,
                run_count INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS schedule_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schedule_id INTEGER NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT DEFAULT 'running',
                pages_crawled INTEGER DEFAULT 0,
                pages_failed INTEGER DEFAULT 0,
                error TEXT,
                FOREIGN KEY (schedule_id) REFERENCES schedules(id)
            );
        """)
        self._conn.commit()
    
    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def create_schedule(self, config: ScheduleConfig) -> int:
        """Create a new schedule."""
        cursor = self._conn.execute("""
            INSERT INTO schedules (
                name, url, schedule_type, cron_expression, interval_seconds,
                run_at, max_pages, max_depth, delay, concurrent,
                respect_robots, render, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            config.name, config.url, config.schedule_type.value,
            config.cron_expression, config.interval_seconds,
            config.run_at.isoformat() if config.run_at else None,
            config.max_pages, config.max_depth, config.delay, config.concurrent,
            1 if config.respect_robots else 0, 1 if config.render else 0,
            config.status.value
        ))
        self._conn.commit()
        return cursor.lastrowid
    
    def get_schedule(self, schedule_id: int) -> Optional[ScheduleConfig]:
        """Get a schedule by ID."""
        cursor = self._conn.execute(
            "SELECT * FROM schedules WHERE id = ?", (schedule_id,)
        )
        row = cursor.fetchone()
        return self._row_to_config(row) if row else None
    
    def get_all_schedules(self) -> list[ScheduleConfig]:
        """Get all schedules."""
        cursor = self._conn.execute("SELECT * FROM schedules ORDER BY id DESC")
        return [self._row_to_config(row) for row in cursor.fetchall()]
    
    def get_active_schedules(self) -> list[ScheduleConfig]:
        """Get active schedules."""
        cursor = self._conn.execute(
            "SELECT * FROM schedules WHERE status = 'active'"
        )
        return [self._row_to_config(row) for row in cursor.fetchall()]
    
    def update_schedule(self, schedule_id: int, **updates) -> bool:
        """Update a schedule."""
        if not updates:
            return False
        
        updates['updated_at'] = datetime.now().isoformat()
        
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [schedule_id]
        
        cursor = self._conn.execute(
            f"UPDATE schedules SET {set_clause} WHERE id = ?", values
        )
        self._conn.commit()
        return cursor.rowcount > 0
    
    def delete_schedule(self, schedule_id: int) -> bool:
        """Delete a schedule."""
        cursor = self._conn.execute(
            "DELETE FROM schedules WHERE id = ?", (schedule_id,)
        )
        self._conn.commit()
        return cursor.rowcount > 0
    
    def record_run_start(self, schedule_id: int) -> int:
        """Record a schedule run start."""
        cursor = self._conn.execute("""
            INSERT INTO schedule_runs (schedule_id, started_at)
            VALUES (?, ?)
        """, (schedule_id, datetime.now().isoformat()))
        
        self._conn.execute("""
            UPDATE schedules SET last_run = ?, run_count = run_count + 1
            WHERE id = ?
        """, (datetime.now().isoformat(), schedule_id))
        
        self._conn.commit()
        return cursor.lastrowid
    
    def record_run_complete(
        self, 
        run_id: int, 
        pages_crawled: int, 
        pages_failed: int,
        error: Optional[str] = None
    ) -> None:
        """Record a schedule run completion."""
        status = 'completed' if not error else 'failed'
        self._conn.execute("""
            UPDATE schedule_runs SET
                completed_at = ?, status = ?, pages_crawled = ?,
                pages_failed = ?, error = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), status, pages_crawled, pages_failed, error, run_id))
        self._conn.commit()
    
    def get_schedule_runs(self, schedule_id: int, limit: int = 10) -> list[dict]:
        """Get recent runs for a schedule."""
        cursor = self._conn.execute("""
            SELECT * FROM schedule_runs
            WHERE schedule_id = ?
            ORDER BY id DESC LIMIT ?
        """, (schedule_id, limit))
        return [dict(row) for row in cursor.fetchall()]
    
    def _row_to_config(self, row: sqlite3.Row) -> ScheduleConfig:
        """Convert a database row to ScheduleConfig."""
        return ScheduleConfig(
            id=row['id'],
            name=row['name'],
            url=row['url'],
            schedule_type=ScheduleType(row['schedule_type']),
            cron_expression=row['cron_expression'],
            interval_seconds=row['interval_seconds'],
            run_at=datetime.fromisoformat(row['run_at']) if row['run_at'] else None,
            max_pages=row['max_pages'],
            max_depth=row['max_depth'],
            delay=row['delay'],
            concurrent=row['concurrent'],
            respect_robots=bool(row['respect_robots']),
            render=bool(row['render']),
            status=ScheduleStatus(row['status']),
            last_run=datetime.fromisoformat(row['last_run']) if row['last_run'] else None,
            next_run=datetime.fromisoformat(row['next_run']) if row['next_run'] else None,
            run_count=row['run_count'],
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else None,
            updated_at=datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None
        )


class CrawlScheduler:
    """
    Manages scheduled crawls using APScheduler.
    """
    
    def __init__(self, db_path: Path, output_dir: Path):
        self.db = SchedulerDatabase(db_path)
        self.output_dir = output_dir
        self.scheduler = AsyncIOScheduler()
        self._running_jobs: dict[int, str] = {}  # schedule_id -> job_id
    
    async def start(self) -> None:
        """Start the scheduler service."""
        self.db.connect()
        self.scheduler.start()
        
        # Load existing schedules
        schedules = self.db.get_active_schedules()
        for schedule in schedules:
            self._add_job(schedule)
    
    async def stop(self) -> None:
        """Stop the scheduler service."""
        self.scheduler.shutdown(wait=False)
        self.db.close()
    
    def create_schedule(self, config: ScheduleConfig) -> ScheduleConfig:
        """Create a new schedule."""
        schedule_id = self.db.create_schedule(config)
        config.id = schedule_id
        
        if config.status == ScheduleStatus.ACTIVE:
            self._add_job(config)
        
        return config
    
    def update_schedule(self, schedule_id: int, **updates) -> Optional[ScheduleConfig]:
        """Update a schedule."""
        # Remove existing job
        if schedule_id in self._running_jobs:
            self.scheduler.remove_job(self._running_jobs[schedule_id])
            del self._running_jobs[schedule_id]
        
        # Update in database
        self.db.update_schedule(schedule_id, **updates)
        
        # Get updated config and re-add job if active
        config = self.db.get_schedule(schedule_id)
        if config and config.status == ScheduleStatus.ACTIVE:
            self._add_job(config)
        
        return config
    
    def delete_schedule(self, schedule_id: int) -> bool:
        """Delete a schedule."""
        # Remove job if running
        if schedule_id in self._running_jobs:
            self.scheduler.remove_job(self._running_jobs[schedule_id])
            del self._running_jobs[schedule_id]
        
        return self.db.delete_schedule(schedule_id)
    
    def pause_schedule(self, schedule_id: int) -> bool:
        """Pause a schedule."""
        if schedule_id in self._running_jobs:
            self.scheduler.pause_job(self._running_jobs[schedule_id])
        return self.db.update_schedule(schedule_id, status=ScheduleStatus.PAUSED.value)
    
    def resume_schedule(self, schedule_id: int) -> bool:
        """Resume a paused schedule."""
        if schedule_id in self._running_jobs:
            self.scheduler.resume_job(self._running_jobs[schedule_id])
        else:
            config = self.db.get_schedule(schedule_id)
            if config:
                self._add_job(config)
        return self.db.update_schedule(schedule_id, status=ScheduleStatus.ACTIVE.value)
    
    def get_schedule(self, schedule_id: int) -> Optional[ScheduleConfig]:
        """Get a schedule."""
        return self.db.get_schedule(schedule_id)
    
    def get_all_schedules(self) -> list[ScheduleConfig]:
        """Get all schedules."""
        return self.db.get_all_schedules()
    
    def get_schedule_runs(self, schedule_id: int) -> list[dict]:
        """Get runs for a schedule."""
        return self.db.get_schedule_runs(schedule_id)
    
    def _add_job(self, config: ScheduleConfig) -> None:
        """Add a job to the scheduler."""
        trigger = self._get_trigger(config)
        if not trigger:
            return
        
        job = self.scheduler.add_job(
            self._run_crawl,
            trigger=trigger,
            args=[config.id],
            id=f"schedule_{config.id}",
            name=config.name,
            replace_existing=True
        )
        
        self._running_jobs[config.id] = job.id
        
        # Update next run time
        if job.next_run_time:
            self.db.update_schedule(
                config.id,
                next_run=job.next_run_time.isoformat()
            )
    
    def _get_trigger(self, config: ScheduleConfig):
        """Get APScheduler trigger for a schedule."""
        if config.schedule_type == ScheduleType.CRON:
            if config.cron_expression:
                return CronTrigger.from_crontab(config.cron_expression)
        
        elif config.schedule_type == ScheduleType.INTERVAL:
            if config.interval_seconds:
                return IntervalTrigger(seconds=config.interval_seconds)
        
        elif config.schedule_type == ScheduleType.ONCE:
            if config.run_at and config.run_at > datetime.now():
                return DateTrigger(run_date=config.run_at)
        
        return None
    
    async def _run_crawl(self, schedule_id: int) -> None:
        """Execute a scheduled crawl."""
        config = self.db.get_schedule(schedule_id)
        if not config:
            return
        
        # Record run start
        run_id = self.db.record_run_start(schedule_id)
        
        try:
            # Create crawler config
            crawler_config = CrawlerConfig(
                max_pages=config.max_pages,
                max_depth=config.max_depth,
                delay_min=config.delay * 0.5,
                delay_max=config.delay * 1.5,
                concurrent_requests=config.concurrent,
                respect_robots_txt=config.respect_robots,
                enable_rendering=config.render,
                output_dir=self.output_dir
            )
            crawler_config.db_path = self.output_dir / "crawler.db"
            
            # Run crawl
            crawler = Crawler(crawler_config)
            stats = await crawler.crawl(config.url)
            
            # Record completion
            self.db.record_run_complete(
                run_id,
                stats.pages_crawled,
                stats.pages_failed
            )
            
            # Mark one-time schedules as completed
            if config.schedule_type == ScheduleType.ONCE:
                self.db.update_schedule(schedule_id, status=ScheduleStatus.COMPLETED.value)
        
        except Exception as e:
            self.db.record_run_complete(run_id, 0, 0, str(e))


# Global scheduler instance
_scheduler: Optional[CrawlScheduler] = None


def get_scheduler(db_path: Path = None, output_dir: Path = None) -> CrawlScheduler:
    """Get or create the global scheduler instance."""
    global _scheduler
    if _scheduler is None:
        _scheduler = CrawlScheduler(
            db_path or Path("./data/scheduler.db"),
            output_dir or Path("./data")
        )
    return _scheduler
