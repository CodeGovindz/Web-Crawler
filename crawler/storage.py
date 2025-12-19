"""
Storage - Data persistence for crawled content.

Features:
- SQLite-based URL tracking
- JSON Lines output for content
- Resumable crawl state
- Export functionality
"""

import asyncio
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional, Any

import aiofiles


class CrawlDatabase:
    """
    SQLite database for crawl state tracking.
    """
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()
    
    def connect(self) -> None:
        """Initialize database connection and schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        
        # Create tables
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS crawl_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seed_url TEXT NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                status TEXT DEFAULT 'running',
                pages_crawled INTEGER DEFAULT 0,
                pages_failed INTEGER DEFAULT 0
            );
            
            CREATE TABLE IF NOT EXISTS urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                url TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                http_status INTEGER,
                content_type TEXT,
                depth INTEGER DEFAULT 0,
                parent_url TEXT,
                crawled_at TIMESTAMP,
                error TEXT,
                FOREIGN KEY (session_id) REFERENCES crawl_sessions(id),
                UNIQUE(session_id, url)
            );
            
            CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(session_id, status);
            CREATE INDEX IF NOT EXISTS idx_urls_url ON urls(url);
            
            CREATE TABLE IF NOT EXISTS domain_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                domain TEXT NOT NULL,
                pages_crawled INTEGER DEFAULT 0,
                pages_failed INTEGER DEFAULT 0,
                last_crawled_at TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES crawl_sessions(id),
                UNIQUE(session_id, domain)
            );
        """)
        
        self._conn.commit()
    
    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    async def create_session(self, seed_url: str) -> int:
        """Create a new crawl session."""
        async with self._lock:
            cursor = self._conn.execute(
                "INSERT INTO crawl_sessions (seed_url) VALUES (?)",
                (seed_url,)
            )
            self._conn.commit()
            return cursor.lastrowid
    
    async def get_session(self, session_id: int) -> Optional[dict]:
        """Get session details."""
        async with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM crawl_sessions WHERE id = ?",
                (session_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    async def get_latest_session(self) -> Optional[dict]:
        """Get the most recent session."""
        async with self._lock:
            cursor = self._conn.execute(
                "SELECT * FROM crawl_sessions ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    
    async def update_session(
        self,
        session_id: int,
        status: Optional[str] = None,
        pages_crawled: Optional[int] = None,
        pages_failed: Optional[int] = None
    ) -> None:
        """Update session stats."""
        async with self._lock:
            updates = []
            values = []
            
            if status:
                updates.append("status = ?")
                values.append(status)
                if status == 'completed':
                    updates.append("completed_at = ?")
                    values.append(datetime.now().isoformat())
            
            if pages_crawled is not None:
                updates.append("pages_crawled = ?")
                values.append(pages_crawled)
            
            if pages_failed is not None:
                updates.append("pages_failed = ?")
                values.append(pages_failed)
            
            if updates:
                values.append(session_id)
                self._conn.execute(
                    f"UPDATE crawl_sessions SET {', '.join(updates)} WHERE id = ?",
                    values
                )
                self._conn.commit()
    
    async def add_url(
        self,
        session_id: int,
        url: str,
        depth: int = 0,
        parent_url: Optional[str] = None
    ) -> bool:
        """Add a URL to crawl. Returns True if added, False if exists."""
        async with self._lock:
            try:
                self._conn.execute(
                    """INSERT INTO urls (session_id, url, depth, parent_url)
                       VALUES (?, ?, ?, ?)""",
                    (session_id, url, depth, parent_url)
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
    
    async def mark_url_crawled(
        self,
        session_id: int,
        url: str,
        http_status: int,
        content_type: Optional[str] = None,
        error: Optional[str] = None
    ) -> None:
        """Mark a URL as crawled."""
        status = 'completed' if not error else 'failed'
        
        async with self._lock:
            self._conn.execute(
                """UPDATE urls SET status = ?, http_status = ?, content_type = ?,
                   crawled_at = ?, error = ? WHERE session_id = ? AND url = ?""",
                (status, http_status, content_type, datetime.now().isoformat(),
                 error, session_id, url)
            )
            self._conn.commit()
    
    async def get_pending_urls(
        self,
        session_id: int,
        limit: int = 100
    ) -> list[dict]:
        """Get pending URLs to crawl."""
        async with self._lock:
            cursor = self._conn.execute(
                """SELECT url, depth, parent_url FROM urls
                   WHERE session_id = ? AND status = 'pending'
                   ORDER BY depth ASC, id ASC LIMIT ?""",
                (session_id, limit)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    async def get_stats(self, session_id: int) -> dict:
        """Get crawl statistics."""
        async with self._lock:
            cursor = self._conn.execute(
                """SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
                   FROM urls WHERE session_id = ?""",
                (session_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else {}


class ContentStorage:
    """
    Stores crawled content as JSON Lines.
    """
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self._content_file: Optional[Path] = None
        self._lock = asyncio.Lock()
    
    def initialize(self, session_id: int) -> None:
        """Initialize storage for a session."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._content_file = self.output_dir / f"content_{session_id}.jsonl"
    
    async def save_page(
        self,
        url: str,
        content: dict[str, Any]
    ) -> None:
        """Save a page's content."""
        if not self._content_file:
            raise RuntimeError("Storage not initialized")
        
        record = {
            'url': url,
            'crawled_at': datetime.now().isoformat(),
            **content
        }
        
        async with self._lock:
            async with aiofiles.open(self._content_file, 'a', encoding='utf-8') as f:
                await f.write(json.dumps(record, ensure_ascii=False) + '\n')
    
    async def export_to_json(self, output_path: Path) -> int:
        """Export all content to a JSON file. Returns record count."""
        if not self._content_file or not self._content_file.exists():
            return 0
        
        records = []
        async with aiofiles.open(self._content_file, 'r', encoding='utf-8') as f:
            async for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        async with aiofiles.open(output_path, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(records, indent=2, ensure_ascii=False))
        
        return len(records)
    
    async def export_to_csv(self, output_path: Path, fields: list[str]) -> int:
        """Export specified fields to CSV. Returns record count."""
        if not self._content_file or not self._content_file.exists():
            return 0
        
        import csv
        
        records = []
        async with aiofiles.open(self._content_file, 'r', encoding='utf-8') as f:
            async for line in f:
                if line.strip():
                    records.append(json.loads(line))
        
        # Write CSV synchronously (csv module doesn't support async)
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            for record in records:
                writer.writerow(record)
        
        return len(records)


class StorageManager:
    """
    Manages both database and content storage.
    """
    
    def __init__(self, db_path: Path, output_dir: Path):
        self.db = CrawlDatabase(db_path)
        self.content = ContentStorage(output_dir)
        self._session_id: Optional[int] = None
    
    async def start_session(self, seed_url: str) -> int:
        """Start a new crawl session."""
        self.db.connect()
        self._session_id = await self.db.create_session(seed_url)
        self.content.initialize(self._session_id)
        return self._session_id
    
    async def resume_session(self) -> Optional[int]:
        """Resume the latest incomplete session."""
        self.db.connect()
        session = await self.db.get_latest_session()
        
        if session and session['status'] == 'running':
            self._session_id = session['id']
            self.content.initialize(self._session_id)
            return self._session_id
        
        return None
    
    @property
    def session_id(self) -> int:
        if not self._session_id:
            raise RuntimeError("No active session")
        return self._session_id
    
    async def close(self) -> None:
        """Close storage connections."""
        if self._session_id:
            await self.db.update_session(self._session_id, status='completed')
        self.db.close()
