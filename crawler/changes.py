"""
Change Detection - Track page changes over time.

Features:
- Content hashing for fast change detection
- Diff calculation between versions
- Change history storage
- Alert system for significant changes
"""

import hashlib
import difflib
from datetime import datetime
from pathlib import Path
from typing import Optional
import sqlite3
import json
import re

from pydantic import BaseModel
from bs4 import BeautifulSoup


class PageVersion(BaseModel):
    """A snapshot of a page at a point in time."""
    id: Optional[int] = None
    url: str
    content_hash: str
    text_content: str
    title: Optional[str] = None
    word_count: int = 0
    captured_at: datetime = None
    
    class Config:
        arbitrary_types_allowed = True


class ContentChange(BaseModel):
    """Detected change between two versions."""
    url: str
    old_version_id: int
    new_version_id: int
    change_type: str  # 'added', 'modified', 'deleted'
    change_percent: float
    added_lines: int
    removed_lines: int
    diff_summary: str
    detected_at: datetime


class ChangeDatabase:
    """SQLite database for storing page versions and changes."""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
    
    def connect(self) -> None:
        """Initialize database connection and schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS page_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                text_content TEXT NOT NULL,
                title TEXT,
                word_count INTEGER DEFAULT 0,
                captured_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(url, content_hash)
            );
            
            CREATE INDEX IF NOT EXISTS idx_versions_url ON page_versions(url);
            CREATE INDEX IF NOT EXISTS idx_versions_hash ON page_versions(content_hash);
            
            CREATE TABLE IF NOT EXISTS content_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL,
                old_version_id INTEGER,
                new_version_id INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                change_percent REAL DEFAULT 0,
                added_lines INTEGER DEFAULT 0,
                removed_lines INTEGER DEFAULT 0,
                diff_summary TEXT,
                detected_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (old_version_id) REFERENCES page_versions(id),
                FOREIGN KEY (new_version_id) REFERENCES page_versions(id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_changes_url ON content_changes(url);
            
            CREATE TABLE IF NOT EXISTS monitored_urls (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                name TEXT,
                check_interval_hours INTEGER DEFAULT 24,
                last_checked TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        self._conn.commit()
    
    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def save_version(self, version: PageVersion) -> int:
        """Save a page version, returns version ID."""
        try:
            cursor = self._conn.execute("""
                INSERT INTO page_versions (url, content_hash, text_content, title, word_count)
                VALUES (?, ?, ?, ?, ?)
            """, (version.url, version.content_hash, version.text_content, 
                  version.title, version.word_count))
            self._conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Version with same hash already exists
            cursor = self._conn.execute(
                "SELECT id FROM page_versions WHERE url = ? AND content_hash = ?",
                (version.url, version.content_hash)
            )
            return cursor.fetchone()['id']
    
    def get_latest_version(self, url: str) -> Optional[PageVersion]:
        """Get the most recent version of a URL."""
        cursor = self._conn.execute("""
            SELECT * FROM page_versions 
            WHERE url = ? 
            ORDER BY id DESC LIMIT 1
        """, (url,))
        row = cursor.fetchone()
        return self._row_to_version(row) if row else None
    
    def get_version_history(self, url: str, limit: int = 20) -> list[PageVersion]:
        """Get version history for a URL."""
        cursor = self._conn.execute("""
            SELECT * FROM page_versions 
            WHERE url = ? 
            ORDER BY id DESC LIMIT ?
        """, (url, limit))
        return [self._row_to_version(row) for row in cursor.fetchall()]
    
    def get_version(self, version_id: int) -> Optional[PageVersion]:
        """Get a specific version by ID."""
        cursor = self._conn.execute(
            "SELECT * FROM page_versions WHERE id = ?", (version_id,)
        )
        row = cursor.fetchone()
        return self._row_to_version(row) if row else None
    
    def save_change(self, change: ContentChange) -> int:
        """Save a detected change."""
        cursor = self._conn.execute("""
            INSERT INTO content_changes 
            (url, old_version_id, new_version_id, change_type, 
             change_percent, added_lines, removed_lines, diff_summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (change.url, change.old_version_id, change.new_version_id,
              change.change_type, change.change_percent, change.added_lines,
              change.removed_lines, change.diff_summary))
        self._conn.commit()
        return cursor.lastrowid
    
    def get_recent_changes(self, limit: int = 50) -> list[dict]:
        """Get recent changes across all URLs."""
        cursor = self._conn.execute("""
            SELECT c.*, 
                   old.title as old_title, 
                   new.title as new_title
            FROM content_changes c
            LEFT JOIN page_versions old ON c.old_version_id = old.id
            JOIN page_versions new ON c.new_version_id = new.id
            ORDER BY c.id DESC LIMIT ?
        """, (limit,))
        return [dict(row) for row in cursor.fetchall()]
    
    def get_changes_for_url(self, url: str, limit: int = 20) -> list[dict]:
        """Get changes for a specific URL."""
        cursor = self._conn.execute("""
            SELECT * FROM content_changes 
            WHERE url = ? 
            ORDER BY id DESC LIMIT ?
        """, (url, limit))
        return [dict(row) for row in cursor.fetchall()]
    
    # Monitored URLs methods
    def add_monitored_url(self, url: str, name: str = None, 
                          check_interval_hours: int = 24) -> int:
        """Add a URL to monitor for changes."""
        cursor = self._conn.execute("""
            INSERT OR REPLACE INTO monitored_urls (url, name, check_interval_hours)
            VALUES (?, ?, ?)
        """, (url, name or url, check_interval_hours))
        self._conn.commit()
        return cursor.lastrowid
    
    def get_monitored_urls(self) -> list[dict]:
        """Get all monitored URLs."""
        cursor = self._conn.execute(
            "SELECT * FROM monitored_urls WHERE is_active = 1"
        )
        return [dict(row) for row in cursor.fetchall()]
    
    def update_last_checked(self, url: str) -> None:
        """Update last checked timestamp for a URL."""
        self._conn.execute("""
            UPDATE monitored_urls SET last_checked = ? WHERE url = ?
        """, (datetime.now().isoformat(), url))
        self._conn.commit()
    
    def delete_monitored_url(self, url_id: int) -> bool:
        """Remove a URL from monitoring."""
        cursor = self._conn.execute(
            "DELETE FROM monitored_urls WHERE id = ?", (url_id,)
        )
        self._conn.commit()
        return cursor.rowcount > 0
    
    def get_tracked_urls(self) -> list[str]:
        """Get all unique URLs that have been tracked."""
        cursor = self._conn.execute(
            "SELECT DISTINCT url FROM page_versions ORDER BY url"
        )
        return [row['url'] for row in cursor.fetchall()]
    
    def _row_to_version(self, row: sqlite3.Row) -> PageVersion:
        return PageVersion(
            id=row['id'],
            url=row['url'],
            content_hash=row['content_hash'],
            text_content=row['text_content'],
            title=row['title'],
            word_count=row['word_count'],
            captured_at=datetime.fromisoformat(row['captured_at']) if row['captured_at'] else None
        )


class ChangeDetector:
    """Detects and tracks content changes."""
    
    def __init__(self, db: ChangeDatabase):
        self.db = db
    
    @staticmethod
    def compute_hash(content: str) -> str:
        """Compute content hash for change detection."""
        # Normalize content before hashing
        normalized = re.sub(r'\s+', ' ', content.strip().lower())
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]
    
    @staticmethod
    def extract_text(html: str) -> tuple[str, str]:
        """Extract clean text and title from HTML."""
        soup = BeautifulSoup(html, 'lxml')
        
        # Get title
        title = ""
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
        
        # Remove unwanted elements
        for tag in soup.find_all(['script', 'style', 'nav', 'footer', 'header']):
            tag.decompose()
        
        # Get text
        text = soup.get_text(separator='\n', strip=True)
        return text, title
    
    def check_for_changes(self, url: str, html: str) -> Optional[ContentChange]:
        """
        Check if content has changed since last version.
        Returns ContentChange if changed, None if no change.
        """
        text, title = self.extract_text(html)
        content_hash = self.compute_hash(text)
        word_count = len(text.split())
        
        # Get previous version
        prev_version = self.db.get_latest_version(url)
        
        # Create new version
        new_version = PageVersion(
            url=url,
            content_hash=content_hash,
            text_content=text,
            title=title,
            word_count=word_count,
            captured_at=datetime.now()
        )
        
        # Save new version
        new_version_id = self.db.save_version(new_version)
        
        # If no previous version or same hash, no change
        if not prev_version:
            # First time seeing this URL - record as 'added'
            change = ContentChange(
                url=url,
                old_version_id=0,
                new_version_id=new_version_id,
                change_type='added',
                change_percent=100.0,
                added_lines=len(text.split('\n')),
                removed_lines=0,
                diff_summary=f"New page tracked: {title or url}",
                detected_at=datetime.now()
            )
            self.db.save_change(change)
            return change
        
        if prev_version.content_hash == content_hash:
            return None  # No change
        
        # Calculate diff
        diff_result = self.calculate_diff(prev_version.text_content, text)
        
        change = ContentChange(
            url=url,
            old_version_id=prev_version.id,
            new_version_id=new_version_id,
            change_type='modified',
            change_percent=diff_result['change_percent'],
            added_lines=diff_result['added'],
            removed_lines=diff_result['removed'],
            diff_summary=diff_result['summary'],
            detected_at=datetime.now()
        )
        
        self.db.save_change(change)
        return change
    
    @staticmethod
    def calculate_diff(old_text: str, new_text: str) -> dict:
        """Calculate difference between two text versions."""
        old_lines = old_text.split('\n')
        new_lines = new_text.split('\n')
        
        differ = difflib.unified_diff(old_lines, new_lines, lineterm='')
        diff_lines = list(differ)
        
        added = sum(1 for line in diff_lines if line.startswith('+') and not line.startswith('+++'))
        removed = sum(1 for line in diff_lines if line.startswith('-') and not line.startswith('---'))
        
        total_lines = max(len(old_lines), len(new_lines), 1)
        change_percent = ((added + removed) / total_lines) * 100
        
        # Generate summary
        if change_percent < 5:
            summary = "Minor text changes"
        elif change_percent < 20:
            summary = "Moderate content update"
        elif change_percent < 50:
            summary = "Significant content changes"
        else:
            summary = "Major page restructure"
        
        return {
            'added': added,
            'removed': removed,
            'change_percent': round(change_percent, 2),
            'summary': summary,
            'diff_lines': diff_lines[:100]  # Limit diff length
        }
    
    @staticmethod
    def get_html_diff(old_text: str, new_text: str) -> str:
        """Generate HTML diff for display."""
        differ = difflib.HtmlDiff()
        return differ.make_table(
            old_text.split('\n')[:100],
            new_text.split('\n')[:100],
            fromdesc='Previous',
            todesc='Current'
        )


# Global instance
_change_db: Optional[ChangeDatabase] = None


def get_change_db(db_path: Path = None) -> ChangeDatabase:
    """Get or create the global change database instance."""
    global _change_db
    if _change_db is None:
        _change_db = ChangeDatabase(db_path or Path("./data/changes.db"))
        _change_db.connect()
    return _change_db
