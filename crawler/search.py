"""
Full-Text Search - Search across all crawled content.

Features:
- SQLite FTS5 full-text search (no external dependencies)
- Ranked search results
- Highlighted snippets
- Filters by session, date, domain
- Real-time indexing during crawl

Uses SQLite FTS5 for fast, portable full-text search without
requiring Elasticsearch or other external services.
"""

import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field


@dataclass
class SearchResult:
    """A single search result."""
    id: int
    url: str
    title: str
    snippet: str
    score: float
    session_id: Optional[int] = None
    crawled_at: Optional[str] = None
    word_count: int = 0


@dataclass
class SearchResponse:
    """Search response with results and metadata."""
    query: str
    total_results: int
    results: list
    search_time_ms: float
    page: int = 1
    per_page: int = 20


class SearchIndex:
    """
    Full-text search index using SQLite FTS5.
    Provides fast, portable search without external dependencies.
    """
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
    
    def connect(self) -> None:
        """Initialize database and create FTS tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        
        # Create main content table
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS indexed_pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                content TEXT,
                description TEXT,
                domain TEXT,
                session_id INTEGER,
                word_count INTEGER DEFAULT 0,
                crawled_at TEXT DEFAULT CURRENT_TIMESTAMP,
                indexed_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX IF NOT EXISTS idx_pages_domain ON indexed_pages(domain);
            CREATE INDEX IF NOT EXISTS idx_pages_session ON indexed_pages(session_id);
        """)
        
        # Create FTS5 virtual table for full-text search
        try:
            self._conn.execute("""
                CREATE VIRTUAL TABLE IF NOT EXISTS pages_fts USING fts5(
                    url,
                    title,
                    content,
                    description,
                    content='indexed_pages',
                    content_rowid='id',
                    tokenize='porter unicode61'
                )
            """)
        except sqlite3.OperationalError:
            # FTS table already exists
            pass
        
        # Create triggers to keep FTS in sync
        self._conn.executescript("""
            CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON indexed_pages BEGIN
                INSERT INTO pages_fts(rowid, url, title, content, description)
                VALUES (new.id, new.url, new.title, new.content, new.description);
            END;
            
            CREATE TRIGGER IF NOT EXISTS pages_ad AFTER DELETE ON indexed_pages BEGIN
                INSERT INTO pages_fts(pages_fts, rowid, url, title, content, description)
                VALUES ('delete', old.id, old.url, old.title, old.content, old.description);
            END;
            
            CREATE TRIGGER IF NOT EXISTS pages_au AFTER UPDATE ON indexed_pages BEGIN
                INSERT INTO pages_fts(pages_fts, rowid, url, title, content, description)
                VALUES ('delete', old.id, old.url, old.title, old.content, old.description);
                INSERT INTO pages_fts(rowid, url, title, content, description)
                VALUES (new.id, new.url, new.title, new.content, new.description);
            END;
        """)
        
        self._conn.commit()
    
    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def index_page(self, url: str, title: str, content: str,
                   description: str = "", session_id: int = None) -> int:
        """
        Add or update a page in the search index.
        Returns the page ID.
        """
        # Extract domain from URL
        domain = ""
        try:
            from urllib.parse import urlparse
            domain = urlparse(url).netloc
        except Exception:
            pass
        
        word_count = len(content.split())
        
        try:
            cursor = self._conn.execute("""
                INSERT INTO indexed_pages 
                (url, title, content, description, domain, session_id, word_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET
                    title = excluded.title,
                    content = excluded.content,
                    description = excluded.description,
                    session_id = excluded.session_id,
                    word_count = excluded.word_count,
                    indexed_at = CURRENT_TIMESTAMP
            """, (url, title, content[:50000], description[:1000], 
                  domain, session_id, word_count))
            self._conn.commit()
            return cursor.lastrowid
        except Exception as e:
            print(f"Error indexing {url}: {e}")
            return 0
    
    def search(self, query: str, page: int = 1, per_page: int = 20,
               session_id: int = None, domain: str = None) -> SearchResponse:
        """
        Search indexed pages using full-text search.
        
        Args:
            query: Search query (supports FTS5 syntax)
            page: Page number (1-indexed)
            per_page: Results per page
            session_id: Optional filter by crawl session
            domain: Optional filter by domain
        
        Returns:
            SearchResponse with ranked results
        """
        import time
        start_time = time.time()
        
        # Clean query for FTS5
        clean_query = self._clean_query(query)
        if not clean_query:
            return SearchResponse(
                query=query, total_results=0, results=[],
                search_time_ms=0, page=page, per_page=per_page
            )
        
        offset = (page - 1) * per_page
        
        # Build query with optional filters
        params = [clean_query]
        filter_sql = ""
        
        if session_id:
            filter_sql += " AND p.session_id = ?"
            params.append(session_id)
        
        if domain:
            filter_sql += " AND p.domain LIKE ?"
            params.append(f"%{domain}%")
        
        # Search with BM25 ranking
        try:
            # Count total results
            count_sql = f"""
                SELECT COUNT(*) FROM pages_fts f
                JOIN indexed_pages p ON f.rowid = p.id
                WHERE pages_fts MATCH ?
                {filter_sql}
            """
            total = self._conn.execute(count_sql, params).fetchone()[0]
            
            # Get paginated results with snippets
            search_sql = f"""
                SELECT 
                    p.id, p.url, p.title, p.word_count, p.session_id, p.crawled_at,
                    snippet(pages_fts, 2, '<mark>', '</mark>', '...', 40) as snippet,
                    bm25(pages_fts) as score
                FROM pages_fts f
                JOIN indexed_pages p ON f.rowid = p.id
                WHERE pages_fts MATCH ?
                {filter_sql}
                ORDER BY score
                LIMIT ? OFFSET ?
            """
            params.extend([per_page, offset])
            
            cursor = self._conn.execute(search_sql, params)
            rows = cursor.fetchall()
            
            results = [
                SearchResult(
                    id=row['id'],
                    url=row['url'],
                    title=row['title'] or 'Untitled',
                    snippet=row['snippet'] or '',
                    score=abs(row['score']),  # BM25 returns negative scores
                    session_id=row['session_id'],
                    crawled_at=row['crawled_at'],
                    word_count=row['word_count']
                )
                for row in rows
            ]
            
        except sqlite3.OperationalError as e:
            # Handle query syntax errors
            print(f"Search error: {e}")
            total = 0
            results = []
        
        search_time = (time.time() - start_time) * 1000
        
        return SearchResponse(
            query=query,
            total_results=total,
            results=results,
            search_time_ms=round(search_time, 2),
            page=page,
            per_page=per_page
        )
    
    def _clean_query(self, query: str) -> str:
        """Clean and prepare query for FTS5."""
        # Remove special characters that break FTS5
        query = re.sub(r'[^\w\s"*-]', ' ', query)
        query = ' '.join(query.split())  # Normalize whitespace
        
        # If simple query, add prefix matching
        if query and not any(c in query for c in ['"', '*', 'AND', 'OR', 'NOT']):
            # Add prefix matching for better results
            terms = query.split()
            query = ' '.join(f'{term}*' for term in terms if term)
        
        return query
    
    def get_stats(self) -> dict:
        """Get search index statistics."""
        stats = {
            "total_pages": 0,
            "total_words": 0,
            "domains": [],
            "sessions": []
        }
        
        try:
            # Total pages
            stats["total_pages"] = self._conn.execute(
                "SELECT COUNT(*) FROM indexed_pages"
            ).fetchone()[0]
            
            # Total words
            result = self._conn.execute(
                "SELECT SUM(word_count) FROM indexed_pages"
            ).fetchone()[0]
            stats["total_words"] = result or 0
            
            # Top domains
            cursor = self._conn.execute("""
                SELECT domain, COUNT(*) as count
                FROM indexed_pages
                WHERE domain != ''
                GROUP BY domain
                ORDER BY count DESC
                LIMIT 10
            """)
            stats["domains"] = [
                {"domain": row["domain"], "count": row["count"]}
                for row in cursor.fetchall()
            ]
            
            # Sessions
            cursor = self._conn.execute("""
                SELECT session_id, COUNT(*) as count
                FROM indexed_pages
                WHERE session_id IS NOT NULL
                GROUP BY session_id
                ORDER BY session_id DESC
                LIMIT 10
            """)
            stats["sessions"] = [
                {"session_id": row["session_id"], "count": row["count"]}
                for row in cursor.fetchall()
            ]
            
        except Exception as e:
            print(f"Stats error: {e}")
        
        return stats
    
    def index_session(self, session_id: int, data_dir: Path) -> int:
        """
        Index all content from a crawl session.
        Returns number of pages indexed.
        """
        import json
        
        content_file = data_dir / f"content_{session_id}.jsonl"
        if not content_file.exists():
            return 0
        
        count = 0
        with open(content_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    try:
                        record = json.loads(line)
                        self.index_page(
                            url=record.get('url', ''),
                            title=record.get('title', ''),
                            content=record.get('text', ''),
                            description=record.get('description', ''),
                            session_id=session_id
                        )
                        count += 1
                    except json.JSONDecodeError:
                        pass
        
        return count
    
    def delete_session(self, session_id: int) -> int:
        """Delete all indexed pages from a session."""
        cursor = self._conn.execute(
            "DELETE FROM indexed_pages WHERE session_id = ?",
            (session_id,)
        )
        self._conn.commit()
        return cursor.rowcount


# Global search index instance
_search_index: Optional[SearchIndex] = None


def get_search_index(db_path: Path = None) -> SearchIndex:
    """Get or create the global search index instance."""
    global _search_index
    if _search_index is None:
        _search_index = SearchIndex(db_path or Path("./data/search.db"))
        _search_index.connect()
    return _search_index
