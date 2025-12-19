#!/usr/bin/env python3
"""
State-of-the-Art Web Crawler - CLI Interface

Usage:
    python main.py crawl <url> [options]
    python main.py resume
    python main.py export [options]
    python main.py stats
"""

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from crawler.config import CrawlerConfig
from crawler.crawler import Crawler

app = typer.Typer(
    name="sota-crawler",
    help="State-of-the-Art Web Crawler - Crawl any website optimally",
    add_completion=False
)

console = Console()


@app.command()
def crawl(
    url: str = typer.Argument(..., help="Starting URL to crawl"),
    max_pages: int = typer.Option(100, "--max-pages", "-n", help="Maximum pages to crawl"),
    max_depth: int = typer.Option(10, "--max-depth", "-d", help="Maximum crawl depth"),
    delay: float = typer.Option(1.0, "--delay", help="Delay between requests (seconds)"),
    render: bool = typer.Option(False, "--render", "-r", help="Enable JavaScript rendering"),
    respect_robots: bool = typer.Option(True, "--respect-robots/--ignore-robots", help="Respect robots.txt"),
    output: Path = typer.Option(Path("./data"), "--output", "-o", help="Output directory"),
    concurrent: int = typer.Option(5, "--concurrent", "-c", help="Concurrent requests"),
    user_agent: Optional[str] = typer.Option(None, "--user-agent", "-ua", help="Custom User-Agent"),
    proxy: Optional[str] = typer.Option(None, "--proxy", "-p", help="Proxy URL"),
    save_html: bool = typer.Option(True, "--save-html/--no-html", help="Save raw HTML"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
):
    """
    Crawl a website starting from the given URL.
    
    Examples:
        python main.py crawl https://example.com
        python main.py crawl https://spa-site.com --render --max-pages 50
        python main.py crawl https://example.com -n 1000 -c 10 -o ./output
    """
    # Validate URL
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Create config
    config = CrawlerConfig(
        max_pages=max_pages,
        max_depth=max_depth,
        delay_min=delay * 0.5,
        delay_max=delay * 1.5,
        enable_rendering=render,
        respect_robots_txt=respect_robots,
        output_dir=output,
        concurrent_requests=concurrent,
        user_agent=user_agent,
        proxy_url=proxy,
        save_html=save_html,
        log_level="DEBUG" if verbose else "INFO"
    )
    
    # Ensure output directory exists
    config.output_dir.mkdir(parents=True, exist_ok=True)
    config.db_path = config.output_dir / "crawler.db"
    
    console.print(f"\n[bold]Configuration:[/bold]")
    console.print(f"  Max pages: {max_pages}")
    console.print(f"  Max depth: {max_depth}")
    console.print(f"  Concurrent: {concurrent}")
    console.print(f"  JS Rendering: {render}")
    console.print(f"  Output: {output}")
    
    # Run crawler
    crawler = Crawler(config)
    
    try:
        asyncio.run(crawler.crawl(url))
    except KeyboardInterrupt:
        console.print("\n[yellow]Crawl interrupted by user[/yellow]")


@app.command()
def resume(
    output: Path = typer.Option(Path("./data"), "--output", "-o", help="Output directory"),
):
    """
    Resume the last interrupted crawl session.
    """
    config = CrawlerConfig(output_dir=output)
    config.db_path = config.output_dir / "crawler.db"
    
    if not config.db_path.exists():
        console.print("[red]No previous crawl session found[/red]")
        raise typer.Exit(1)
    
    crawler = Crawler(config)
    
    try:
        asyncio.run(crawler.crawl("", resume=True))
    except KeyboardInterrupt:
        console.print("\n[yellow]Crawl interrupted by user[/yellow]")


@app.command()
def export(
    output: Path = typer.Option(Path("./data"), "--output", "-o", help="Output directory"),
    format: str = typer.Option("json", "--format", "-f", help="Export format (json, csv)"),
    dest: Optional[Path] = typer.Option(None, "--dest", help="Destination file"),
):
    """
    Export crawled data to a file.
    """
    from crawler.storage import ContentStorage
    
    # Find content file
    content_files = list(output.glob("content_*.jsonl"))
    if not content_files:
        console.print("[red]No crawl data found[/red]")
        raise typer.Exit(1)
    
    # Use latest
    content_file = sorted(content_files)[-1]
    session_id = int(content_file.stem.split('_')[1])
    
    storage = ContentStorage(output)
    storage._content_file = content_file
    
    if dest is None:
        dest = output / f"export_{session_id}.{format}"
    
    if format == "json":
        count = asyncio.run(storage.export_to_json(dest))
    elif format == "csv":
        fields = ['url', 'title', 'description', 'crawled_at']
        count = asyncio.run(storage.export_to_csv(dest, fields))
    else:
        console.print(f"[red]Unknown format: {format}[/red]")
        raise typer.Exit(1)
    
    console.print(f"[green]Exported {count} records to {dest}[/green]")


@app.command()
def stats(
    output: Path = typer.Option(Path("./data"), "--output", "-o", help="Output directory"),
):
    """
    Show statistics from the last crawl.
    """
    import sqlite3
    
    db_path = output / "crawler.db"
    if not db_path.exists():
        console.print("[red]No crawl database found[/red]")
        raise typer.Exit(1)
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    
    # Get latest session
    cursor = conn.execute(
        "SELECT * FROM crawl_sessions ORDER BY id DESC LIMIT 1"
    )
    session = cursor.fetchone()
    
    if not session:
        console.print("[red]No crawl sessions found[/red]")
        raise typer.Exit(1)
    
    # Get URL stats
    cursor = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending
        FROM urls WHERE session_id = ?
    """, (session['id'],))
    url_stats = cursor.fetchone()
    
    # Display
    table = Table(title=f"Crawl Session #{session['id']}")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Seed URL", session['seed_url'])
    table.add_row("Status", session['status'])
    table.add_row("Started", session['started_at'])
    table.add_row("Completed", session['completed_at'] or "N/A")
    table.add_row("", "")
    table.add_row("Total URLs", str(url_stats['total']))
    table.add_row("Completed", str(url_stats['completed']))
    table.add_row("Failed", str(url_stats['failed']))
    table.add_row("Pending", str(url_stats['pending']))
    
    console.print(table)
    
    conn.close()


@app.command()
def clean(
    output: Path = typer.Option(Path("./data"), "--output", "-o", help="Output directory"),
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
):
    """
    Clean all crawl data.
    """
    if not confirm:
        confirm = typer.confirm("This will delete all crawl data. Continue?")
    
    if not confirm:
        raise typer.Abort()
    
    import shutil
    
    if output.exists():
        shutil.rmtree(output)
        console.print(f"[green]Cleaned {output}[/green]")
    else:
        console.print("[yellow]Nothing to clean[/yellow]")


if __name__ == "__main__":
    app()
