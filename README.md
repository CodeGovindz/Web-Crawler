<div align="center">

# SOTA Web Crawler

### A State-of-the-Art Web Crawling System

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-009688?style=for-the-badge&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Playwright](https://img.shields.io/badge/Playwright-Enabled-2EAD33?style=for-the-badge&logo=playwright&logoColor=white)](https://playwright.dev)
[![License](https://img.shields.io/badge/License-MIT-yellow?style=for-the-badge)](LICENSE)

[![Code Style](https://img.shields.io/badge/Code%20Style-Black-000000?style=flat-square)](https://github.com/psf/black)
[![Async](https://img.shields.io/badge/Async-Native-blue?style=flat-square)]()
[![WebSocket](https://img.shields.io/badge/WebSocket-Real--time-orange?style=flat-square)]()

<p align="center">
  <strong>Real-time monitoring • AI Classification • Full-text Search • Change Detection</strong>
</p>

---

</div>

## Overview

SOTA Web Crawler is a production-ready, high-performance web crawling system built with modern Python. It features real-time progress monitoring via WebSocket, intelligent content classification using AI, and a beautiful glassmorphic web interface.

## Key Features

<table>
<tr>
<td width="50%">

### Core Engine
- **Universal Compatibility** - Static HTML & JavaScript SPAs
- **Async Architecture** - High-throughput concurrent crawling
- **Smart Extraction** - Intelligent link and content parsing
- **Resilient** - Retry logic, rate limiting, error recovery

</td>
<td width="50%">

### Intelligence
- **AI Classification** - Auto-categorize into 15 categories
- **Full-text Search** - SQLite FTS5 with BM25 ranking
- **Change Detection** - Track content changes with diffs
- **Keyword Extraction** - Automatic topic identification

</td>
</tr>
<tr>
<td width="50%">

### Compliance
- **robots.txt** - Full protocol compliance
- **Sitemap Parsing** - XML sitemap support
- **Rate Limiting** - Configurable request delays
- **Politeness** - Respects crawl-delay directives

</td>
<td width="50%">

### Interface
- **Real-time Dashboard** - Live WebSocket updates
- **Modern UI** - Glassmorphic dark theme
- **REST API** - Full programmatic access
- **CLI** - Command-line interface

</td>
</tr>
</table>

## Quick Start

### Prerequisites

- Python 3.11+
- pip

### Installation

```bash
# Clone the repository
git clone https://github.com/CodeGovindz/Web-Crawler.git
cd Web-Crawler

# Install dependencies
pip install -r requirements.txt

# Install browser for JS rendering (optional)
playwright install chromium
```

### Usage

**Web Interface**
```bash
uvicorn web_api:app --reload
# Open http://localhost:8000
```

**Command Line**
```bash
# Basic crawl
python main.py crawl https://example.com --max-pages 100

# With JavaScript rendering
python main.py crawl https://spa-site.com --render

# Resume interrupted crawl
python main.py resume
```

## API Reference

| Endpoint | Method | Description |
|:---------|:-------|:------------|
| `/api/crawl` | `POST` | Start a new crawl session |
| `/api/crawl/{id}/stop` | `POST` | Stop a running crawl |
| `/api/history` | `GET` | Retrieve crawl history |
| `/api/schedules` | `GET` `POST` | Manage scheduled crawls |
| `/api/changes` | `GET` | Get content change history |
| `/api/classify/session/{id}` | `GET` | AI classification results |
| `/api/search?q=query` | `GET` | Full-text search |

## Configuration

| Option | Description | Default |
|:-------|:------------|:--------|
| `--max-pages` | Maximum pages to crawl | `1000` |
| `--max-depth` | Maximum crawl depth | `10` |
| `--delay` | Delay between requests (seconds) | `1.0` |
| `--concurrent` | Concurrent requests | `5` |
| `--render` | Enable JavaScript rendering | `False` |
| `--respect-robots` | Respect robots.txt | `True` |

## Architecture

```
Web-Crawler/
├── crawler/
│   ├── crawler.py      # Main crawl engine
│   ├── fetcher.py      # HTTP client with retry logic
│   ├── parser.py       # HTML/content parser
│   ├── frontier.py     # URL queue management
│   ├── scheduler.py    # Cron/interval scheduling
│   ├── changes.py      # Change detection system
│   ├── classifier.py   # AI content classification
│   ├── search.py       # Full-text search (FTS5)
│   └── robots.py       # robots.txt handler
├── ui/                 # Web interface (HTML/CSS/JS)
├── web_api.py          # FastAPI REST API
├── main.py             # CLI entry point
└── requirements.txt
```

## Tech Stack

| Category | Technology |
|:---------|:-----------|
| Runtime | Python 3.11+ |
| Web Framework | FastAPI |
| Async HTTP | aiohttp, httpx |
| Browser Automation | Playwright |
| Database | SQLite (FTS5) |
| Real-time | WebSocket |
| Frontend | Vanilla JS, CSS3 |

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with modern Python for reliable, intelligent web crawling**

</div>
