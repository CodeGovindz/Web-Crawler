# 🕷️ SOTA Web Crawler

A **state-of-the-art web crawling system** with real-time monitoring, AI classification, and full-text search.

![Python](https://img.shields.io/badge/Python-3.11+-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green)
![License](https://img.shields.io/badge/License-MIT-yellow)

## ✨ Features

### Core Crawler
- **Universal Compatibility**: Static HTML + JavaScript-rendered SPAs
- **Intelligent Crawling**: robots.txt compliance, sitemap parsing, smart link extraction
- **High Performance**: Async I/O, concurrent crawling, request deduplication
- **Robust**: Retry logic, rate limiting, graceful error handling

### Web Interface
- **Real-time Dashboard**: Live crawl progress with WebSocket updates
- **Crawl History**: View and manage past crawl sessions
- **Modern UI**: Glassmorphic design with dark mode

### Advanced Features
- ⏰ **Scheduling**: Cron/interval-based recurring crawls
- 🔄 **Change Detection**: Track page changes with diff visualization
- 🤖 **AI Classification**: Auto-categorize pages (15 categories)
- 🔍 **Full-Text Search**: SQLite FTS5 with highlighted results

## 🚀 Quick Start

### Installation
```bash
git clone https://github.com/CodeGovindz/Web-Crawler.git
cd Web-Crawler
pip install -r requirements.txt
playwright install chromium
```

### Run Web UI
```bash
uvicorn web_api:app --reload
```
Open http://localhost:8000

### CLI Usage
```bash
# Basic crawl
python main.py crawl https://example.com --max-pages 100

# JavaScript rendering
python main.py crawl https://spa-site.com --render

# Resume interrupted crawl
python main.py resume
```

## 🌐 Deploy to Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template/fastapi)

### Manual Deployment:
1. Go to [railway.app](https://railway.app) and sign in with GitHub
2. Click **"New Project"** → **"Deploy from GitHub repo"**
3. Select **CodeGovindz/Web-Crawler**
4. Railway auto-detects the config and deploys!
5. Click **"Generate Domain"** to get your public URL

The project includes:
- `Procfile` - Start command
- `runtime.txt` - Python 3.11
- `nixpacks.toml` - Build config
- `railway.json` - Deploy settings

## 📋 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/crawl` | POST | Start a new crawl |
| `/api/crawl/{id}/stop` | POST | Stop a running crawl |
| `/api/history` | GET | Get crawl history |
| `/api/schedules` | GET/POST | Manage schedules |
| `/api/changes` | GET | Recent content changes |
| `/api/classify/session/{id}` | GET | AI classify session |
| `/api/search?q=query` | GET | Full-text search |

## ⚙️ Configuration

| Flag | Description | Default |
|------|-------------|---------|
| `--max-pages` | Maximum pages to crawl | 1000 |
| `--max-depth` | Maximum crawl depth | 10 |
| `--delay` | Delay between requests | 1.0s |
| `--render` | Enable JS rendering | False |
| `--concurrent` | Concurrent requests | 5 |

## 📁 Project Structure

```
Web-Crawler/
├── crawler/          # Core crawler modules
│   ├── crawler.py    # Main crawler engine
│   ├── scheduler.py  # Cron/interval scheduling
│   ├── changes.py    # Change detection
│   ├── classifier.py # AI classification
│   └── search.py     # Full-text search
├── ui/               # Web interface
├── web_api.py        # FastAPI backend
└── main.py           # CLI entrypoint
```

## 📄 License

MIT

