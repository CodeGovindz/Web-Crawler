# State-of-the-Art Web Crawler

A robust, scalable, and intelligent web crawling system designed to work optimally for any website.

## Features

- **Universal Compatibility**: Static HTML + JavaScript-rendered SPAs
- **Intelligent Crawling**: robots.txt compliance, sitemap parsing, smart link extraction
- **Anti-Bot Resilience**: User-Agent rotation, proxy support, human-like behavior
- **High Performance**: Async I/O, concurrent crawling, request deduplication
- **Robust**: Retry logic, rate limiting, graceful error handling, resumable crawls

## Installation

```bash
pip install -r requirements.txt
playwright install chromium
```

## Usage

### Basic Crawl
```bash
python main.py crawl https://example.com --max-pages 100
```

### JavaScript-Rendered Sites
```bash
python main.py crawl https://spa-website.com --render --max-pages 50
```

### Resume Interrupted Crawl
```bash
python main.py resume
```

### Export Data
```bash
python main.py export --format json --output ./output
```

## Configuration

See `crawler/config.py` for available options or use CLI flags:

| Flag | Description | Default |
|------|-------------|---------|
| `--max-pages` | Maximum pages to crawl | 1000 |
| `--max-depth` | Maximum crawl depth | 10 |
| `--delay` | Delay between requests (seconds) | 1.0 |
| `--render` | Enable JavaScript rendering | False |
| `--respect-robots` | Respect robots.txt | True |
| `--output` | Output directory | ./data |

## License

MIT
