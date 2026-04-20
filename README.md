# Scrapyard v3

**Production-grade, async car-parts web scraper for Egyptian and international automotive sites.**

---

## Features

| Feature | Details |
|---------|---------|
| **Async-first** | `aiohttp` + `asyncio` for maximum throughput |
| **Playwright support** | JavaScript-rendered & Arabic/RTL sites via Chromium |
| **Browserless pool** | Connect to a remote Browserless instance via `PLAYWRIGHT_WS` |
| **Shopify JSON extraction** | Pulls embedded product JSON for 100% data accuracy |
| **Bilingual** | English + Arabic text, including Arabic-Indic digits |
| **Brotli support** | `auto_decompress=True` + `brotli` package; gzip fallback on error |
| **Blocking detection** | Content-based CAPTCHA/block pattern scanning with `BlockedError` |
| **Proxy & UA rotation** | Pluggable proxy pool, 12+ modern browser User-Agents |
| **Jina AI fallback** | Fetches via [r.jina.ai](https://r.jina.ai) when all HTTP retries fail |
| **LLM extraction** | `--llm` flag enables GPT-4o-mini structured extraction as a last resort |
| **Checkpointing** | SQLite-backed resume: skip already-scraped categories/products |
| **QA validation** | Products failing schema checks saved to `_invalid.csv` |
| **Multi-format export** | CSV, JSON, Excel, SQLite, PostgreSQL, MySQL |
| **Slack notifications** | Run start, completion, errors, high failure-rate alerts |
| **Metrics audit trail** | Per-request log saved to `output/run_<site>_<ts>_meta.json` |
| **Respectful scraping** | Configurable delays, exponential backoff, `max_pages` cap |
| **Docker ready** | Multi-stage Dockerfile + Compose with Browserless & Postgres |
| **Concurrency control** | `--concurrency N` limits parallel scrapes via `asyncio.Semaphore` |

---

## Supported Sites

| Key | Site | Type |
|-----|------|------|
| `egycarparts` | egycarparts.com | Shopify (aiohttp) |
| `alkhaleeg` | elkhaberstores.com | Arabic / Custom (Playwright) |

Additional sites are configured via `config/sites.yaml` — no code changes needed.

---

## Installation

### Requirements

- Python 3.10+
- pip

```bash
# 1. Clone
git clone https://github.com/MohamedBoghdaddy/scrapyard.git
cd scrapyard

# 2. Virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install Playwright browser (for Playwright-based scrapers)
playwright install chromium

# 5. Configure environment
cp .env.example .env
# Edit .env and fill in API keys / tokens you want to use
```

---

## Usage

### Basic

```bash
python main.py --site egycarparts
python main.py --site egycarparts --format csv
python main.py --site egycarparts --format excel
python main.py --site egycarparts --format sqlite
```

### Resume an interrupted scrape

```bash
python main.py --site egycarparts --resume
```

### Scrape detail pages (richer data)

```bash
python main.py --site egycarparts --details --format json
```

### Parallel scraping with concurrency control

```bash
python main.py --site egycarparts --concurrency 3
```

### Enable LLM extraction fallback

```bash
# Requires OPENAI_API_KEY in .env
python main.py --site egycarparts --llm
```

### Export to database

```bash
# Requires DATABASE_URL in .env
python main.py --site egycarparts --format postgres
python main.py --site egycarparts --format mysql
```

### Arabic site with Playwright

```bash
python main.py --site alkhaleeg --format excel
```

### Full options

```
usage: scrapyard [-h] --site {egycarparts,alkhaleeg}
                 [--config CONFIG] [--output OUTPUT]
                 [--format {csv,json,excel,sqlite,postgres,mysql}]
                 [--resume] [--concurrency N] [--details]
                 [--llm] [--max-pages N] [--ignore-ssl]
                 [--log-level {DEBUG,INFO,WARNING,ERROR}]

options:
  --site          Target site identifier (required)
  --config        Path to YAML config        (default: config/sites.yaml)
  --output        Output directory           (default: output)
  --format        Export format              (default: json)
  --resume        Skip already-scraped URLs
  --concurrency   Max concurrent fetches     (default: 5)
  --details       Scrape full product detail pages
  --llm           Enable GPT-4o-mini extraction fallback
  --max-pages     Override config max_pages  (default: use config value)
  --ignore-ssl    Disable SSL verification
  --log-level     Logging verbosity          (default: INFO)
```

---

## Configuration

Add new sites by adding a YAML block to `config/sites.yaml`:

```yaml
mysite:
  base_url: "https://example.com"
  type: "custom"              # or "shopify"
  categories_selector: "nav a"
  product_container: "div.product"
  product_link: "a"
  product_title: "h2"
  price_selector: ".price"
  vendor_selector: ".brand"
  next_page: "a.next"
  use_javascript: false
  request_delay_min: 1.0
  request_delay_max: 3.0
  max_retries: 3
  timeout: 30
  max_pages: 10
```

Then implement a scraper class inheriting from `scrapers.base.BaseScraper` and register it in `scrapers/__init__.py`.

---

## Docker Deployment

```bash
# From project root
cp .env.example .env  # fill in tokens

# Build and start all services (Scrapyard + Browserless + Postgres)
docker compose -f docker/docker-compose.yml up --build

# Run a specific site
docker compose -f docker/docker-compose.yml run scrapyard \
    --site egycarparts --format postgres
```

**Services:**

| Service | Purpose |
|---------|---------|
| `scrapyard` | Scraper application |
| `browserless` | Remote Chromium pool (connects via `PLAYWRIGHT_WS`) |
| `postgres` | Optional database for `--format postgres` |

---

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `JINA_API_KEY` | Jina AI HTTP fallback (after all retries fail) |
| `OPENAI_API_KEY` | GPT-4o-mini LLM extraction (`--llm` flag) |
| `SLACK_WEBHOOK_URL` | Slack notifications for run events |
| `BROWSERLESS_TOKEN` | Auth token for Browserless service |
| `PLAYWRIGHT_WS` | WebSocket URL of Browserless instance |
| `DATABASE_URL` | PostgreSQL or MySQL connection string |
| `PROXY_LIST` | Comma-separated proxy URLs |

---

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| `name` | str | Product name |
| `url` | str | Product page URL |
| `price` | float | Parsed numeric price |
| `raw_price` | str | Original price string |
| `vendor` | str | Brand / manufacturer |
| `part_number` | str | OEM or SKU reference |
| `image_url` | str | Main product image |
| `stock_status` | str | `in_stock` / `out_of_stock` / `unknown` |
| `category` | str | Parent category name |
| `source` | str | Scraper identifier |
| `description`* | str | Full product description |
| `specifications`* | dict | Key-value spec table |
| `variants`* | list | Size/variant options |

\* Only present with `--details`.

---

## Project Structure

```
scrapyard/
├── config/
│   └── sites.yaml          # Site-specific CSS selectors & settings
├── db/
│   ├── checkpoint.py       # SQLite/Postgres resume-state manager
│   └── models.py           # SQL DDL for all backends
├── docker/
│   ├── Dockerfile          # Multi-stage Python 3.11 image
│   └── docker-compose.yml  # Scrapyard + Browserless + Postgres
├── notifiers/
│   └── slack.py            # Slack webhook notifier
├── scrapers/
│   ├── base.py             # Abstract base class
│   ├── egycarparts.py      # Shopify / aiohttp scraper
│   ├── alkhaleeg.py        # Arabic / Playwright scraper
│   └── utils.py            # Shared scraping helpers
├── utils/
│   ├── cleaners.py         # Text / price / URL cleaning + first_match()
│   ├── jina.py             # Jina AI HTTP fallback
│   ├── llm_extractor.py    # GPT-4o-mini structured extraction
│   ├── metrics.py          # Per-request audit trail + summary
│   ├── proxies.py          # Proxy pool manager
│   ├── storage.py          # Multi-format export + QA validation
│   └── user_agents.py      # UA rotation pool
├── logs/
├── output/
├── .env.example
├── main.py                 # CLI entry point
└── requirements.txt
```

---

## Legal Disclaimer

This tool is for **educational and authorised research purposes only**.

- Always check a site's `robots.txt` before scraping.
- Respect `Crawl-delay` and rate-limit directives.
- Obtain permission from site owners before scraping at scale.
- Do not scrape personal data or copyrighted content without authorisation.
- The authors accept no liability for misuse of this software.

---

## License

MIT
