# Scrapyard

A high-performance, asynchronous web scraper for automotive parts catalogs across Egyptian and international vendors.

---

## Features

- **Async-first** — built on `aiohttp` + `asyncio` for maximum throughput
- **Playwright support** — handles JavaScript-rendered and Arabic content automatically
- **Shopify JSON extraction** — pulls embedded product JSON for 100% data accuracy on Shopify stores
- **Bilingual** — handles both English and Arabic text (including Arabic-Indic digits)
- **Proxy & User-Agent rotation** — pluggable proxy pool, realistic browser headers
- **Multiple export formats** — CSV, JSON, Excel, SQLite
- **Respectful scraping** — configurable delays, exponential backoff on failures
- **Progress bars** — real-time feedback via `tqdm`
- **Structured logging** — console + file handler at configurable verbosity

---

## Supported Sites

| Key | Site | Type |
|-----|------|------|
| `egycarparts` | egycarparts.com | Shopify (aiohttp) |
| `alkhaleeg` | example-alkhaleeg.com | Custom / WooCommerce (Playwright) |

---

## Installation

### Prerequisites

- Python 3.10+
- pip

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/MohamedBoghdaddy/scrapyard.git
cd scrapyard

# 2. Create and activate a virtual environment
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Install Playwright browsers (only needed for Playwright-based scrapers)
playwright install chromium
```

---

## Usage

### Basic – JSON output (default)

```bash
python main.py --site egycarparts
```

### Choose output format

```bash
python main.py --site egycarparts --format csv
python main.py --site egycarparts --format excel
python main.py --site egycarparts --format sqlite
```

### Also scrape full product detail pages

```bash
python main.py --site egycarparts --details --format json
```

### Arabic site with Playwright

```bash
python main.py --site alkhaleeg --format excel
```

### Custom output directory and log level

```bash
python main.py --site egycarparts --output data/run1 --log-level DEBUG
```

### Full options

```
usage: scrapyard [-h] --site {egycarparts,alkhaleeg}
                 [--config CONFIG] [--output OUTPUT]
                 [--format {csv,json,excel,sqlite}]
                 [--details] [--log-level {DEBUG,INFO,WARNING,ERROR}]

options:
  --site          Target site identifier (required)
  --config        Path to YAML config (default: config/sites.yaml)
  --output        Output directory    (default: output)
  --format        Export format       (default: json)
  --details       Scrape product detail pages too
  --log-level     Logging verbosity   (default: INFO)
```

---

## Configuration

Site-specific selectors live in `config/sites.yaml`:

```yaml
egycarparts:
  base_url: "https://egycarparts.com"
  type: "shopify"
  categories_selector: "nav ul li a"
  product_container: "ul.product-grid li, div.product-item, div.product-card"
  product_link: "a"
  product_title: ".product-title, h3"
  price_selector: ".price, .product-price"
  vendor_selector: ".vendor, .brand"
  next_page: "a.pagination__next"
  use_javascript: false
  request_delay_min: 1.0
  request_delay_max: 3.0
  max_retries: 3
  timeout: 30
```

To add a new site, add a new YAML block and create a corresponding scraper class
that inherits from `scrapers.base.BaseScraper`.

---

## Proxy Configuration

Set the `PROXY_LIST` environment variable with a comma-separated list of proxies:

```bash
export PROXY_LIST="http://user:pass@host1:3128,http://user:pass@host2:3128"
python main.py --site egycarparts
```

When `PROXY_LIST` is empty, the scraper runs without a proxy.

---

## Output Schema

Each product record contains:

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
| `variants`* | list | Size/colour variants |

\* Only present when `--details` flag is used.

---

## Project Structure

```
scrapyard/
├── config/
│   └── sites.yaml          # Site-specific CSS selectors & settings
├── scrapers/
│   ├── __init__.py
│   ├── base.py             # Abstract base class
│   ├── egycarparts.py      # Shopify / aiohttp scraper
│   ├── alkhaleeg.py        # Arabic / Playwright scraper
│   └── utils.py            # Shared scraping helpers
├── utils/
│   ├── __init__.py
│   ├── cleaners.py         # Text / price / URL cleaning
│   ├── proxies.py          # Proxy pool manager
│   ├── storage.py          # Multi-format export
│   └── user_agents.py      # UA rotation pool
├── logs/                   # Runtime log files
├── output/                 # Scraped data (gitignored)
├── main.py                 # CLI entry point
├── requirements.txt
└── README.md
```

---

## Legal Disclaimer

This tool is provided for **educational and research purposes only**.

- Always check a site's `robots.txt` before scraping.
- Respect `Crawl-delay` directives and rate-limit settings.
- Do not scrape personal data or content protected by copyright without permission.
- The authors accept no liability for misuse of this software.

---

## License

MIT
