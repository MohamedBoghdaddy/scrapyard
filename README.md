# Scrapyard v3.1

Async car-parts scraper for Egyptian and regional storefronts, with both HTTP
and Playwright engines, config-driven site onboarding, and export formats for
CSV, JSON, Excel, SQLite, PostgreSQL, and MySQL.

## Highlights

- Config-driven site support in `config/sites.yaml`
- HTTP scraper for plain HTML, Shopify-style, WooCommerce-style, and catalog pages
- Playwright scraper for JS-heavy, Arabic, Wix, Next.js, and widget catalogs
- Automatic Shopify JSON detection on product pages, even when a site is marked `type: custom`
- Seeded categories for sites where menus are unreliable or not discoverable
- Listing-only support for fragment-based catalogs such as `a-part.com`
- QA validation with `_invalid.csv` output for malformed rows
- Optional detail-page enrichment with `--details`
- Multi-sheet Excel export designed to be easier for LLM workflows

## Supported Site IDs

The current live config includes these site ids:

| Site ID | Website | Engine | Notes |
|---|---|---|---|
| `tawfiqia` | `tawfiqia.com` | HTTP | Standard custom storefront |
| `elcatalog` | `el-catalog.store` | HTTP | Shopify-like, JSON auto-detection supported |
| `autospare` | `autospare.com.eg` | HTTP | Custom storefront |
| `pringi` | `pringi.com` | HTTP | WooCommerce/custom, not treated as Shopify by config |
| `fitandfix` | `fitandfix.com` | Playwright | Seeded category preserved |
| `a-part` | `a-part.com` | Playwright | Fragment-based catalog, listing-only support |
| `apart` | alias of `a-part` | Playwright | Backwards-compatible alias |
| `a_part` | alias of `a-part` | Playwright | Backwards-compatible alias |
| `yourparts` | `yourparts.com` | Playwright | JS-rendered product catalog |
| `elkhaberstores` | `elkhaberstores.com` | Playwright | Arabic storefront |
| `alkhaleeg` | alias of `elkhaberstores` | Playwright | Backwards-compatible alias |
| `egycarparts` | `egycarparts.com` | HTTP | Native Shopify storefront |
| `partfinderegypt` | `partfinderegypt.com` | HTTP | Inventory lookup with seeded brand pages |
| `elboltygroup` | `elboltygroup.com` | Playwright | Wix storefront with seeded shop page |

## Installation

```bash
git clone https://github.com/MohamedBoghdaddy/scrapyard.git
cd scrapyard

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
playwright install chromium
```

Optional environment variables:

- `DATABASE_URL` for `postgres` / `mysql`
- `OPENAI_API_KEY` for `--llm`
- `SLACK_WEBHOOK_URL` for notifications
- `PLAYWRIGHT_WS` for Browserless / remote Chromium
- `PROXY_LIST` for HTTP proxy rotation
- `JINA_API_KEY` for Jina fallback

## Usage

Basic examples:

```bash
python main.py --site egycarparts --format json
python main.py --site a-part --format csv --max-pages 1
python main.py --site elkhaberstores --format excel
python main.py --site partfinderegypt --format csv --max-pages 1
```

Resume a run:

```bash
python main.py --site egycarparts --resume
```

Fetch detail pages:

```bash
python main.py --site elcatalog --details --format json
```

Enable LLM fallback:

```bash
python main.py --site tawfiqia --llm
```

## Configuration

All site behavior lives in `config/sites.yaml`.

Minimal template:

```yaml
mysite:
  display_name: "My Site"
  base_url: "https://example.com"
  type: "custom"
  platform_type: "Custom"
  currency: "EGP"
  engine: "http"               # or "playwright"
  categories_selector: 'a[href*="/category/"]'
  category_link_patterns:
    - "/category/"
  product_container: "div.product"
  product_link: "a"
  product_link_patterns:
    - "/product/"
  product_title: "h2, h3"
  part_number_selector: ".sku"
  price_selector: ".price"
  vendor_selector: ".brand"
  next_page: "a[rel='next']"
  use_javascript: false
  max_pages: 10
  notes: "Site-specific behavior goes here."
```

### Seeded Categories

Some sites do not expose a reliable crawlable category tree. For those, keep
manual seeds in YAML:

```yaml
seed_categories:
  - name: "Spare Parts"
    url: "https://example.com/categories/spare-parts"
```

Scrapyard also accepts the older `categories:` key for backwards compatibility,
but `seed_categories:` is the preferred name.

Current seeded sites:

- `fitandfix`
- `a-part`
- `elkhaberstores`
- `partfinderegypt`
- `elboltygroup`

## Site-Specific Notes

### `a-part.com`

- The catalogue uses fragment-based references such as `#material-...`
- Real product detail URLs are not exposed as normal links
- Scrapyard builds synthetic URLs and preserves listing data instead of forcing a broken detail-page crawl
- Part numbers are extracted from `data-material-id`

### `partfinderegypt.com`

- This behaves like an inventory lookup by brand/model
- Seeded brand pages are the reliable entry points
- The first level behaves more like brand indexes than normal e-commerce categories

### `pringi.com`

- This is handled as a generic custom/WooCommerce-style site
- It is not assumed to be Shopify
- If a page ever exposes Shopify JSON anyway, the hybrid scraper will use it automatically

### Shopify Auto-Detection

Product detail parsing checks for Shopify-style embedded JSON on every product
page. That means:

- `type: shopify` still works as expected
- `type: custom` can still benefit from Shopify JSON when it is actually present
- Sites such as `elcatalog` can stay config-driven without hard-coding platform assumptions into the scraper

## Excel Export

`--format excel` now writes a multi-sheet workbook intended to be easier to use
for downstream analysis and LLM workflows.

Sheets:

- `products`: flat primary table
- `vendors`: vendor lookup table
- `compatibility`: structured fitment rows when compatibility data exists
- `categories`: unique category list
- `scrape_metadata`: run-level audit metadata

Important `products` columns:

- `product_id`
- `part_name`
- `part_number`
- `brand`
- `category`
- `price_egp`
- `price_raw`
- `vendor_name`
- `vendor_id`
- `stock_status`
- `product_url`
- `image_url`
- `scraped_at`
- `description`
- `specifications`
- `compatibility_text`
- `oem_references`
- `notes`

## Output Formats

- `csv`: flat row export
- `json`: raw structured output
- `excel`: multi-sheet workbook
- `sqlite`: local SQLite database
- `postgres`: PostgreSQL upsert
- `mysql`: MySQL insert/upsert flow

## Development Notes

Main code paths:

- `main.py`: CLI entry point and orchestration
- `config/sites.yaml`: site configuration
- `scrapers/base.py`: shared helpers, seeded categories, fragment handling
- `scrapers/egycarparts.py`: HTTP hybrid scraper
- `scrapers/alkhaleeg.py`: Playwright scraper for JS-heavy sites
- `utils/storage.py`: file/database export logic

When onboarding a new site:

1. Add a YAML entry with selectors and notes
2. Decide whether it should use `engine: http` or `engine: playwright`
3. Add `seed_categories` if navigation is unreliable
4. Add `extract_from_listing: true` if the listing card is the best source of truth
5. Run `python main.py --site <site-id> --max-pages 1 --format csv`

## Verification

Typical smoke test:

```bash
python main.py --site a-part --max-pages 1 --format csv
python main.py --site elcatalog --details --max-pages 1 --format json
python main.py --site partfinderegypt --max-pages 1 --format csv
```
