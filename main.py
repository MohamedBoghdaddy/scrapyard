"""
Scrapyard – CLI entry point.

Usage examples:
  python main.py --site egycarparts --format json
  python main.py --site alkhaleeg --output output/alkhaleeg --format excel
  python main.py --site egycarparts --details --format sqlite
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

import yaml
from tqdm.asyncio import tqdm

from scrapers import get_scraper
from scrapers.base import BaseScraper
from utils.storage import DataStorage

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


def _configure_logging(level: str = "INFO") -> None:
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    handlers: list[logging.Handler] = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "scrapyard.log", encoding="utf-8"),
    ]
    logging.basicConfig(level=getattr(logging, level), format=fmt, datefmt=datefmt,
                        handlers=handlers)
    # Silence noisy third-party loggers
    for lib in ("aiohttp", "asyncio", "playwright"):
        logging.getLogger(lib).setLevel(logging.WARNING)


logger = logging.getLogger("scrapyard")

KNOWN_SITES = ["egycarparts", "alkhaleeg"]

# ---------------------------------------------------------------------------
# Core async logic
# ---------------------------------------------------------------------------


async def run(args: argparse.Namespace, site_config: Dict[str, Any]) -> None:
    scraper_cls = get_scraper(args.site)
    all_products: List[Dict[str, Any]] = []

    async with scraper_cls(site_config) as scraper:
        logger.info("Scraping categories from %s …", site_config["base_url"])
        categories = await scraper.scrape_categories()

        if not categories:
            logger.error("No categories found – aborting")
            return

        logger.info("Found %d categories", len(categories))

        # Scrape products from every category
        for cat in tqdm(categories, desc="Categories", unit="cat"):
            products = await scraper.scrape_products_from_category(
                cat["url"], category_name=cat["name"]
            )
            all_products.extend(products)

        logger.info("Total products collected: %d", len(all_products))

        # Optional: enrich with full detail pages
        if args.details and all_products:
            logger.info("Fetching product detail pages …")
            enriched: List[Dict[str, Any]] = []
            for product in tqdm(all_products, desc="Details", unit="prod"):
                detail = await scraper.scrape_product_details(product["url"])
                # Merge: listing data as base, detail overrides
                merged = {**product, **detail}
                enriched.append(merged)
            all_products = enriched

    if not all_products:
        logger.warning("No data to save")
        return

    output_path = Path(args.output) / args.site
    saved = DataStorage.save(all_products, output_path, fmt=args.format)
    logger.info("Results saved to %s", saved)
    print(f"\nDone. {len(all_products)} products saved to: {saved}")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scrapyard",
        description="High-performance car parts web scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--site",
        required=True,
        choices=KNOWN_SITES,
        help="Target site identifier",
    )
    parser.add_argument(
        "--config",
        default="config/sites.yaml",
        help="Path to the YAML site configuration file",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Output directory (file name is derived from --site)",
    )
    parser.add_argument(
        "--format",
        default="json",
        choices=["csv", "json", "excel", "sqlite"],
        help="Export format",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Also scrape individual product detail pages (slower)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    _configure_logging(args.log_level)

    config_path = Path(args.config)
    if not config_path.exists():
        logger.error("Config file not found: %s", config_path)
        sys.exit(1)

    with open(config_path, encoding="utf-8") as fh:
        all_configs: Dict[str, Any] = yaml.safe_load(fh)

    if args.site not in all_configs:
        logger.error(
            "Site '%s' not found in %s. Available: %s",
            args.site, config_path, list(all_configs),
        )
        sys.exit(1)

    site_config = all_configs[args.site]

    Path(args.output).mkdir(parents=True, exist_ok=True)

    try:
        asyncio.run(run(args, site_config))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
