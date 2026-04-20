"""
Scrapyard v3 – CLI entry point.

Usage examples:
  python main.py --site egycarparts --format json
  python main.py --site egycarparts --format csv --resume
  python main.py --site alkhaleeg --output output/alkhaleeg --format excel
  python main.py --site egycarparts --details --concurrency 3 --llm
  python main.py --site egycarparts --format postgres   # requires DATABASE_URL
  python main.py --site egycarparts --format mysql      # requires DATABASE_URL
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from tqdm.asyncio import tqdm

from scrapers import get_scraper
from utils.metrics import MetricsCollector
from utils.storage import DataStorage
from notifiers.slack import SlackNotifier
from db.checkpoint import CheckpointManager

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
    logging.basicConfig(
        level=getattr(logging, level),
        format=fmt,
        datefmt=datefmt,
        handlers=handlers,
    )
    for lib in ("aiohttp", "asyncio", "playwright"):
        logging.getLogger(lib).setLevel(logging.WARNING)


logger = logging.getLogger("scrapyard")

# ---------------------------------------------------------------------------
# Core async logic
# ---------------------------------------------------------------------------


async def run(args: argparse.Namespace, site_config: Dict[str, Any]) -> None:
    # ── Infrastructure ────────────────────────────────────────────────────
    metrics = MetricsCollector(site=args.site)
    notifier = SlackNotifier()  # reads SLACK_WEBHOOK_URL from env; no-ops if absent

    checkpoint: Optional[CheckpointManager] = None
    if args.resume:
        checkpoint = CheckpointManager(db_url="scraper_state.db")
        await checkpoint.setup()
        logger.info("Checkpoint manager ready (resume mode)")

    # Propagate CLI overrides into site config
    if args.max_pages:
        site_config["max_pages"] = args.max_pages
    site_config["llm_enabled"] = args.llm

    # ── Scraping ──────────────────────────────────────────────────────────
    all_products: List[Dict[str, Any]] = []
    site_config["site_id"] = args.site
    scraper_cls = get_scraper(args.site, site_config)

    async with scraper_cls(site_config, metrics=metrics) as scraper:

        # Categories
        logger.info("Fetching categories from %s …", site_config["base_url"])
        try:
            categories = await scraper.scrape_categories()
        except Exception as exc:
            logger.warning("scrape_categories() raised an exception: %s", exc)
            categories = []

        if not categories:
            logger.warning(
                "No categories found for '%s' – check base_url and selectors in config. "
                "Exiting cleanly.",
                args.site,
            )
            await notifier.notify_error(
                args.site,
                "No categories found – verify base_url and CSS selectors.",
            )
            sys.exit(2)

        logger.info("Found %d categories", len(categories))
        await notifier.notify_start(args.site, len(categories))

        # Concurrency semaphore
        sem = asyncio.Semaphore(args.concurrency)

        async def _scrape_category(cat: Dict[str, str]) -> List[Dict[str, Any]]:
            async with sem:
                cat_url = cat["url"]
                cat_name = cat["name"]

                # Resume: skip already-finished categories
                if checkpoint and await checkpoint.is_scraped(cat_url):
                    logger.info("Skipping (already scraped): %s", cat_name)
                    metrics.record_checkpoint_resume()
                    return []

                # Resume: pick up mid-category
                start_page = 1
                if checkpoint:
                    progress = await checkpoint.get_progress(args.site, cat_name)
                    start_page = progress.get("last_page", 1)
                    if start_page > 1:
                        logger.info(
                            "Resuming '%s' from page %d", cat_name, start_page
                        )

                try:
                    products = await scraper.scrape_products_from_category(
                        cat_url,
                        category_name=cat_name,
                        start_page=start_page,
                    )
                except Exception as exc:
                    logger.warning(
                        "Category '%s' failed: %s – continuing", cat_name, exc
                    )
                    return []

                metrics.record_products(len(products))
                metrics.record_category()

                # Persist checkpoint
                if checkpoint:
                    await checkpoint.mark_scraped(cat_url, site=args.site)
                    await checkpoint.save_progress(args.site, cat_name, last_page=1)

                return products

        # Gather all categories with live progress bar
        tasks = [_scrape_category(cat) for cat in categories]
        results = await tqdm.gather(*tasks, desc="Categories", unit="cat")

        for result in results:
            if isinstance(result, list):
                all_products.extend(result)
            elif isinstance(result, Exception):
                logger.error("Category task error: %s", result)

        logger.info("Total products collected: %d", len(all_products))

        # Check for high failure rate and alert
        if metrics.failure_rate_pct > 50 and metrics.total_requests >= 10:
            await notifier.notify_high_failure_rate(
                args.site,
                metrics.failure_rate_pct,
                metrics.failed_requests,
                metrics.total_requests,
            )

        # ── Optional: detail pages ─────────────────────────────────────
        if args.details and all_products:
            logger.info("Fetching %d product detail pages …", len(all_products))

            async def _scrape_detail(product: Dict[str, Any]) -> Dict[str, Any]:
                async with sem:
                    if checkpoint and await checkpoint.is_scraped(product["url"]):
                        metrics.record_checkpoint_resume()
                        return product
                    try:
                        detail = await scraper.scrape_product_details(product["url"])
                        metrics.record_detail_fetch()
                        if checkpoint:
                            await checkpoint.mark_scraped(
                                product["url"], site=args.site
                            )
                        return {**product, **detail}
                    except Exception as exc:
                        logger.warning(
                            "Detail fetch failed for %s: %s", product["url"], exc
                        )
                        return product

            detail_tasks = [_scrape_detail(p) for p in all_products]
            enriched = await tqdm.gather(
                *detail_tasks, desc="Details", unit="prod"
            )
            all_products = [r for r in enriched if isinstance(r, dict)]

    # ── Save results ──────────────────────────────────────────────────────
    summary = metrics.finish()
    run_metadata = _build_run_metadata(args, site_config, summary)

    if not all_products:
        logger.warning("No products to save")
        _write_metrics(args.site, metrics)
        await notifier.notify_complete(args.site, summary)
        if checkpoint:
            await checkpoint.close()
        return

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / args.site

    if args.format in ("postgres", "mysql"):
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            logger.error(
                "DATABASE_URL environment variable is required for --format %s",
                args.format,
            )
            sys.exit(1)
        if args.format == "postgres":
            saved = await DataStorage.save_postgres(all_products, db_url)
            logger.info("PostgreSQL: %d rows upserted", saved)
        else:
            saved = await DataStorage.save_mysql(all_products, db_url)
            logger.info("MySQL: %d rows inserted", saved)
        # Also save a local JSON copy for reference
        DataStorage.save(
            all_products,
            output_path,
            fmt="json",
            site_config=site_config,
            run_metadata=run_metadata,
        )
    else:
        saved_path = DataStorage.save(
            all_products,
            output_path,
            fmt=args.format,
            site_config=site_config,
            run_metadata=run_metadata,
        )
        logger.info("Results saved to %s", saved_path)
        print(f"\nDone. {len(all_products)} products saved to: {saved_path}")

    # ── Metrics + notifications ───────────────────────────────────────────
    metrics_path = _write_metrics(args.site, metrics)
    logger.info("Run metadata saved to %s", metrics_path)

    await notifier.notify_complete(args.site, summary)

    if checkpoint:
        await checkpoint.close()

    _print_summary(summary)


def _write_metrics(site: str, metrics: MetricsCollector) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = Path("output") / f"run_{site}_{timestamp}_meta.json"
    metrics.save_summary(path)
    return path


def _build_run_metadata(
    args: argparse.Namespace,
    site_config: Dict[str, Any],
    summary: Dict[str, Any],
) -> Dict[str, Any]:
    completed_at = datetime.now(timezone.utc)
    started_at = completed_at - timedelta(
        seconds=float(summary.get("elapsed_seconds", 0) or 0)
    )
    filters = {
        "site": args.site,
        "format": args.format,
        "resume": args.resume,
        "concurrency": args.concurrency,
        "details": args.details,
        "llm": args.llm,
        "max_pages": args.max_pages or site_config.get("max_pages", 0),
        "ignore_ssl": args.ignore_ssl,
    }
    return {
        "run_id": f"{args.site}_{completed_at.strftime('%Y%m%dT%H%M%SZ')}",
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "total_products": int(summary.get("total_products", 0)),
        "sites_scraped": args.site,
        "filters_applied": json.dumps(filters, ensure_ascii=False),
    }


def _print_summary(summary: Dict[str, Any]) -> None:
    print("\n── Run Summary ───────────────────────────────")
    print(f"  Site          : {summary['site']}")
    print(f"  Products      : {summary['total_products']}")
    print(f"  Categories    : {summary['categories_scraped']}")
    print(f"  Requests      : {summary['total_requests']} "
          f"({summary['success_rate_pct']}% success)")
    print(f"  Duration      : {summary['elapsed_seconds']}s")
    print(f"  Products/min  : {summary['products_per_minute']}")
    if summary.get("jina_fallbacks"):
        print(f"  Jina fallbacks: {summary['jina_fallbacks']}")
    if summary.get("llm_extractions"):
        print(f"  LLM extracts  : {summary['llm_extractions']}")
    print("──────────────────────────────────────────────")


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="scrapyard",
        description="Scrapyard v3 – production car-parts web scraper",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--site",
        required=True,
        help="Target site identifier from config/sites.yaml",
    )
    parser.add_argument(
        "--config",
        default="config/sites.yaml",
        help="Path to the YAML site configuration file",
    )
    parser.add_argument(
        "--output",
        default="output",
        help="Output directory (filename is derived from --site)",
    )
    parser.add_argument(
        "--format",
        default="json",
        choices=["csv", "json", "excel", "sqlite", "postgres", "mysql"],
        help="Export format",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume a previous run – skip categories/products already scraped",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum number of concurrent category or detail-page fetches",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Also scrape individual product detail pages (slower)",
    )
    parser.add_argument(
        "--llm",
        action="store_true",
        help="Enable LLM (GPT-4o-mini) fallback when CSS selectors return empty values",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        metavar="N",
        help="Override config max_pages (0 = use config value)",
    )
    parser.add_argument(
        "--ignore-ssl",
        action="store_true",
        help="Disable SSL certificate verification",
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

    # Validate base_url
    base_url = site_config.get("base_url", "")
    if not base_url or "example" in base_url.lower():
        logger.warning(
            "base_url for '%s' looks like a placeholder ('%s'). "
            "Update config/sites.yaml before scraping.",
            args.site, base_url,
        )

    # Propagate SSL flag into config for aiohttp scrapers
    site_config["ignore_ssl"] = args.ignore_ssl

    Path(args.output).mkdir(parents=True, exist_ok=True)

    try:
        asyncio.run(run(args, site_config))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
