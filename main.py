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
import copy
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from tqdm.asyncio import tqdm

from scrapers.detail_helpers import merge_product_payloads
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
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

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

DEFAULT_SITE_SELECTOR = "all"
DEFAULT_MAX_PAGES = 200

_OUTPUT_SUFFIX_FORMATS = {
    ".csv": "csv",
    ".json": "json",
    ".xlsx": "excel",
    ".db": "sqlite",
}


class SiteRunError(RuntimeError):
    """Raised when a single site run fails in a recoverable way."""


def _product_has_price(product: Dict[str, Any]) -> bool:
    """Return True when a scraped row already contains a usable numeric price."""
    price = product.get("price")
    if price is None:
        return False
    if isinstance(price, str):
        return price.strip() != ""
    return price == price


def _needs_price_backfill(product: Dict[str, Any]) -> bool:
    """Backfill only rows that are missing both parsed and raw price values."""
    if _product_has_price(product):
        return False
    raw_price = str(product.get("raw_price", "") or "").strip()
    if raw_price:
        return False
    product_url = str(product.get("url", "") or "")
    if not product_url or "#material-" in product_url:
        return False
    if product.get("listing_only"):
        return False
    return True

# ---------------------------------------------------------------------------
# Core async logic
# ---------------------------------------------------------------------------


async def run(
    args: argparse.Namespace,
    site_config: Dict[str, Any],
) -> Dict[str, Any]:
    # ── Infrastructure ────────────────────────────────────────────────────
    metrics = MetricsCollector(site=args.site)
    notifier = SlackNotifier()  # reads SLACK_WEBHOOK_URL from env; no-ops if absent

    checkpoint: Optional[CheckpointManager] = None
    if args.resume or args.incremental:
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
            raise SiteRunError(
                f"No categories found for '{args.site}' - verify config selectors."
            )

        logger.info("Found %d categories", len(categories))
        await notifier.notify_start(args.site, len(categories))

        # Concurrency semaphore
        sem = asyncio.Semaphore(args.concurrency)

        async def _scrape_category(cat: Dict[str, str]) -> List[Dict[str, Any]]:
            async with sem:
                cat_url = cat["url"]
                cat_name = cat["name"]

                # Resume: skip already-finished categories
                if (
                    checkpoint
                    and args.resume
                    and not args.incremental
                    and not args.force
                    and await checkpoint.is_scraped(cat_url)
                ):
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
        if getattr(args, "show_progress", True):
            results = await tqdm.gather(
                *tasks,
                desc=f"{args.site}: Categories",
                unit="cat",
            )
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

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
        should_fetch_details = args.details or bool(site_config.get("auto_details"))
        should_backfill_missing_prices = bool(
            site_config.get("backfill_missing_prices", True)
        )
        missing_price_indexes = [
            index
            for index, product in enumerate(all_products)
            if _needs_price_backfill(product)
        ]
        target_indexes = (
            list(range(len(all_products)))
            if should_fetch_details
            else missing_price_indexes if should_backfill_missing_prices else []
        )
        if target_indexes and all_products:
            if should_fetch_details:
                logger.info("Fetching %d product detail pages …", len(target_indexes))
            else:
                logger.info(
                    "Backfilling missing prices from %d product detail pages …",
                    len(target_indexes),
                )

            async def _scrape_detail(
                product: Dict[str, Any],
                *,
                force_fetch: bool = False,
            ) -> Dict[str, Any]:
                async with sem:
                    if (
                        checkpoint
                        and not force_fetch
                        and args.resume
                        and not args.incremental
                        and not args.force
                        and await checkpoint.is_scraped(product["url"])
                    ):
                        metrics.record_checkpoint_resume()
                        return product
                    try:
                        detail = await scraper.scrape_product_details(product["url"])
                        metrics.record_detail_fetch()
                        if checkpoint:
                            await checkpoint.mark_scraped(
                                product["url"], site=args.site
                            )
                        return merge_product_payloads(product, detail)
                    except Exception as exc:
                        logger.warning(
                            "Detail fetch failed for %s: %s", product["url"], exc
                        )
                        return product

            detail_tasks = [
                _scrape_detail(
                    all_products[index],
                    force_fetch=not should_fetch_details,
                )
                for index in target_indexes
            ]
            if getattr(args, "show_progress", True):
                enriched = await tqdm.gather(
                    *detail_tasks,
                    desc=(
                        f"{args.site}: Details"
                        if should_fetch_details
                        else f"{args.site}: Price Backfill"
                    ),
                    unit="prod",
                )
            else:
                enriched = await asyncio.gather(
                    *detail_tasks,
                    return_exceptions=True,
                )
            for index, result in zip(target_indexes, enriched):
                if isinstance(result, dict):
                    all_products[index] = result

    # ── Save results ──────────────────────────────────────────────────────
    summary = metrics.finish()
    run_metadata = _build_run_metadata(args, site_config, summary)

    scraped_at = datetime.now(timezone.utc).isoformat()
    for product in all_products:
        product["vendor_id"] = product.get("vendor_id") or args.site
        product["vendor_name"] = product.get("vendor_name") or site_config.get(
            "display_name",
            args.site,
        )
        product["site_url"] = product.get("site_url") or site_config.get("base_url", "")
        product["platform_type"] = product.get("platform_type") or site_config.get(
            "platform_type",
            site_config.get("type", "custom"),
        )
        product["currency"] = product.get("currency") or site_config.get("currency", "EGP")
        product["scraped_at"] = product.get("scraped_at") or scraped_at
        if checkpoint:
            product["product_id"] = product.get("product_id") or checkpoint._generate_product_id(
                args.site,
                str(product.get("url", "")),
            )
        else:
            product["product_id"] = product.get("product_id") or DataStorage._product_id(
                str(product.get("url", "")),
                args.site,
            )

    if not all_products:
        logger.warning("No products to save")
        _write_metrics(args.site, metrics)
        await notifier.notify_complete(args.site, summary)
        if checkpoint:
            await checkpoint.close()
        return {
            "status": "success",
            "site": args.site,
            "saved_count": 0,
            "total_products": 0,
            "output_path": None,
            "summary": summary,
            "products": [],
        }

    if getattr(args, "defer_save", False):
        export_products = all_products

        if args.incremental and checkpoint:
            changed_products: List[Dict[str, Any]] = []
            for product in all_products:
                changed = await checkpoint.has_changed(
                    str(product.get("product_id", "")),
                    product.get("price"),
                    str(product.get("stock_status", "unknown")),
                )
                if changed:
                    changed_products.append(product)
            export_products = changed_products

        if export_products:
            await DataStorage.enrich_products_for_export(
                export_products,
                use_llm=args.llm,
            )

            if args.incremental and checkpoint:
                for product in export_products:
                    await checkpoint.update_snapshot(
                        str(product.get("product_id", "")),
                        args.site,
                        str(product.get("url", "")),
                        product.get("price"),
                        str(product.get("stock_status", "unknown")),
                        product,
                    )
        else:
            logger.info("No new or changed products detected for %s", args.site)

        metrics_path = _write_metrics(args.site, metrics)
        logger.info("Run metadata saved to %s", metrics_path)

        await notifier.notify_complete(args.site, summary)

        if checkpoint:
            await checkpoint.close()

        return {
            "status": "success",
            "site": args.site,
            "saved_count": len(export_products),
            "total_products": int(summary.get("total_products", 0)),
            "output_path": None,
            "summary": summary,
            "products": export_products,
        }

    output_path = _resolve_output_path(
        args.output,
        args.site,
        multi_site=getattr(args, "multi_site", False),
    )
    saved_path: Optional[Path] = None
    saved_count = len(all_products)

    if not args.incremental or args.format in ("postgres", "mysql"):
        await DataStorage.enrich_products_for_export(
            all_products,
            use_llm=args.llm,
        )

    if args.incremental and args.format in ("postgres", "mysql"):
        logger.warning(
            "Incremental mode is not supported for %s export yet; falling back to full export",
            args.format,
        )

    if args.incremental and args.format not in ("postgres", "mysql"):
        saved_path, changed_count = await DataStorage.save_products_incremental(
            all_products,
            output_path,
            format=args.format,
            checkpoint_mgr=checkpoint,
            site=args.site,
            site_config=site_config,
            run_metadata=run_metadata,
            use_llm=args.llm,
        )
        if changed_count == 0:
            logger.info("No new or changed products detected for %s", args.site)
            print(f"\nDone. No new or changed products for: {args.site}")
        else:
            logger.info(
                "Saved %d new/changed products to %s",
                changed_count,
                saved_path,
            )
            print(
                f"\nDone. {changed_count} new/changed products saved to: {saved_path}"
            )
        saved_count = changed_count
    elif args.format in ("postgres", "mysql"):
        db_url = os.environ.get("DATABASE_URL", "")
        if not db_url:
            raise SiteRunError(
                f"DATABASE_URL environment variable is required for --format {args.format}"
            )
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
        saved_count = saved
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
        saved_count = len(all_products)

    # ── Metrics + notifications ───────────────────────────────────────────
    metrics_path = _write_metrics(args.site, metrics)
    logger.info("Run metadata saved to %s", metrics_path)

    await notifier.notify_complete(args.site, summary)

    if checkpoint:
        await checkpoint.close()

    _print_summary(summary)
    return {
        "status": "success",
        "site": args.site,
        "saved_count": saved_count,
        "total_products": int(summary.get("total_products", 0)),
        "output_path": str(saved_path) if saved_path else None,
        "summary": summary,
        "products": [],
    }


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
        "incremental": args.incremental,
        "force": args.force,
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


def _resolve_output_path(
    output_arg: str,
    site: str,
    *,
    multi_site: bool = False,
) -> Path:
    """Treat --output as either a directory or a concrete file path."""
    raw_path = Path(output_arg)
    if raw_path.suffix.lower() in _OUTPUT_SUFFIX_FORMATS:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        if multi_site:
            target_dir = raw_path.parent / raw_path.stem
            target_dir.mkdir(parents=True, exist_ok=True)
            return target_dir / site
        return raw_path.with_suffix("")

    raw_path.mkdir(parents=True, exist_ok=True)
    return raw_path / site


def _resolve_combined_output_path(output_arg: str) -> Path:
    """Resolve the export target for a combined multi-site workbook/file."""
    raw_path = Path(output_arg)
    if raw_path.suffix.lower() in _OUTPUT_SUFFIX_FORMATS:
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        return raw_path.with_suffix("")

    raw_path.mkdir(parents=True, exist_ok=True)
    return raw_path / "all_sites"


def _resolve_output_format(args: argparse.Namespace) -> str:
    """Infer export format from args and output path."""
    explicit = args.format
    inferred = _OUTPUT_SUFFIX_FORMATS.get(Path(args.output).suffix.lower())
    if explicit:
        if inferred and inferred != explicit:
            logger.warning(
                "Output suffix '%s' does not match --format '%s'; using the explicit format",
                Path(args.output).suffix,
                explicit,
            )
        return explicit
    return inferred or "excel"


def _canonical_site_ids(all_configs: Dict[str, Any]) -> List[str]:
    """Return canonical site ids, skipping alias duplicates."""
    seen: set[tuple[str, str, str]] = set()
    site_ids: List[str] = []
    for site_id, config in all_configs.items():
        key = (
            str(config.get("base_url", "")).rstrip("/"),
            str(config.get("engine", "")),
            str(config.get("display_name", site_id)),
        )
        if key in seen:
            continue
        seen.add(key)
        site_ids.append(site_id)
    return site_ids


def _resolve_site_ids(requested_site: str, all_configs: Dict[str, Any]) -> List[str]:
    """Resolve the requested site selector into concrete site ids."""
    if requested_site.lower() == "all":
        return _canonical_site_ids(all_configs)
    if requested_site in all_configs:
        return [requested_site]
    return []


async def _run_many_sites(
    args: argparse.Namespace,
    all_configs: Dict[str, Any],
    site_ids: List[str],
) -> List[Dict[str, Any]]:
    """Run multiple sites concurrently."""
    limit = args.site_concurrency or len(site_ids)
    limit = max(1, limit)
    logger.info(
        "Running %d sites concurrently (limit=%d): %s",
        len(site_ids),
        limit,
        ", ".join(site_ids),
    )
    semaphore = asyncio.Semaphore(limit)

    async def _run_one(site_id: str) -> Dict[str, Any]:
        async with semaphore:
            site_args = argparse.Namespace(**vars(args))
            site_args.site = site_id
            site_args.multi_site = True
            site_args.show_progress = False
            site_config = copy.deepcopy(all_configs[site_id])
            site_config["ignore_ssl"] = args.ignore_ssl

            base_url = site_config.get("base_url", "")
            if not base_url or "example" in str(base_url).lower():
                logger.warning(
                    "base_url for '%s' looks like a placeholder ('%s').",
                    site_id,
                    base_url,
                )

            logger.info("Starting site: %s", site_id)
            try:
                result = await run(site_args, site_config)
            except Exception as exc:
                logger.exception("Site '%s' failed", site_id)
                return {
                    "status": "failed",
                    "site": site_id,
                    "saved_count": 0,
                    "total_products": 0,
                    "output_path": None,
                    "error": str(exc),
                }

            logger.info("Completed site: %s", site_id)
            return result

    tasks = [asyncio.create_task(_run_one(site_id)) for site_id in site_ids]
    return await asyncio.gather(*tasks)


def _print_multi_site_summary(results: List[Dict[str, Any]]) -> None:
    print("\n-- Multi-Site Summary -----------------------")
    success_count = 0
    failure_count = 0
    total_saved = 0

    for result in results:
        site = result.get("site", "?")
        status = result.get("status", "failed")
        if status == "success":
            success_count += 1
            saved = int(result.get("saved_count", 0) or 0)
            total_saved += saved
            print(f"  {site:<16} OK     saved={saved}")
        else:
            failure_count += 1
            error = result.get("error", "unknown error")
            print(f"  {site:<16} FAILED {error}")

    print("---------------------------------------------")
    print(f"  Sites succeeded : {success_count}")
    print(f"  Sites failed    : {failure_count}")
    print(f"  Total saved     : {total_saved}")
    print("---------------------------------------------")


def _build_combined_run_metadata(
    args: argparse.Namespace,
    products: List[Dict[str, Any]],
    site_ids: List[str],
) -> Dict[str, Any]:
    completed_at = datetime.now(timezone.utc)
    filters = {
        "site": "all",
        "format": args.format,
        "resume": args.resume,
        "concurrency": args.concurrency,
        "site_concurrency": args.site_concurrency,
        "details": args.details,
        "llm": args.llm,
        "incremental": args.incremental,
        "force": args.force,
        "max_pages": args.max_pages,
        "ignore_ssl": args.ignore_ssl,
    }
    return {
        "run_id": f"all_sites_{completed_at.strftime('%Y%m%dT%H%M%SZ')}",
        "started_at": completed_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "total_products": len(products),
        "sites_scraped": ", ".join(sorted(site_ids)),
        "filters_applied": json.dumps(filters, ensure_ascii=False),
    }


def _print_summary(summary: Dict[str, Any]) -> None:
    print("\n-- Run Summary ------------------------------")
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
    print("---------------------------------------------")


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
        default=DEFAULT_SITE_SELECTOR,
        help="Target site identifier from config/sites.yaml, or 'all' to scrape every canonical site concurrently",
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
        default=None,
        choices=["csv", "json", "excel", "sqlite", "postgres", "mysql"],
        help="Export format. If omitted, the format is inferred from --output or defaults to excel",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume a previous run – skip categories/products already scraped",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Export only products whose price or stock status changed",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore checkpoint skip logic and re-scrape categories",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum number of concurrent category or detail-page fetches",
    )
    parser.add_argument(
        "--site-concurrency",
        type=int,
        default=0,
        help="Maximum number of concurrent site runs when --site all (0 = run all selected sites in parallel)",
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
        default=DEFAULT_MAX_PAGES,
        metavar="N",
        help="Maximum pages to scrape per site by default (pass 0 to use the per-site config value)",
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

    args.format = _resolve_output_format(args)
    selected_sites = _resolve_site_ids(args.site, all_configs)

    if not selected_sites:
        logger.error(
            "Site '%s' not found in %s. Available: %s",
            args.site, config_path, list(all_configs),
        )
        sys.exit(1)

    logger.info("Using export format: %s", args.format)
    if args.site.lower() == DEFAULT_SITE_SELECTOR:
        logger.info(
            "Running all %d canonical sites: %s",
            len(selected_sites),
            ", ".join(selected_sites),
        )

    try:
        if len(selected_sites) == 1:
            args.site = selected_sites[0]
            args.multi_site = False
            args.show_progress = True
            site_config = copy.deepcopy(all_configs[args.site])

            # Validate base_url
            base_url = site_config.get("base_url", "")
            if not base_url or "example" in str(base_url).lower():
                logger.warning(
                    "base_url for '%s' looks like a placeholder ('%s'). "
                    "Update config/sites.yaml before scraping.",
                    args.site, base_url,
                )

            # Propagate SSL flag into config for aiohttp scrapers
            site_config["ignore_ssl"] = args.ignore_ssl
            asyncio.run(run(args, site_config))
        else:
            args.multi_site = True
            args.show_progress = False
            args.defer_save = args.format in ("csv", "json", "excel", "sqlite")
            results = asyncio.run(_run_many_sites(args, all_configs, selected_sites))
            combined_output_path: Optional[Path] = None

            if args.defer_save:
                combined_products: List[Dict[str, Any]] = []
                successful_sites: List[str] = []
                for result in results:
                    if result.get("status") != "success":
                        continue
                    successful_sites.append(str(result.get("site", "")))
                    combined_products.extend(result.get("products", []))

                if combined_products:
                    combined_output_path = DataStorage.save(
                        combined_products,
                        _resolve_combined_output_path(args.output),
                        fmt=args.format,
                        site_config={
                            "site_id": "all_sites",
                            "display_name": "All Sites",
                            "currency": "EGP",
                        },
                        run_metadata=_build_combined_run_metadata(
                            args,
                            combined_products,
                            successful_sites,
                        ),
                    )
                    saved_file = combined_output_path.with_suffix(
                        ".xlsx" if args.format == "excel" else f".{args.format}"
                    )
                    logger.info(
                        "Combined %s export saved to %s",
                        args.format,
                        saved_file,
                    )
                    print(f"\nCombined export saved to: {saved_file}")
                else:
                    logger.info("No products available to write for the combined export")

            _print_multi_site_summary(results)
            if any(result.get("status") != "success" for result in results):
                sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except SiteRunError as exc:
        logger.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
