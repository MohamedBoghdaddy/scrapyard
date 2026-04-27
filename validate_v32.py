from __future__ import annotations

import argparse
import asyncio
import copy
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple

import pandas as pd
import yaml

from scrapers import get_scraper
from utils.storage import parse_compatibility_text

REPO_ROOT = Path(__file__).resolve().parent
MAIN_PY = REPO_ROOT / "main.py"
CONFIG_YAML = REPO_ROOT / "config" / "sites.yaml"

REQUIRED_SHEETS = {
    "products",
    "aggregated_prices",
    "vendors",
    "compatibility",
    "categories",
    "scrape_metadata",
}


def _build_command(
    config_path: Path,
    site: str,
    output_file: Path,
    max_pages: int,
    extra_args: Iterable[str],
) -> list[str]:
    return [
        sys.executable,
        str(MAIN_PY),
        "--config",
        str(config_path),
        "--site",
        site,
        "--incremental",
        "--max-pages",
        str(max_pages),
        "--output",
        str(output_file),
        "--log-level",
        "WARNING",
        *extra_args,
    ]


def _load_site_config(site: str) -> dict[str, Any]:
    with open(CONFIG_YAML, encoding="utf-8") as fh:
        all_configs = yaml.safe_load(fh)
    if site not in all_configs:
        raise AssertionError(
            f"Site '{site}' was not found in {CONFIG_YAML}."
        )
    site_config = copy.deepcopy(all_configs[site])
    site_config["site_id"] = site
    return site_config


async def _discover_seed_category(site: str) -> dict[str, str]:
    site_config = _load_site_config(site)
    existing_seeds = site_config.get("seed_categories") or site_config.get("categories") or []
    if existing_seeds:
        first = existing_seeds[0]
        if isinstance(first, str):
            return {"name": site, "url": first}
        return {
            "name": first.get("name") or site,
            "url": first.get("url", ""),
        }

    scraper_cls = get_scraper(site, site_config)
    async with scraper_cls(site_config) as scraper:
        categories = await scraper.scrape_categories()

    if not categories:
        raise AssertionError(f"No categories discovered for site '{site}'.")

    chosen = categories[0]
    if not chosen.get("url"):
        raise AssertionError(
            f"Discovered category for site '{site}' is missing a URL: {chosen}"
        )
    return {
        "name": chosen.get("name") or site,
        "url": chosen["url"],
    }


def _write_validation_config(
    *,
    workdir: Path,
    site: str,
    max_pages: int,
) -> tuple[Path, dict[str, str]]:
    site_config = _load_site_config(site)
    seed_category = asyncio.run(_discover_seed_category(site))
    site_config["seed_categories"] = [seed_category]
    site_config["max_pages"] = max_pages

    config_dir = workdir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_path = config_dir / "validate_sites.yaml"
    with open(config_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(
            {site: site_config},
            fh,
            sort_keys=False,
            allow_unicode=True,
        )
    return config_path, seed_category


def _extract_changed_count(output: str) -> int:
    saved_match = re.search(
        r"Done\.\s+(\d+)\s+new/changed products saved to:",
        output,
        flags=re.IGNORECASE,
    )
    if saved_match:
        return int(saved_match.group(1))

    if re.search(r"Done\.\s+No new or changed products", output, flags=re.IGNORECASE):
        return 0

    logger_match = re.search(
        r"Saved\s+(\d+)\s+new/changed products to",
        output,
        flags=re.IGNORECASE,
    )
    if logger_match:
        return int(logger_match.group(1))

    raise AssertionError(
        "Could not determine incremental save count from scraper output.\n"
        f"Captured output:\n{output[-3000:]}"
    )


def _run_incremental(
    *,
    workdir: Path,
    config_path: Path,
    site: str,
    output_file: Path,
    max_pages: int,
    timeout_seconds: int,
    extra_args: Iterable[str],
) -> Tuple[int, str]:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONPATH"] = (
        str(REPO_ROOT)
        if not env.get("PYTHONPATH")
        else str(REPO_ROOT) + os.pathsep + env["PYTHONPATH"]
    )

    command = _build_command(
        config_path=config_path,
        site=site,
        output_file=output_file,
        max_pages=max_pages,
        extra_args=extra_args,
    )
    result = subprocess.run(
        command,
        cwd=workdir,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_seconds,
        check=False,
    )
    output = "\n".join(part for part in (result.stdout, result.stderr) if part)
    if result.returncode != 0:
        raise AssertionError(
            "Scraper command failed.\n"
            f"Command: {' '.join(command)}\n"
            f"Exit code: {result.returncode}\n"
            f"Output:\n{output[-4000:]}"
        )
    return _extract_changed_count(output), output


async def _validate_compatibility() -> list[Dict[str, Any]]:
    sample = "Toyota Corolla 2015-2020 | تويوتا كورولا 2015-2020"
    parsed = await parse_compatibility_text(sample)

    expected_english = any(
        item.get("make") == "Toyota"
        and item.get("model") == "Corolla"
        and item.get("year_start") == 2015
        and item.get("year_end") == 2020
        for item in parsed
    )
    expected_arabic = any(
        item.get("make") == "تويوتا"
        and item.get("model") == "كورولا"
        and item.get("year_start") == 2015
        and item.get("year_end") == 2020
        for item in parsed
    )

    if not expected_english or not expected_arabic:
        raise AssertionError(
            "Bilingual compatibility parsing failed.\n"
            f"Parsed output: {json.dumps(parsed, ensure_ascii=False, indent=2)}"
        )
    return parsed


def _validate_workbook(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise AssertionError(f"Expected Excel export was not created: {path}")

    workbook = pd.ExcelFile(path)
    try:
        sheet_names = set(workbook.sheet_names)
        missing = sorted(REQUIRED_SHEETS - sheet_names)
        if missing:
            raise AssertionError(
                f"Workbook is missing expected sheets: {missing}. "
                f"Found: {sorted(sheet_names)}"
            )

        products_df = pd.read_excel(path, sheet_name="products")
        if products_df.empty:
            raise AssertionError("Products sheet exists but contains no rows.")

        return {
            "sheet_names": workbook.sheet_names,
            "product_rows": len(products_df),
        }
    finally:
        workbook.close()


def _run_validation(
    *,
    site: str,
    max_pages: int,
    timeout_seconds: int,
    keep_artifacts: bool,
    extra_args: Iterable[str],
) -> dict[str, Any]:
    if keep_artifacts:
        workdir = REPO_ROOT / "output" / "validate_v32_artifacts"
        workdir.mkdir(parents=True, exist_ok=True)
        cleanup = None
    else:
        cleanup = tempfile.TemporaryDirectory(prefix="validate_v32_")
        workdir = Path(cleanup.name)

    try:
        output_file = workdir / "output" / f"{site}_validate_v32.xlsx"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        config_path, seed_category = _write_validation_config(
            workdir=workdir,
            site=site,
            max_pages=max_pages,
        )

        first_count, first_output = _run_incremental(
            workdir=workdir,
            config_path=config_path,
            site=site,
            output_file=output_file,
            max_pages=max_pages,
            timeout_seconds=timeout_seconds,
            extra_args=extra_args,
        )
        if first_count <= 0:
            raise AssertionError(
                "First incremental run did not save any products.\n"
                f"Output:\n{first_output[-4000:]}"
            )

        workbook_info = _validate_workbook(output_file)
        compatibility = asyncio.run(_validate_compatibility())

        second_count, second_output = _run_incremental(
            workdir=workdir,
            config_path=config_path,
            site=site,
            output_file=output_file,
            max_pages=max_pages,
            timeout_seconds=timeout_seconds,
            extra_args=extra_args,
        )
        if second_count != 0:
            raise AssertionError(
                f"Expected second incremental run to save 0 products, got {second_count}.\n"
                f"Output:\n{second_output[-4000:]}"
            )

        return {
            "site": site,
            "workdir": str(workdir),
            "output_file": str(output_file),
            "seed_category": seed_category,
            "first_run_saved": first_count,
            "second_run_saved": second_count,
            "sheets": workbook_info["sheet_names"],
            "product_rows": workbook_info["product_rows"],
            "compatibility_matches": compatibility,
        }
    finally:
        if cleanup is not None:
            cleanup.cleanup()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Scrapyard v3.2 incremental export and workbook behavior.",
    )
    parser.add_argument(
        "--site",
        default="egycarparts",
        help="Site id to scrape during validation.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Maximum category pages to scrape for the live validation run.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=900,
        help="Per-run timeout in seconds for the scraper subprocess.",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep the isolated validation workspace under output/validate_v32_artifacts.",
    )
    parser.add_argument(
        "extra_args",
        nargs="*",
        help="Additional arguments passed through to main.py after the built-in validation flags.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    try:
        results = _run_validation(
            site=args.site,
            max_pages=args.max_pages,
            timeout_seconds=args.timeout,
            keep_artifacts=args.keep_artifacts,
            extra_args=args.extra_args,
        )
    except Exception as exc:
        print(f"validate_v32: FAIL\n{exc}", file=sys.stderr)
        return 1

    print("validate_v32: PASS")
    print(json.dumps(results, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
