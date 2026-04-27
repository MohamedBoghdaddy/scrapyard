"""
DataStorage: export scraped data to CSV, JSON, Excel, SQLite, PostgreSQL, or MySQL.

Sync formats (csv/json/excel/sqlite) use pandas.
Async DB formats (postgres/mysql) use asyncpg / aiomysql.

Includes QA validation: invalid products are logged and saved to _invalid.csv.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from hashlib import sha1
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd

from utils.cleaners import (
    clean_part_number,
    generate_canonical_id,
    normalise_arabic,
    to_slug,
)

from db.models import (
    POSTGRES_CREATE_PRODUCTS,
    MYSQL_CREATE_PRODUCTS,
    SQLITE_CREATE_PRODUCTS,
)

logger = logging.getLogger(__name__)

SyncFormat = Literal["csv", "json", "excel", "sqlite"]

# ---------------------------------------------------------------------------
# Lazy NLP import (gracefully absent when nlp package not installed)
# ---------------------------------------------------------------------------

def _try_nlp_enrich(
    products: List[Dict[str, Any]],
    *,
    enabled: bool = True,
) -> List[Dict[str, Any]]:
    """Run NLP pipeline on all products; return originals if nlp package unavailable."""
    if not enabled:
        return products
    try:
        from nlp.pipeline import enrich_batch_nlp, NLPConfig  # type: ignore
        cfg = NLPConfig(
            language=True,
            keywords=True,
            summarize=True,
            classify=True,
            max_keywords=8,
            summary_sentences=2,
        )
        return enrich_batch_nlp(products, cfg)
    except ImportError:
        logger.debug("nlp package not available; skipping NLP enrichment")
        return products
    except Exception as exc:
        logger.warning("NLP enrichment failed: %s", exc)
        return products

_JSON_COLS = {"specifications", "variants", "tags"}

_PRODUCT_COLS = [
    "url", "name", "price", "raw_price", "vendor", "part_number",
    "image_url", "stock_status", "category", "source",
    "description", "specifications", "variants",
]

# ---------------------------------------------------------------------------
# QA Validation
# ---------------------------------------------------------------------------

PRODUCT_QA: Dict[str, Dict[str, Any]] = {
    "name":  {"required": True,  "min_length": 2},
    "price": {"required": False, "type": float},
    "url":   {"required": True,  "min_length": 10},
}

_ARABIC_DIGIT_MAP = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")

VEHICLE_PATTERNS = [
    re.compile(
        r"(?P<make>[A-Za-z]+)\s+(?P<model>[A-Za-z0-9\-]+)\s+"
        r"(?P<year_start>\d{4})\s*[-–]\s*(?P<year_end>\d{4})"
    ),
    re.compile(
        r"(?P<make_ar>[\u0621-\u064A]+)\s+(?P<model_ar>[\u0621-\u064A0-9\-]+)\s+"
        r"(?P<year_start>\d{4})\s*[-–]\s*(?P<year_end>\d{4})"
    ),
    re.compile(
        r"(?P<make>[A-Za-z]+)\s+(?P<model>[A-Za-z0-9\-]+)\s+"
        r"(?P<year>\d{4})(?!\s*[-–]\s*\d{4})"
    ),
    re.compile(
        r"(?P<make_ar>[\u0621-\u064A]+)\s+(?P<model_ar>[\u0621-\u064A0-9\-]+)\s+"
        r"(?P<year>\d{4})(?!\s*[-–]\s*\d{4})"
    ),
]


def validate_product(product: Dict[str, Any]) -> List[str]:
    """
    Run QA rules against a product dict.
    Returns a list of error messages (empty list = valid).
    """
    errors: List[str] = []
    for field, rules in PRODUCT_QA.items():
        value = product.get(field)
        if rules.get("required") and not value:
            errors.append(f"Missing required field: {field!r}")
            continue
        if value is None:
            continue
        min_len = rules.get("min_length")
        if min_len and isinstance(value, str) and len(value) < min_len:
            errors.append(
                f"Field {field!r} too short: {len(value)} < {min_len}"
            )
        expected_type = rules.get("type")
        if expected_type and not isinstance(value, expected_type):
            try:
                expected_type(value)
            except (TypeError, ValueError):
                errors.append(
                    f"Field {field!r} is not {expected_type.__name__}: {value!r}"
                )
    return errors


def partition_products(
    data: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Split *data* into (valid, invalid) based on PRODUCT_QA rules.
    Invalid products have an extra '_validation_errors' key.
    """
    valid, invalid = [], []
    for product in data:
        errs = validate_product(product)
        if errs:
            invalid.append({**product, "_validation_errors": errs})
            logger.warning("Invalid product %s: %s", product.get("url", "?"), errs)
        else:
            valid.append(product)
    return valid, invalid


async def parse_compatibility_text(
    text: str,
    use_llm: bool = False,
) -> List[Dict[str, Any]]:
    """Extract structured vehicle compatibility from raw text."""
    if not text:
        return []

    normalised = normalise_arabic(str(text)).translate(_ARABIC_DIGIT_MAP)
    matches: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, Optional[int], Optional[int], str]] = set()

    for pattern in VEHICLE_PATTERNS:
        for match in pattern.finditer(normalised):
            groups = match.groupdict()
            make = groups.get("make") or groups.get("make_ar") or ""
            model = groups.get("model") or groups.get("model_ar") or ""
            year_start = groups.get("year_start") or groups.get("year")
            year_end = groups.get("year_end") or year_start
            entry = {
                "make": make.strip(),
                "model": model.strip(),
                "year_start": int(year_start) if year_start else None,
                "year_end": int(year_end) if year_end else None,
                "raw": match.group().strip(),
            }
            dedupe_key = (
                entry["make"],
                entry["model"],
                entry["year_start"],
                entry["year_end"],
                entry["raw"],
            )
            if dedupe_key not in seen:
                seen.add(dedupe_key)
                matches.append(entry)

    if matches or not use_llm or not os.getenv("OPENAI_API_KEY"):
        return matches

    try:
        from openai import AsyncOpenAI
    except ImportError:
        logger.warning("OpenAI client not installed; skipping compatibility LLM fallback")
        return matches

    client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    prompt = (
        "Extract vehicle compatibility information from the text below. "
        "Return JSON with a top-level key 'compatibility' containing a list of "
        "objects with keys: make, model, year_start, year_end, raw.\n\n"
        f"Text: {text}"
    )
    try:
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
        )
        payload = json.loads(response.choices[0].message.content or "{}")
        compat = payload.get("compatibility", [])
        if isinstance(compat, list):
            return [entry for entry in compat if isinstance(entry, dict)]
    except Exception as exc:
        logger.warning("Compatibility LLM fallback failed: %s", exc)
    return matches


# ---------------------------------------------------------------------------
# Main storage class
# ---------------------------------------------------------------------------


class DataStorage:
    """
    Unified data export for Scrapyard.

    Sync usage::

        DataStorage.save(products, "output/egycarparts", fmt="json")

    Async DB usage::

        await DataStorage.save_postgres(products, "postgresql://...")
        await DataStorage.save_mysql(products, "mysql://...")
    """

    @staticmethod
    async def enrich_products_for_export(
        products: List[Dict[str, Any]],
        *,
        use_llm: bool = False,
    ) -> List[Dict[str, Any]]:
        """Add structured compatibility data when raw compatibility text exists."""
        for product in products:
            compatibility_text = product.get("compatibility_text", "")
            if product.get("compatibility") or not compatibility_text:
                continue
            compatibility = await parse_compatibility_text(
                compatibility_text,
                use_llm=use_llm,
            )
            if compatibility:
                product["compatibility"] = compatibility
                product["compatibility_parsed"] = json.dumps(
                    compatibility,
                    ensure_ascii=False,
                )
        return products

    @staticmethod
    async def save_products_incremental(
        products: List[Dict[str, Any]],
        filepath: str | Path,
        *,
        format: SyncFormat = "excel",
        checkpoint_mgr: Any = None,
        site: Optional[str] = None,
        site_config: Optional[Dict[str, Any]] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
        use_llm: bool = False,
    ) -> Tuple[Path, int]:
        """
        Save only new/changed products based on the checkpoint snapshot table.

        Returns ``(output_path, changed_count)``.
        """
        path = Path(filepath)
        if format not in {"csv", "json", "excel", "sqlite"}:
            raise ValueError(
                "Incremental export currently supports csv/json/excel/sqlite only"
            )

        vendor_id = site or (site_config or {}).get("site_id") or "site"
        filtered: List[Dict[str, Any]] = []

        for product in products:
            product_url = str(product.get("url", ""))
            if not product_url:
                continue

            product["vendor_id"] = product.get("vendor_id") or vendor_id
            product["vendor_name"] = product.get("vendor_name") or (site_config or {}).get(
                "display_name", vendor_id
            )
            product["currency"] = product.get("currency") or (site_config or {}).get(
                "currency",
                "EGP",
            )
            product["scraped_at"] = product.get("scraped_at") or datetime.now(
                timezone.utc
            ).isoformat()

            if checkpoint_mgr:
                product["product_id"] = product.get("product_id") or checkpoint_mgr._generate_product_id(
                    vendor_id,
                    product_url,
                )
                changed = await checkpoint_mgr.has_changed(
                    product["product_id"],
                    product.get("price"),
                    product.get("stock_status", "unknown"),
                )
                if not changed:
                    continue
            else:
                product["product_id"] = product.get("product_id") or DataStorage._product_id(
                    product_url,
                    vendor_id,
                )

            filtered.append(product)

        if not filtered:
            logger.info("No new or changed products detected for %s", vendor_id)
            return path, 0

        await DataStorage.enrich_products_for_export(filtered, use_llm=use_llm)

        for product in filtered:
            if checkpoint_mgr:
                await checkpoint_mgr.update_snapshot(
                    product["product_id"],
                    vendor_id,
                    str(product.get("url", "")),
                    product.get("price"),
                    product.get("stock_status", "unknown"),
                    product,
                )

        saved_path = DataStorage.save(
            filtered,
            path,
            fmt=format,
            site_config=site_config,
            run_metadata=run_metadata,
        )
        return saved_path, len(filtered)

    # ------------------------------------------------------------------
    # Synchronous file exports
    # ------------------------------------------------------------------

    @staticmethod
    def save(
        data: List[Dict[str, Any]],
        filepath: str | Path,
        fmt: SyncFormat = "json",
        *,
        site_config: Optional[Dict[str, Any]] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
        # New safe-export parameters
        safe_mode: bool = True,
        max_rows_per_file: int = 0,
        fallback_to_csv: bool = True,
        no_excel_fallback: bool = False,
        generate_quality_report: bool = False,
        nlp_enabled: bool = True,
    ) -> Path:
        """
        Write *data* to *filepath* in the chosen format.

        Valid products are written to the main file.
        Invalid products (failing QA) are written to a sibling _invalid.csv file.
        Returns the resolved output Path.
        """
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)

        if not data:
            logger.warning("save() called with empty data - skipping write")
            return path

        valid, invalid = partition_products(data)
        logger.info(
            "QA results: %d valid, %d invalid out of %d total",
            len(valid), len(invalid), len(data),
        )

        if invalid:
            inv_path = path.parent / f"{path.name}_invalid"
            DataStorage._save_csv(invalid, inv_path)
            logger.warning(
                "%d invalid products written to %s", len(invalid), inv_path.with_suffix(".csv")
            )

        if not valid:
            logger.warning("No valid products to save")
            return path

        # NLP enrichment — runs for ALL formats when enabled
        enriched = _try_nlp_enrich(valid, enabled=nlp_enabled)

        # Build optional quality report AFTER enrichment so NLP fields are populated
        quality_report: Optional[Dict[str, Any]] = None
        if generate_quality_report:
            from utils.quality_report import build_quality_report
            quality_report = build_quality_report(
                enriched,
                run_meta=run_metadata,
                export_format=fmt,
            )

        if fmt == "csv":
            saved = DataStorage._save_csv(enriched, path)
        elif fmt == "json":
            DataStorage._save_json(enriched, path)
            saved = path.with_suffix(".json")
        elif fmt == "excel":
            saved = DataStorage._save_excel(
                enriched,
                path,
                site_config=site_config,
                run_metadata=run_metadata,
                safe_mode=safe_mode,
                max_rows_per_file=max_rows_per_file,
                fallback_to_csv=fallback_to_csv,
                no_excel_fallback=no_excel_fallback,
                quality_report=quality_report,
            )
        elif fmt == "sqlite":
            DataStorage._save_sqlite(enriched, path)
            saved = path.with_suffix(".db")
        else:
            raise ValueError(
                "Unsupported format "
                f"'{fmt}'. Choose from: ['csv', 'json', 'excel', 'sqlite']"
            )

        # Persist quality report as a sidecar JSON if requested
        if generate_quality_report and quality_report:
            from utils.quality_report import save_quality_report
            qr_path = path.parent / f"{path.name}_quality_report.json"
            save_quality_report(quality_report, qr_path)

        logger.info(
            "Saved %d records → %s (%s)",
            len(valid), path, fmt,
        )
        return path

    @staticmethod
    def _save_csv(data: List[Dict[str, Any]], path: Path) -> Path:
        out = path.with_suffix(".csv")
        from utils.data_sanitizer import sanitize_dataframe
        df = DataStorage._flatten(data)
        sanitize_dataframe(df).to_csv(out, index=False, encoding="utf-8-sig")
        return out

    @staticmethod
    def _save_json(data: List[Dict[str, Any]], path: Path) -> None:
        with open(path.with_suffix(".json"), "w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _save_excel(
        data: List[Dict[str, Any]],
        path: Path,
        *,
        site_config: Optional[Dict[str, Any]] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
        safe_mode: bool = True,
        max_rows_per_file: int = 0,
        fallback_to_csv: bool = True,
        no_excel_fallback: bool = False,
        quality_report: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """
        Write an Excel workbook using the hardened writer.

        If safe_mode is True (default), writes to a temp file first, validates,
        then atomically renames. Falls back to CSV if Excel fails and
        fallback_to_csv is True.

        Returns the final output path (.xlsx or .csv).
        """
        from utils.excel_writer import safe_excel_write, split_and_write_excel
        from utils.quality_report import quality_report_to_dataframe

        workbook = DataStorage._build_excel_workbook(
            data,
            site_config=site_config,
            run_metadata=run_metadata,
        )

        # Attach quality report sheet when available
        if quality_report:
            workbook["data_quality_report"] = quality_report_to_dataframe(quality_report)

        t0 = __import__("time").monotonic()

        if max_rows_per_file > 0 and len(data) > max_rows_per_file:
            results = split_and_write_excel(
                workbook,
                path,
                max_rows_per_file=max_rows_per_file,
                fallback_to_csv=fallback_to_csv,
                no_excel_fallback=no_excel_fallback,
            )
            elapsed = round(__import__("time").monotonic() - t0, 2)
            logger.info(
                "Excel split export: %d parts in %.1fs",
                len(results), elapsed,
            )
            # Return path of first part so callers get a usable path
            return results[0].path

        if safe_mode:
            result = safe_excel_write(
                workbook,
                path,
                fallback_to_csv=fallback_to_csv,
                no_excel_fallback=no_excel_fallback,
            )
        else:
            # Legacy direct write (kept for backward compat)
            out = path.with_suffix(".xlsx")
            with pd.ExcelWriter(str(out), engine="openpyxl") as writer:
                for sheet_name, df in workbook.items():
                    df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
                    DataStorage._autosize_worksheet(writer.sheets[sheet_name[:31]])
            return out

        elapsed = round(__import__("time").monotonic() - t0, 2)
        if result.fallback_used:
            logger.warning(
                "Excel export failed → CSV fallback used: %s (%.1fs)",
                result.path, elapsed,
            )
        else:
            logger.info(
                "Excel export: %d rows, %d sheets → %s (%.1fs)",
                result.rows_written, len(result.sheets), result.path, elapsed,
            )
        return result.path

    @staticmethod
    def _save_sqlite(data: List[Dict[str, Any]], path: Path) -> None:
        out = path.with_suffix(".db")
        df = DataStorage._flatten(data)
        with sqlite3.connect(out) as conn:
            conn.execute(SQLITE_CREATE_PRODUCTS)
            df.to_sql("products", conn, if_exists="append", index=False)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_source ON products(source)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_category ON products(category)"
            )

    # ------------------------------------------------------------------
    # Async database exports
    # ------------------------------------------------------------------

    @staticmethod
    async def save_postgres(
        data: List[Dict[str, Any]],
        db_url: str,
        table: str = "products",
    ) -> int:
        """Bulk-insert *data* into PostgreSQL. Returns rows inserted."""
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError(
                "asyncpg is required for PostgreSQL export: pip install asyncpg"
            ) from exc

        conn = await asyncpg.connect(db_url)
        try:
            await conn.execute(POSTGRES_CREATE_PRODUCTS)
            rows = DataStorage._to_db_rows(data)
            if not rows:
                return 0

            inserted = 0
            async with conn.transaction():
                for row in rows:
                    try:
                        await conn.execute(
                            f"""INSERT INTO {table}
                                (url, name, price, raw_price, vendor, part_number,
                                 image_url, stock_status, category, source,
                                 description, specifications, variants)
                                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                                ON CONFLICT (url) DO UPDATE SET
                                    price        = EXCLUDED.price,
                                    stock_status = EXCLUDED.stock_status,
                                    scraped_at   = NOW()
                            """,
                            row["url"],
                            row.get("name"),
                            row.get("price"),
                            row.get("raw_price", ""),
                            row.get("vendor", ""),
                            row.get("part_number", ""),
                            row.get("image_url", ""),
                            row.get("stock_status", "unknown"),
                            row.get("category", ""),
                            row.get("source", ""),
                            row.get("description", ""),
                            row.get("specifications"),
                            row.get("variants"),
                        )
                        inserted += 1
                    except Exception as exc:
                        logger.warning("Skipped row (url=%s): %s", row.get("url"), exc)
            logger.info("PostgreSQL: upserted %d rows into '%s'", inserted, table)
            return inserted
        finally:
            await conn.close()

    @staticmethod
    async def save_mysql(
        data: List[Dict[str, Any]],
        db_url: str,
        table: str = "products",
    ) -> int:
        """Bulk-insert *data* into MySQL. Returns rows inserted."""
        try:
            import aiomysql
        except ImportError as exc:
            raise RuntimeError(
                "aiomysql is required for MySQL export: pip install aiomysql"
            ) from exc

        parsed = urlparse(db_url)
        conn = await aiomysql.connect(
            host=parsed.hostname or "localhost",
            port=parsed.port or 3306,
            user=parsed.username or "root",
            password=parsed.password or "",
            db=parsed.path.lstrip("/"),
            charset="utf8mb4",
            autocommit=False,
        )
        try:
            async with conn.cursor() as cur:
                await cur.execute(MYSQL_CREATE_PRODUCTS)
                rows = DataStorage._to_db_rows(data)
                if not rows:
                    return 0

                sql = f"""
                    INSERT INTO {table}
                        (url, name, price, raw_price, vendor, part_number,
                         image_url, stock_status, category, source,
                         description, specifications, variants)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        price        = VALUES(price),
                        stock_status = VALUES(stock_status)
                """
                batch = [
                    (
                        r["url"],
                        r.get("name"),
                        r.get("price"),
                        r.get("raw_price", ""),
                        r.get("vendor", ""),
                        r.get("part_number", ""),
                        r.get("image_url", ""),
                        r.get("stock_status", "unknown"),
                        r.get("category", ""),
                        r.get("source", ""),
                        r.get("description", ""),
                        json.dumps(r.get("specifications") or {}, ensure_ascii=False),
                        json.dumps(r.get("variants") or [], ensure_ascii=False),
                    )
                    for r in rows
                ]
                await cur.executemany(sql, batch)
                await conn.commit()
                inserted = cur.rowcount
                logger.info("MySQL: inserted %d rows into '%s'", inserted, table)
                return inserted
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_excel_workbook(
        data: List[Dict[str, Any]],
        *,
        site_config: Optional[Dict[str, Any]] = None,
        run_metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, pd.DataFrame]:
        site_cfg = site_config or {}
        meta = DataStorage._normalise_run_metadata(data, site_cfg, run_metadata)
        # Note: NLP enrichment already applied in save() before this point
        nlp_data = data
        products_df = DataStorage._build_products_sheet(nlp_data, site_cfg, meta)
        aggregated_df = DataStorage.aggregate_products(products_df.copy())
        vendors_df = DataStorage._build_vendors_sheet(nlp_data, site_cfg)
        compatibility_df = DataStorage._build_compatibility_sheet(nlp_data, meta["scraped_at"])
        categories_df = DataStorage._build_categories_sheet(nlp_data)
        nlp_df = DataStorage._build_nlp_sheet(nlp_data)
        metadata_df = pd.DataFrame(
            [
                {
                    "run_id": meta["run_id"],
                    "started_at": meta["started_at"],
                    "completed_at": meta["completed_at"],
                    "total_products": meta["total_products"],
                    "sites_scraped": meta["sites_scraped"],
                    "filters_applied": meta["filters_applied"],
                }
            ]
        )
        return {
            "products": products_df,
            "aggregated_prices": aggregated_df,
            "vendors": vendors_df,
            "compatibility": compatibility_df,
            "categories": categories_df,
            "nlp_enrichment": nlp_df,
            "scrape_metadata": metadata_df,
        }

    @staticmethod
    def _normalise_run_metadata(
        data: List[Dict[str, Any]],
        site_config: Dict[str, Any],
        run_metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        vendor_ids = sorted(
            {
                str(
                    row.get("vendor_id")
                    or row.get("source")
                    or site_config.get("site_id")
                    or "site"
                )
                for row in data
            }
        )
        meta = {
            "run_id": f"{vendor_ids[0] if vendor_ids else 'run'}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
            "started_at": now,
            "completed_at": now,
            "total_products": len(data),
            "sites_scraped": ", ".join(vendor_ids),
            "filters_applied": "",
        }
        if run_metadata:
            meta.update({k: v for k, v in run_metadata.items() if v is not None})
        meta["scraped_at"] = meta.get("completed_at", now)
        return meta

    @staticmethod
    def _build_products_sheet(
        data: List[Dict[str, Any]],
        site_config: Dict[str, Any],
        meta: Dict[str, Any],
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        default_currency = site_config.get("currency", "EGP")
        site_vendor_name = site_config.get("display_name") or site_config.get("site_id") or ""

        for row in data:
            vendor_id = str(
                row.get("vendor_id")
                or row.get("source")
                or site_config.get("site_id")
                or "site"
            )
            product_url = str(row.get("url", ""))
            part_number = str(row.get("part_number", "") or "")
            oem_refs = row.get("oem_references", "")
            if isinstance(oem_refs, list):
                oem_refs = ", ".join(str(item) for item in oem_refs if item)
            elif not oem_refs and part_number:
                oem_refs = part_number

            rows.append(
                {
                    "product_id": row.get("product_id")
                    or DataStorage._product_id(product_url, vendor_id),
                    "part_name": row.get("name", ""),
                    "part_number": part_number,
                    "brand": row.get("vendor", ""),
                    "category": row.get("category", ""),
                    "subcategory": row.get("subcategory", ""),
                    "price_egp": row.get("price"),
                    "price_raw": row.get("raw_price", ""),
                    "currency": row.get("currency") or default_currency,
                    "vendor_name": row.get("vendor_name") or site_vendor_name or vendor_id,
                    "vendor_id": vendor_id,
                    "stock_status": row.get("stock_status", "unknown"),
                    "product_url": product_url,
                    "image_url": row.get("image_url", ""),
                    "scraped_at": row.get("scraped_at") or meta["scraped_at"],
                    "description": row.get("description", ""),
                    "specifications": json.dumps(
                        row.get("specifications") or {},
                        ensure_ascii=False,
                    ),
                    "compatibility_text": row.get("compatibility_text", ""),
                    "oem_references": oem_refs or "",
                    "notes": row.get("notes", ""),
                    # NLP-enriched fields
                    "language": row.get("language", ""),
                    "topic_category": row.get("topic_category", ""),
                    "keywords": row.get("keywords", ""),
                    "ai_summary": row.get("ai_summary", ""),
                }
            )

        columns = [
            "product_id",
            "part_name",
            "part_number",
            "brand",
            "category",
            "subcategory",
            "price_egp",
            "price_raw",
            "currency",
            "vendor_name",
            "vendor_id",
            "stock_status",
            "product_url",
            "image_url",
            "scraped_at",
            "description",
            "specifications",
            "compatibility_text",
            "oem_references",
            "notes",
            "language",
            "topic_category",
            "keywords",
            "ai_summary",
        ]
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def aggregate_products(df: pd.DataFrame) -> pd.DataFrame:
        """Group products by a normalized identifier and compute price stats."""
        columns = [
            "canonical_id",
            "part_number",
            "part_name",
            "category",
            "avg_price_egp",
            "min_price_egp",
            "max_price_egp",
            "vendor_count",
            "vendors",
            "vendor_prices",
            "last_updated",
        ]
        if df.empty:
            return pd.DataFrame(columns=columns)

        working = df.copy()
        working["_part_number_clean"] = working["part_number"].fillna("").map(clean_part_number)
        working["_name_key"] = working["part_name"].fillna("").map(
            lambda value: to_slug(normalise_arabic(str(value)))[:120]
        )
        working["_match_key"] = working.apply(
            lambda row: row["_part_number_clean"] or row["_name_key"],
            axis=1,
        )
        working = working[working["_match_key"].astype(str) != ""]
        if working.empty:
            return pd.DataFrame(columns=columns)

        working["_canonical_id"] = working.apply(
            lambda row: generate_canonical_id(row["_match_key"], row.get("category", "")),
            axis=1,
        )

        rows: List[Dict[str, Any]] = []
        for canonical_id, group in working.groupby("_canonical_id"):
            prices = pd.to_numeric(group["price_egp"], errors="coerce").dropna()
            vendors = sorted(str(v) for v in group["vendor_id"].dropna().astype(str).unique() if v)
            vendor_prices: Dict[str, float] = {}
            for _, entry in group.iterrows():
                vendor = str(entry.get("vendor_id") or "").strip()
                price = entry.get("price_egp")
                if vendor and pd.notna(price):
                    vendor_prices[vendor] = float(price)

            part_name_mode = group["part_name"].mode()
            category_mode = group["category"].mode()
            cleaned_part_numbers = [value for value in group["_part_number_clean"] if value]
            rows.append(
                {
                    "canonical_id": canonical_id,
                    "part_number": cleaned_part_numbers[0] if cleaned_part_numbers else "",
                    "part_name": part_name_mode.iloc[0] if not part_name_mode.empty else group["part_name"].iloc[0],
                    "category": category_mode.iloc[0] if not category_mode.empty else group["category"].iloc[0],
                    "avg_price_egp": float(prices.mean()) if not prices.empty else None,
                    "min_price_egp": float(prices.min()) if not prices.empty else None,
                    "max_price_egp": float(prices.max()) if not prices.empty else None,
                    "vendor_count": len(vendors),
                    "vendors": ", ".join(vendors),
                    "vendor_prices": json.dumps(vendor_prices, ensure_ascii=False),
                    "last_updated": group["scraped_at"].max(),
                }
            )
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _build_vendors_sheet(
        data: List[Dict[str, Any]],
        site_config: Dict[str, Any],
    ) -> pd.DataFrame:
        vendor_rows: Dict[str, Dict[str, Any]] = {}
        for row in data:
            vendor_id = str(
                row.get("vendor_id")
                or row.get("source")
                or site_config.get("site_id")
                or "site"
            )
            vendor_rows.setdefault(
                vendor_id,
                {
                    "vendor_id": vendor_id,
                    "vendor_full_name": row.get("vendor_name")
                    or site_config.get("display_name")
                    or vendor_id,
                    "website_url": row.get("site_url") or site_config.get("base_url", ""),
                    "platform_type": row.get("platform_type")
                    or site_config.get("platform_type")
                    or site_config.get("type", "custom"),
                    "currency": row.get("currency") or site_config.get("currency", "EGP"),
                    "shipping_notes": row.get("shipping_notes")
                    or site_config.get("shipping_notes", ""),
                    "reliability_score": row.get("reliability_score")
                    if row.get("reliability_score") is not None
                    else site_config.get("reliability_score"),
                },
            )
        rows = [vendor_rows[key] for key in sorted(vendor_rows)]
        columns = [
            "vendor_id",
            "vendor_full_name",
            "website_url",
            "platform_type",
            "currency",
            "shipping_notes",
            "reliability_score",
        ]
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _build_compatibility_sheet(
        data: List[Dict[str, Any]],
        scraped_at: str,
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for row in data:
            compat = row.get("compatibility")
            if not compat and row.get("compatibility_parsed"):
                try:
                    compat = json.loads(row["compatibility_parsed"])
                except (TypeError, json.JSONDecodeError):
                    compat = []
            if not isinstance(compat, list):
                continue
            vendor_id = str(row.get("vendor_id") or row.get("source") or "site")
            product_id = row.get("product_id") or DataStorage._product_id(
                str(row.get("url", "")),
                vendor_id,
            )
            for index, entry in enumerate(compat, start=1):
                if not isinstance(entry, dict):
                    continue
                rows.append(
                    {
                        "compat_id": f"{product_id}_compat_{index}",
                        "product_id": product_id,
                        "make": entry.get("make", ""),
                        "model": entry.get("model", ""),
                        "year_start": entry.get("year_start"),
                        "year_end": entry.get("year_end"),
                        "engine": entry.get("engine", ""),
                        "notes": entry.get("notes", ""),
                    }
                )
        columns = [
            "compat_id",
            "product_id",
            "make",
            "model",
            "year_start",
            "year_end",
            "engine",
            "notes",
        ]
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _build_categories_sheet(data: List[Dict[str, Any]]) -> pd.DataFrame:
        categories = sorted({str(row.get("category", "")).strip() for row in data if row.get("category")})
        rows = [
            {
                "category_id": sha1(name.encode("utf-8")).hexdigest()[:12],
                "category_name": name,
                "parent_id": "",
            }
            for name in categories
        ]
        return pd.DataFrame(rows, columns=["category_id", "category_name", "parent_id"])

    @staticmethod
    def _build_nlp_sheet(data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Build a dedicated NLP enrichment sheet for the Excel workbook."""
        columns = [
            "product_id",
            "part_name",
            "language",
            "topic_category",
            "keywords",
            "ai_summary",
        ]
        rows: List[Dict[str, Any]] = []
        for row in data:
            product_url = str(row.get("url", ""))
            vendor_id = str(row.get("vendor_id") or row.get("source") or "site")
            rows.append(
                {
                    "product_id": row.get("product_id")
                    or DataStorage._product_id(product_url, vendor_id),
                    "part_name": row.get("name", ""),
                    "language": row.get("language", ""),
                    "topic_category": row.get("topic_category", ""),
                    "keywords": row.get("keywords", ""),
                    "ai_summary": row.get("ai_summary", ""),
                }
            )
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _autosize_worksheet(worksheet: Any) -> None:
        for col in worksheet.columns:
            max_len = max((len(str(c.value)) for c in col if c.value is not None), default=10)
            worksheet.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    @staticmethod
    def _product_id(product_url: str, vendor_id: str) -> str:
        digest = sha1(f"{vendor_id}|{product_url}".encode("utf-8")).hexdigest()[:16]
        return f"{vendor_id}_{digest}"

    @staticmethod
    def _flatten(data: List[Dict[str, Any]]) -> pd.DataFrame:
        """Flatten nested dicts/lists into JSON strings for flat file formats."""
        flat = []
        for row in data:
            record: Dict[str, Any] = {}
            for key, val in row.items():
                if isinstance(val, (dict, list)):
                    record[key] = json.dumps(val, ensure_ascii=False)
                else:
                    record[key] = val
            flat.append(record)
        return pd.DataFrame(flat)

    @staticmethod
    def _to_db_rows(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out rows without a URL (required for DB unique key)."""
        return [row for row in data if row.get("url")]
