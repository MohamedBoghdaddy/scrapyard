"""
DataStorage: export scraped data to CSV, JSON, Excel, SQLite, PostgreSQL, or MySQL.

Sync formats (csv/json/excel/sqlite) use pandas.
Async DB formats (postgres/mysql) use asyncpg / aiomysql.

Includes QA validation: invalid products are logged and saved to _invalid.csv.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from hashlib import sha1
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd

from db.models import (
    POSTGRES_CREATE_PRODUCTS,
    MYSQL_CREATE_PRODUCTS,
    SQLITE_CREATE_PRODUCTS,
)

logger = logging.getLogger(__name__)

SyncFormat = Literal["csv", "json", "excel", "sqlite"]

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

        if fmt == "csv":
            DataStorage._save_csv(valid, path)
        elif fmt == "json":
            DataStorage._save_json(valid, path)
        elif fmt == "excel":
            DataStorage._save_excel(
                valid,
                path,
                site_config=site_config,
                run_metadata=run_metadata,
            )
        elif fmt == "sqlite":
            DataStorage._save_sqlite(valid, path)
        else:
            raise ValueError(
                "Unsupported format "
                f"'{fmt}'. Choose from: ['csv', 'json', 'excel', 'sqlite']"
            )
        logger.info("Saved %d records -> %s (%s)", len(valid), path, fmt)
        return path

    @staticmethod
    def _save_csv(data: List[Dict[str, Any]], path: Path) -> None:
        DataStorage._flatten(data).to_csv(
            path.with_suffix(".csv"), index=False, encoding="utf-8-sig"
        )

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
    ) -> None:
        out = path.with_suffix(".xlsx")
        workbook = DataStorage._build_excel_workbook(
            data,
            site_config=site_config,
            run_metadata=run_metadata,
        )
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            for sheet_name, df in workbook.items():
                df.to_excel(writer, index=False, sheet_name=sheet_name)
                DataStorage._autosize_worksheet(writer.sheets[sheet_name])

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
        products_df = DataStorage._build_products_sheet(data, site_cfg, meta)
        vendors_df = DataStorage._build_vendors_sheet(data, site_cfg)
        compatibility_df = DataStorage._build_compatibility_sheet(data, meta["scraped_at"])
        categories_df = DataStorage._build_categories_sheet(data)
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
            "vendors": vendors_df,
            "compatibility": compatibility_df,
            "categories": categories_df,
            "scrape_metadata": metadata_df,
        }

    @staticmethod
    def _normalise_run_metadata(
        data: List[Dict[str, Any]],
        site_config: Dict[str, Any],
        run_metadata: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        vendor_ids = sorted({str(row.get("source") or site_config.get("site_id") or "site") for row in data})
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
            vendor_id = str(row.get("source") or site_config.get("site_id") or "site")
            product_url = str(row.get("url", ""))
            part_number = str(row.get("part_number", "") or "")
            oem_refs = row.get("oem_references", "")
            if isinstance(oem_refs, list):
                oem_refs = ", ".join(str(item) for item in oem_refs if item)
            elif not oem_refs and part_number:
                oem_refs = part_number

            rows.append(
                {
                    "product_id": DataStorage._product_id(
                        product_url,
                        vendor_id,
                        meta["scraped_at"],
                    ),
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
                    "scraped_at": meta["scraped_at"],
                    "description": row.get("description", ""),
                    "specifications": json.dumps(
                        row.get("specifications") or {},
                        ensure_ascii=False,
                    ),
                    "compatibility_text": row.get("compatibility_text", ""),
                    "oem_references": oem_refs or "",
                    "notes": row.get("notes", ""),
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
        ]
        return pd.DataFrame(rows, columns=columns)

    @staticmethod
    def _build_vendors_sheet(
        data: List[Dict[str, Any]],
        site_config: Dict[str, Any],
    ) -> pd.DataFrame:
        vendor_ids = sorted({str(row.get("source") or site_config.get("site_id") or "site") for row in data})
        rows = [
            {
                "vendor_id": vendor_id,
                "vendor_full_name": site_config.get("display_name") or vendor_id,
                "website_url": site_config.get("base_url", ""),
                "platform_type": site_config.get("platform_type") or site_config.get("type", "custom"),
                "currency": site_config.get("currency", "EGP"),
                "shipping_notes": site_config.get("shipping_notes", ""),
                "reliability_score": site_config.get("reliability_score"),
            }
            for vendor_id in vendor_ids
        ]
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
            if not isinstance(compat, list):
                continue
            vendor_id = str(row.get("source") or "site")
            product_id = DataStorage._product_id(
                str(row.get("url", "")),
                vendor_id,
                scraped_at,
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
    def _autosize_worksheet(worksheet: Any) -> None:
        for col in worksheet.columns:
            max_len = max((len(str(c.value)) for c in col if c.value is not None), default=10)
            worksheet.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    @staticmethod
    def _product_id(product_url: str, vendor_id: str, scraped_at: str) -> str:
        digest = sha1(f"{vendor_id}|{product_url}|{scraped_at}".encode("utf-8")).hexdigest()[:12]
        date_tag = str(scraped_at)[:10].replace("-", "") or "run"
        return f"{vendor_id}_{digest}_{date_tag}"

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
