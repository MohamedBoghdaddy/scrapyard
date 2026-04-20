"""
DataStorage: export scraped data to CSV, JSON, Excel, SQLite, PostgreSQL, or MySQL.

Sync formats (csv/json/excel/sqlite) use pandas.
Async DB formats (postgres/mysql) use asyncpg / aiomysql.
"""
from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
from urllib.parse import urlparse

import pandas as pd

from db.models import (
    POSTGRES_CREATE_PRODUCTS,
    MYSQL_CREATE_PRODUCTS,
    SQLITE_CREATE_PRODUCTS,
)

logger = logging.getLogger(__name__)

SyncFormat = Literal["csv", "json", "excel", "sqlite"]

# Product columns that hold structured data (serialised to JSON for flat formats)
_JSON_COLS = {"specifications", "variants", "tags"}

# Canonical column order for DB inserts
_PRODUCT_COLS = [
    "url", "name", "price", "raw_price", "vendor", "part_number",
    "image_url", "stock_status", "category", "source",
    "description", "specifications", "variants",
]


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
    ) -> Path:
        """
        Write *data* to *filepath* in the chosen format.
        Returns the resolved output Path.
        """
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)

        if not data:
            logger.warning("save() called with empty data – skipping write")
            return path

        dispatch = {
            "csv": DataStorage._save_csv,
            "json": DataStorage._save_json,
            "excel": DataStorage._save_excel,
            "sqlite": DataStorage._save_sqlite,
        }
        handler = dispatch.get(fmt)
        if handler is None:
            raise ValueError(
                f"Unsupported format '{fmt}'. Choose from: {list(dispatch)}"
            )

        handler(data, path)
        logger.info("Saved %d records → %s (%s)", len(data), path, fmt)
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
    def _save_excel(data: List[Dict[str, Any]], path: Path) -> None:
        df = DataStorage._flatten(data)
        out = path.with_suffix(".xlsx")
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Products")
            ws = writer.sheets["Products"]
            for col in ws.columns:
                max_len = max(
                    (len(str(c.value)) for c in col if c.value), default=10
                )
                ws.column_dimensions[col[0].column_letter].width = min(
                    max_len + 4, 60
                )

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
        """
        Bulk-insert *data* into a PostgreSQL table using asyncpg.
        Creates the table automatically if it does not exist.
        Returns the number of rows inserted.
        """
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
                            row["name"],
                            row.get("price"),
                            row.get("raw_price", ""),
                            row.get("vendor", ""),
                            row.get("part_number", ""),
                            row.get("image_url", ""),
                            row.get("stock_status", "unknown"),
                            row.get("category", ""),
                            row.get("source", ""),
                            row.get("description", ""),
                            row.get("specifications"),   # JSONB
                            row.get("variants"),         # JSONB
                        )
                        inserted += 1
                    except Exception as exc:
                        logger.warning(
                            "Skipped row (url=%s): %s", row.get("url"), exc
                        )
            logger.info("PostgreSQL: inserted/upserted %d rows into '%s'", inserted, table)
            return inserted
        finally:
            await conn.close()

    @staticmethod
    async def save_mysql(
        data: List[Dict[str, Any]],
        db_url: str,
        table: str = "products",
    ) -> int:
        """
        Bulk-insert *data* into a MySQL table using aiomysql.
        Creates the table automatically if it does not exist.
        Returns the number of rows inserted.
        """
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
        """
        Prepare rows for DB insertion:
         – ensure 'url' exists (skip rows without it)
         – keep dict/list values as-is for asyncpg (JSONB) but serialise for MySQL
        """
        out = []
        for row in data:
            if not row.get("url"):
                continue
            out.append(row)
        return out
