from __future__ import annotations

import html
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

app = FastAPI(title="Scrapyard Dashboard")

OUTPUT_DIR = Path("output")


def _read_sheet(path: Path, sheet_name: str) -> pd.DataFrame:
    try:
        return pd.read_excel(path, sheet_name=sheet_name)
    except Exception:
        return pd.DataFrame()


def _candidate_files(site: str) -> List[Path]:
    exact = OUTPUT_DIR / f"{site}.xlsx"
    files = list(OUTPUT_DIR.glob(f"{site}_*.xlsx"))
    if exact.exists():
        files.append(exact)
    return sorted(set(files), key=lambda path: path.stat().st_mtime, reverse=True)


def get_latest_file(site: str) -> Optional[Path]:
    files = _candidate_files(site)
    return files[0] if files else None


def get_site_ids() -> List[str]:
    site_ids = set()
    for file in OUTPUT_DIR.glob("*.xlsx"):
        stem = file.stem
        if (
            stem.startswith("all_vendors_")
            or stem == "all_vendors"
            or stem.startswith("all_sites_")
            or stem == "all_sites"
        ):
            continue
        site_ids.add(stem.split("_")[0] if "_" in stem else stem)

    combined_file = _latest_combined_file()
    if combined_file:
        combined_df = _read_sheet(combined_file, "products")
        if not combined_df.empty and "vendor_id" in combined_df.columns:
            for vendor_id in combined_df["vendor_id"].dropna().astype(str):
                vendor_id = vendor_id.strip()
                if vendor_id:
                    site_ids.add(vendor_id)

    return sorted(site_ids)


def _combined_files() -> List[Path]:
    files = _candidate_files("all_sites") + _candidate_files("all_vendors")
    return sorted(set(files), key=lambda path: path.stat().st_mtime, reverse=True)


def _latest_combined_file() -> Optional[Path]:
    files = _combined_files()
    return files[0] if files else None


def _load_products_for_site(site: str) -> Tuple[pd.DataFrame, Optional[Path]]:
    aggregate_file = _latest_combined_file()
    if aggregate_file:
        aggregate_df = _read_sheet(aggregate_file, "products")
        if not aggregate_df.empty and "vendor_id" in aggregate_df.columns:
            site_df = aggregate_df[aggregate_df["vendor_id"].astype(str) == site].copy()
            if not site_df.empty:
                return site_df, aggregate_file

    site_file = get_latest_file(site)
    if not site_file:
        return pd.DataFrame(), None
    return _read_sheet(site_file, "products"), site_file


def _history_frames_for_site(site: str) -> List[Tuple[Path, pd.DataFrame]]:
    history: List[Tuple[Path, pd.DataFrame]] = []

    for file in sorted(_combined_files(), key=lambda path: path.stat().st_mtime):
        df = _read_sheet(file, "products")
        if df.empty or "vendor_id" not in df.columns:
            continue
        site_df = df[df["vendor_id"].astype(str) == site].copy()
        if not site_df.empty:
            history.append((file, site_df))

    if len(history) >= 2:
        return history

    fallback: List[Tuple[Path, pd.DataFrame]] = []
    for file in sorted(_candidate_files(site), key=lambda path: path.stat().st_mtime):
        df = _read_sheet(file, "products")
        if not df.empty:
            fallback.append((file, df))
    return fallback


def _compute_trends(site: str) -> pd.DataFrame:
    history = _history_frames_for_site(site)
    if len(history) < 2:
        return pd.DataFrame()

    _, old_df = history[-2]
    _, new_df = history[-1]
    if "product_id" not in old_df.columns or "product_id" not in new_df.columns:
        return pd.DataFrame()

    merged = new_df.merge(old_df, on="product_id", suffixes=("_new", "_old"))
    if "price_egp_new" not in merged.columns or "price_egp_old" not in merged.columns:
        return pd.DataFrame()

    merged["price_change"] = merged["price_egp_new"] - merged["price_egp_old"]
    changed = merged[merged["price_change"].abs() > 0.01].copy()
    if changed.empty:
        return changed

    if "part_name_new" not in changed.columns and "part_name" in changed.columns:
        changed["part_name_new"] = changed["part_name"]
    if "vendor_id_new" not in changed.columns and "vendor_id" in changed.columns:
        changed["vendor_id_new"] = changed["vendor_id"]

    columns = [
        "product_id",
        "part_name_new",
        "vendor_id_new",
        "price_egp_old",
        "price_egp_new",
        "price_change",
    ]
    available = [column for column in columns if column in changed.columns]
    return changed[available].sort_values("price_change", ascending=False)


def _apply_filters(
    df: pd.DataFrame,
    *,
    search: Optional[str],
    min_price: Optional[float],
    max_price: Optional[float],
) -> pd.DataFrame:
    filtered = df.copy()
    if search and "part_name" in filtered.columns:
        filtered = filtered[
            filtered["part_name"].fillna("").str.contains(search, case=False, na=False)
        ]
    if min_price is not None and "price_egp" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["price_egp"], errors="coerce") >= min_price]
    if max_price is not None and "price_egp" in filtered.columns:
        filtered = filtered[pd.to_numeric(filtered["price_egp"], errors="coerce") <= max_price]
    return filtered


def _render_table(records: Iterable[dict], columns: List[str]) -> str:
    rows = []
    for record in records:
        cells = "".join(
            f"<td>{html.escape('' if record.get(column) is None else str(record.get(column)))}</td>"
            for column in columns
        )
        rows.append(f"<tr>{cells}</tr>")
    if not rows:
        rows.append(f"<tr><td colspan='{len(columns)}'>No rows found.</td></tr>")

    header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body = "".join(rows)
    return (
        "<table border='1' cellspacing='0' cellpadding='6'>"
        f"<thead><tr>{header}</tr></thead>"
        f"<tbody>{body}</tbody>"
        "</table>"
    )


def _render_trend_chart(trends: pd.DataFrame) -> str:
    if trends.empty or "price_change" not in trends.columns:
        return "<p>No price changes detected yet.</p>"

    top = trends.head(8).copy()
    max_abs = max(abs(float(value)) for value in top["price_change"]) or 1.0
    width = 760
    row_height = 34
    chart_height = len(top) * row_height + 28
    baseline = 330
    bar_span = 260
    items = [
        f"<line x1='{baseline}' y1='10' x2='{baseline}' y2='{chart_height - 12}' stroke='#333' stroke-width='2' />"
    ]

    for index, (_, row) in enumerate(top.iterrows(), start=1):
        change = float(row["price_change"])
        bar_width = max(4, int(bar_span * abs(change) / max_abs))
        y = index * row_height
        x = baseline - bar_width if change < 0 else baseline
        color = "#b42318" if change > 0 else "#027a48"
        label = html.escape(str(row.get("part_name_new", row.get("product_id", "")))[:42])
        value = f"{change:+.2f} EGP"
        items.append(f"<text x='8' y='{y + 16}' font-size='12'>{label}</text>")
        items.append(
            f"<rect x='{x}' y='{y}' width='{bar_width}' height='16' fill='{color}' opacity='0.82' />"
        )
        value_x = x + bar_width + 8 if change >= 0 else x - 88
        items.append(f"<text x='{value_x}' y='{y + 13}' font-size='12'>{html.escape(value)}</text>")

    return (
        "<svg "
        f"width='{width}' height='{chart_height}' viewBox='0 0 {width} {chart_height}' "
        "xmlns='http://www.w3.org/2000/svg'>"
        + "".join(items)
        + "</svg>"
    )


def _page_shell(title: str, body: str) -> HTMLResponse:
    markup = f"""
    <html>
      <head>
        <title>{html.escape(title)}</title>
        <style>
          body {{ font-family: Segoe UI, Arial, sans-serif; margin: 24px; color: #1f2937; }}
          a {{ color: #0f62fe; text-decoration: none; }}
          a:hover {{ text-decoration: underline; }}
          .muted {{ color: #6b7280; }}
          .grid {{ display: grid; gap: 24px; }}
          .card {{ border: 1px solid #d0d5dd; border-radius: 10px; padding: 16px; }}
          .stats {{ display: flex; gap: 18px; flex-wrap: wrap; }}
          .stat {{ background: #f8fafc; border-radius: 8px; padding: 12px 14px; min-width: 140px; }}
          input {{ padding: 8px; margin-right: 8px; }}
          table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
          th {{ background: #f8fafc; text-align: left; }}
          td, th {{ border: 1px solid #e5e7eb; padding: 6px; vertical-align: top; }}
        </style>
      </head>
      <body>{body}</body>
    </html>
    """
    return HTMLResponse(markup)


@app.get("/", response_class=HTMLResponse)
async def home() -> HTMLResponse:
    sites = get_site_ids()
    links = "".join(
        f"<li><a href='/site/{html.escape(site)}'>{html.escape(site)}</a></li>"
        for site in sites
    ) or "<li>No site exports found yet.</li>"
    body = (
        "<h1>Scrapyard Dashboard</h1>"
        "<p class='muted'>Browse the latest exported data, filter products, and inspect price changes.</p>"
        "<div class='card'><h2>Sites</h2><ul>"
        f"{links}"
        "</ul>"
        "<p><a href='/aggregated'>View cross-vendor aggregated prices</a></p>"
        "</div>"
    )
    return _page_shell("Scrapyard Dashboard", body)


@app.get("/site/{site}", response_class=HTMLResponse)
async def site_dashboard(
    site: str,
    search: str | None = Query(None),
    min_price: float | None = None,
    max_price: float | None = None,
    limit: int = Query(100, ge=1, le=500),
) -> HTMLResponse:
    df, source_file = _load_products_for_site(site)
    if df.empty:
        return _page_shell(
            f"{site} | Scrapyard",
            f"<h1>{html.escape(site)}</h1><p>No data found for this site.</p>",
        )

    filtered = _apply_filters(
        df,
        search=search,
        min_price=min_price,
        max_price=max_price,
    )
    filtered = filtered.sort_values("scraped_at", ascending=False) if "scraped_at" in filtered.columns else filtered
    trends = _compute_trends(site)

    stats = [
        ("Rows", len(filtered)),
        ("Unique parts", filtered["product_id"].nunique() if "product_id" in filtered.columns else len(filtered)),
        ("Avg price", f"{pd.to_numeric(filtered['price_egp'], errors='coerce').dropna().mean():.2f} EGP" if "price_egp" in filtered.columns and not filtered.empty and pd.to_numeric(filtered["price_egp"], errors="coerce").dropna().size else "n/a"),
        ("Price changes", len(trends)),
    ]
    stats_html = "".join(
        f"<div class='stat'><div class='muted'>{html.escape(label)}</div><strong>{html.escape(str(value))}</strong></div>"
        for label, value in stats
    )

    form = f"""
    <form method="get">
      <input type="text" name="search" placeholder="Search part name" value="{html.escape(search or '')}" />
      <input type="number" step="0.01" name="min_price" placeholder="Min price" value="{'' if min_price is None else min_price}" />
      <input type="number" step="0.01" name="max_price" placeholder="Max price" value="{'' if max_price is None else max_price}" />
      <input type="number" name="limit" min="1" max="500" value="{limit}" />
      <button type="submit">Apply</button>
    </form>
    """

    table_columns = [
        column
        for column in [
            "part_name",
            "part_number",
            "brand",
            "category",
            "price_egp",
            "stock_status",
            "vendor_id",
            "product_url",
            "scraped_at",
        ]
        if column in filtered.columns
    ]
    rows = filtered.head(limit).fillna("").to_dict(orient="records")
    body = (
        f"<h1>{html.escape(site)}</h1>"
        f"<p class='muted'>Source workbook: {html.escape(source_file.name if source_file else 'n/a')}</p>"
        "<div class='grid'>"
        f"<div class='card'><div class='stats'>{stats_html}</div></div>"
        f"<div class='card'><h2>Filters</h2>{form}<p class='muted'>JSON API: <a href='/api/site/{html.escape(site)}'>/api/site/{html.escape(site)}</a></p></div>"
        f"<div class='card'><h2>Price Trend Chart</h2>{_render_trend_chart(trends)}</div>"
        f"<div class='card'><h2>Products</h2>{_render_table(rows, table_columns)}</div>"
        "</div>"
    )
    return _page_shell(f"{site} | Scrapyard", body)


@app.get("/aggregated", response_class=HTMLResponse)
async def aggregated_dashboard(limit: int = Query(100, ge=1, le=500)) -> HTMLResponse:
    file = _latest_combined_file()
    if not file:
        return _page_shell(
            "Aggregated Prices | Scrapyard",
            "<h1>Aggregated Prices</h1><p>No combined workbook found yet.</p>",
        )

    df = _read_sheet(file, "aggregated_prices")
    if df.empty:
        return _page_shell(
            "Aggregated Prices | Scrapyard",
            "<h1>Aggregated Prices</h1><p>The latest workbook does not contain aggregated data.</p>",
        )

    table_columns = [
        column
        for column in [
            "part_number",
            "part_name",
            "category",
            "avg_price_egp",
            "min_price_egp",
            "max_price_egp",
            "vendor_count",
            "vendors",
            "last_updated",
        ]
        if column in df.columns
    ]
    rows = df.sort_values("vendor_count", ascending=False).head(limit).fillna("").to_dict(orient="records")
    body = (
        "<h1>Aggregated Prices</h1>"
        f"<p class='muted'>Source workbook: {html.escape(file.name)}</p>"
        "<div class='card'>"
        f"{_render_table(rows, table_columns)}"
        "<p class='muted'>JSON API: <a href='/api/aggregated/latest'>/api/aggregated/latest</a></p>"
        "</div>"
    )
    return _page_shell("Aggregated Prices | Scrapyard", body)


@app.get("/api/site/{site}")
async def site_products_json(
    site: str,
    search: str | None = Query(None),
    min_price: float | None = None,
    max_price: float | None = None,
):
    df, source_file = _load_products_for_site(site)
    if df.empty:
        return {"error": "No data found"}
    filtered = _apply_filters(df, search=search, min_price=min_price, max_price=max_price)
    return {
        "site": site,
        "source_file": source_file.name if source_file else None,
        "count": len(filtered),
        "records": filtered.to_dict(orient="records"),
    }


@app.get("/api/aggregated/latest")
async def aggregated_latest():
    file = _latest_combined_file()
    if not file:
        return {"error": "No aggregated workbook found"}
    df = _read_sheet(file, "aggregated_prices")
    return {
        "source_file": file.name,
        "count": len(df),
        "records": df.to_dict(orient="records"),
    }


@app.get("/api/trends/{site}")
async def price_trends(site: str):
    history = _history_frames_for_site(site)
    if len(history) < 2:
        return {"error": "Not enough history"}
    trends = _compute_trends(site)
    return {
        "site": site,
        "count": len(trends),
        "records": trends.to_dict(orient="records"),
    }
