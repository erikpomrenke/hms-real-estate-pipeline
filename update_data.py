"""
HMS Kaupskrá fasteigna - automated ETL pipeline
=================================================
Downloads the Icelandic property-transaction register published by HMS,
cleans/transforms it, and pushes it into a Google Sheet that Tableau Public
can auto-refresh from every 24 hours.

Designed to run headlessly (e.g. in GitHub Actions) once a day.

Env vars expected:
  GOOGLE_SERVICE_ACCOUNT_JSON  - full JSON key of a Google service account,
                                  as a single-line string (store as a GitHub secret)
  SPREADSHEET_ID                - the ID of the target Google Sheet
                                  (the long string in the sheet's URL)
"""

import io
import json
import os
import sys
from datetime import datetime, timezone

import pandas as pd
import requests

# gspread / google-auth are only needed when actually pushing to Sheets;
# imported lazily inside push_to_google_sheets() so the transform logic can
# be tested/run without those packages installed.

SOURCE_URL = "https://frs3o1zldvgn.objectstorage.eu-frankfurt-1.oci.customer-oci.com/n/frs3o1zldvgn/b/public_data_for_download/o/kaupskra.csv"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# Columns we actually want for analysis (see HMS's "eigendalýsing" data dictionary).
# We drop pure ID/key columns (faerslunumer, emnr, skjalanumer, fastnum, heinum, fepilog)
# since they add size without adding analytical value for a dashboard.
KEEP_COLUMNS = [
    "heimilisfang",
    "postnr",
    "svfn",
    "sveitarfelag",
    "utgdag",
    "thinglystdags",
    "kaupverd",
    "fasteignamat",
    "brunabotamat_gildandi",
    "byggar",
    "einflm",
    "lod_flm",
    "lod_flmein",
    "fjherb",
    "tegund",
    "fullbuid",
    "onothaefur_samningur",
]

# Google Sheets hard limit is 10,000,000 cells per spreadsheet (across all tabs).
# We stay well under that; if the source ever grows past this, drop older years
# from the granular tab (the monthly summary tab is unaffected either way).
MAX_CELLS = 9_000_000


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)


def download_source_csv() -> pd.DataFrame:
    log(f"Downloading source CSV from {SOURCE_URL}")
    resp = requests.get(SOURCE_URL, timeout=180)
    resp.raise_for_status()
    size_mb = len(resp.content) / (1024 * 1024)
    log(f"Downloaded {size_mb:.1f} MB")

    # HMS publishes this as a semicolon or comma delimited CSV with Icelandic
    # characters; latin1/utf-8 both show up in the wild for this feed, so we
    # try utf-8 first and fall back to latin1.
    raw = resp.content
    for encoding in ("utf-8", "utf-8-sig", "latin1"):
        try:
            df = pd.read_csv(io.BytesIO(raw), sep=None, engine="python", encoding=encoding)
            log(f"Parsed CSV with encoding={encoding}, shape={df.shape}")
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    raise RuntimeError("Could not parse source CSV with any known encoding")


def clean_and_transform(df: pd.DataFrame) -> pd.DataFrame:
    log("Cleaning and transforming data")

    # normalize column names in case of casing/whitespace differences
    df.columns = [c.strip().lower() for c in df.columns]

    missing = [c for c in KEEP_COLUMNS if c not in df.columns]
    if missing:
        log(f"WARNING: expected columns missing from source, continuing without them: {missing}")

    cols = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[cols].copy()

    # parse dates
    for date_col in ("utgdag", "thinglystdags"):
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # derived fields for trend analysis
    if "thinglystdags" in df.columns:
        df["ar"] = df["thinglystdags"].dt.year
        df["manudur"] = df["thinglystdags"].dt.month
        df["ar_manudur"] = df["thinglystdags"].dt.to_period("M").astype(str)

    if "kaupverd" in df.columns and "einflm" in df.columns:
        df["verd_per_fm"] = (df["kaupverd"] / df["einflm"]).replace([float("inf"), float("-inf")], pd.NA)

    # drop rows with no usable date or price - useless for trend charts
    before = len(df)
    df = df.dropna(subset=[c for c in ("thinglystdags", "kaupverd") if c in df.columns])
    log(f"Dropped {before - len(df)} rows missing date/price ({len(df)} remain)")

    # cap cell count by trimming oldest years if needed, rather than failing
    if "ar" in df.columns:
        n_cols = len(df.columns)
        while len(df) * n_cols > MAX_CELLS:
            oldest_year = df["ar"].min()
            df = df[df["ar"] != oldest_year]
            log(f"Trimmed year {oldest_year} to stay under Google Sheets cell limit")

    return df.reset_index(drop=True)


def build_monthly_summary(df: pd.DataFrame) -> pd.DataFrame:
    log("Building monthly summary table")
    usable = df[df.get("onothaefur_samningur", 0) == 0] if "onothaefur_samningur" in df.columns else df

    group_cols = [c for c in ("ar_manudur", "sveitarfelag", "tegund") if c in usable.columns]
    if not group_cols:
        return pd.DataFrame()

    summary = (
        usable.groupby(group_cols)
        .agg(
            fjoldi_samninga=("kaupverd", "count"),
            medalverd=("kaupverd", "mean"),
            midgildi_verd=("kaupverd", "median"),
            medal_verd_per_fm=("verd_per_fm", "mean") if "verd_per_fm" in usable.columns else ("kaupverd", "mean"),
        )
        .reset_index()
    )
    summary = summary.round(0)
    return summary


def write_dataframe_in_chunks(ws, df: pd.DataFrame, max_cells_per_request: int = 400_000) -> None:
    """
    Write a dataframe to a worksheet in batches instead of one giant call.
    A single ws.update() with millions of cells reliably triggers a
    generic "APIError: 500 Internal error" from the Sheets API - this
    keeps each request small enough to succeed, and retries transient
    failures with backoff.
    """
    import time

    import gspread

    n_cols = max(len(df.columns), 1)
    rows_per_chunk = max(1, max_cells_per_request // n_cols)

    # header row first
    ws.update(range_name="A1", values=[df.columns.tolist()])

    values = df.values.tolist()
    total_rows = len(values)
    for start in range(0, total_rows, rows_per_chunk):
        chunk = values[start : start + rows_per_chunk]
        start_row = start + 2  # +1 for header, +1 because sheets are 1-indexed
        range_name = f"A{start_row}"

        attempt = 0
        while True:
            attempt += 1
            try:
                ws.update(range_name=range_name, values=chunk)
                break
            except gspread.exceptions.APIError as exc:
                if attempt >= 4:
                    raise
                wait = 5 * attempt
                log(f"  Sheets API error on rows {start_row}-{start_row + len(chunk) - 1}, "
                    f"retrying in {wait}s (attempt {attempt}/4): {exc}")
                time.sleep(wait)

        log(f"  Wrote rows {start_row}-{start_row + len(chunk) - 1} of {total_rows + 1}")
        time.sleep(1.1)  # stay comfortably under Sheets API per-minute write quota


def push_to_google_sheets(tables: dict) -> None:
    creds_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID")

    if not creds_json or not spreadsheet_id:
        log("GOOGLE_SERVICE_ACCOUNT_JSON or SPREADSHEET_ID not set - writing local CSVs instead of pushing")
        for name, df in tables.items():
            out_path = f"{name}.csv"
            df.to_csv(out_path, index=False)
            log(f"Wrote {out_path} ({len(df)} rows)")
        return

    import gspread
    from google.oauth2.service_account import Credentials

    log("Authenticating with Google Sheets")
    info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)

    for sheet_name, df in tables.items():
        log(f"Pushing tab '{sheet_name}' ({len(df)} rows x {len(df.columns)} cols)")
        try:
            ws = sh.worksheet(sheet_name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=sheet_name, rows=str(len(df) + 10), cols=str(len(df.columns) + 5))

        # gspread needs everything as strings/numbers, not NaT/NaN
        clean_df = df.copy()
        for col in clean_df.columns:
            if pd.api.types.is_datetime64_any_dtype(clean_df[col]):
                clean_df[col] = clean_df[col].dt.strftime("%Y-%m-%d")
        clean_df = clean_df.fillna("")

        write_dataframe_in_chunks(ws, clean_df)

    # stamp a "last updated" marker so the Tableau dashboard can show data freshness
    try:
        meta_ws = sh.worksheet("_metadata")
        meta_ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        meta_ws = sh.add_worksheet(title="_metadata", rows="5", cols="2")
    meta_ws.update([["last_updated_utc", datetime.now(timezone.utc).isoformat()]])

    log("Push to Google Sheets complete")


def main() -> int:
    try:
        raw_df = download_source_csv()
        clean_df = clean_and_transform(raw_df)
        monthly_df = build_monthly_summary(clean_df)

        tables = {"transactions": clean_df}
        if not monthly_df.empty:
            tables["monthly_summary"] = monthly_df

        push_to_google_sheets(tables)
        log("Done.")
        return 0
    except Exception as exc:  # noqa: BLE001
        log(f"FAILED: {exc}")
        raise


if __name__ == "__main__":
    sys.exit(main())