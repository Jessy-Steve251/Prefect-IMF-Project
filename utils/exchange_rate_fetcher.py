"""
exchange_rate_fetcher.py
------------------------
Fetches exchange rate data from the IMF SDMX 2.1 API and resolves
currency codes via the REST Countries API.

Enhanced features:
  - Supports single-month AND date-range fetching
  - Chunked bulk fetching (yearly chunks) for 2000->present backfills
  - Currency cache persisted to disk between runs (currency_cache.json)
  - REST Countries lookups run in parallel (ThreadPoolExecutor)
  - All output goes through Prefect logger (not print)
  - Retry on REST Countries API failures (up to 3 attempts)
  - Force-refetch option to overwrite existing CSVs
"""

import os
import json
import time
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

from utils.config import (
    DATA_DIR,
    IMF_FLOW_REF,
    IMF_KEY,
    IMF_API_TIMEOUT_SEC,
    REST_COUNTRIES_TIMEOUT_SEC,
    REST_COUNTRIES_MAX_WORKERS,
    CURRENCY_CACHE_FILE,
    SPECIAL_CURRENCY_OVERRIDES,
)


# ---------------------------------------------------------------------------
# Persistent currency cache
# ---------------------------------------------------------------------------

def _load_cache() -> dict:
    if CURRENCY_CACHE_FILE.exists():
        try:
            with open(CURRENCY_CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_cache(cache: dict):
    CURRENCY_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CURRENCY_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


# Module-level cache
_currency_cache: dict = _load_cache()


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def last_month() -> datetime:
    """Returns a datetime object representing the first day of last month."""
    today = datetime.today()
    return (today.replace(day=1) - timedelta(days=1)).replace(day=1)


def last_month_ym_str() -> str:
    """Returns last month as 'YYYY_MM' (used in filenames)."""
    return last_month().strftime("%Y_%m")


def last_month_api_str() -> str:
    """Returns last month as 'YYYY-MM' (used in IMF API calls)."""
    return last_month().strftime("%Y-%m")


def build_month_list(start_year: int, start_month: int,
                     end_year: int = None, end_month: int = None) -> list[tuple[int, int]]:
    """
    Returns list of (year, month) tuples from start to end (inclusive).
    If end not specified, defaults to last month.
    """
    if end_year is None or end_month is None:
        today = datetime.now()
        end_year = today.year
        end_month = today.month - 1
        if end_month == 0:
            end_year -= 1
            end_month = 12

    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


# ---------------------------------------------------------------------------
# REST Countries API - currency code lookup
# ---------------------------------------------------------------------------

def _fetch_currency_from_api(country_code: str) -> str | None:
    url = f"https://restcountries.com/v3.1/alpha/{country_code}"
    req = urllib.request.Request(url, headers={"User-Agent": "Python-IMF-Pipeline/2.0"})

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=REST_COUNTRIES_TIMEOUT_SEC) as resp:
                if resp.getcode() == 200:
                    data = json.loads(resp.read().decode("utf-8"))
                    if data and "currencies" in data[0]:
                        return list(data[0]["currencies"].keys())[0]
        except Exception:
            if attempt < 2:
                time.sleep(1.5 ** attempt)
    return None


def resolve_currency_codes(country_codes: list, logger=None) -> dict:
    """
    Resolves currency codes for a list of country codes.
    Uses the persistent cache; only hits the API for unknown codes.
    """
    result = {}
    to_fetch = []

    for code in country_codes:
        if code in SPECIAL_CURRENCY_OVERRIDES:
            result[code] = SPECIAL_CURRENCY_OVERRIDES[code]
        elif code in _currency_cache:
            result[code] = _currency_cache[code]
        else:
            to_fetch.append(code)

    if to_fetch:
        if logger:
            logger.info(f"Fetching currency codes for {len(to_fetch)} new countries...")

        with ThreadPoolExecutor(max_workers=REST_COUNTRIES_MAX_WORKERS) as executor:
            futures = {executor.submit(_fetch_currency_from_api, code): code for code in to_fetch}
            for future in as_completed(futures):
                code = futures[future]
                currency = future.result()
                result[code] = currency
                _currency_cache[code] = currency

        _save_cache(_currency_cache)

        if logger:
            failed = [c for c in to_fetch if result.get(c) is None]
            if failed:
                logger.warning(f"Could not resolve currency for: {failed}")

    return result


# ---------------------------------------------------------------------------
# IMF SDMX API
# ---------------------------------------------------------------------------

def get_currency_data_from_imf(start_date: str, end_date: str,
                                logger=None, timeout: int = None) -> str | None:
    """
    Fetches XML exchange rate data from IMF SDMX 2.1 API.

    Args:
        start_date: 'YYYY-MM'
        end_date:   'YYYY-MM'
        timeout:    Override default timeout (useful for large date ranges)

    Returns:
        Raw XML string, or None on failure.
    """
    effective_timeout = timeout or IMF_API_TIMEOUT_SEC
    url = (
        f"https://api.imf.org/external/sdmx/2.1/data/{IMF_FLOW_REF}/{IMF_KEY}"
        f"?startPeriod={start_date}&endPeriod={end_date}"
        f"&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
    )
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"}, method="GET")

    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=effective_timeout) as resp:
                if resp.getcode() == 200:
                    return resp.read().decode("utf-8")
                if logger:
                    logger.warning(f"IMF API returned HTTP {resp.getcode()}")
        except Exception as exc:
            if attempt < 2:
                wait = 2 ** attempt
                if logger:
                    logger.warning(f"IMF API attempt {attempt+1} failed: {exc}. Retrying in {wait}s...")
                time.sleep(wait)
            else:
                if logger:
                    logger.error(f"IMF API request failed after 3 attempts for {start_date}->{end_date}: {exc}")
    return None


# ---------------------------------------------------------------------------
# XML -> DataFrame
# ---------------------------------------------------------------------------

def process_xml_to_dataframe(xml_data: str, logger=None) -> pd.DataFrame:
    """
    Parses IMF SDMX XML into a clean DataFrame.
    Handles multi-month responses (date ranges).
    """
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as exc:
        if logger:
            logger.error(f"XML parse error: {exc}")
        return pd.DataFrame()

    fetch_ts = datetime.now().isoformat()

    # First pass - collect all series data
    series_data = []
    for series in root.findall(".//Series"):
        country_code = series.get("COUNTRY")
        indicator    = series.get("INDICATOR")
        obs_list     = [
            (obs.get("TIME_PERIOD"), obs.get("OBS_VALUE"))
            for obs in series.findall("Obs")
            if obs.get("OBS_VALUE") is not None
        ]
        if country_code and obs_list:
            series_data.append((country_code, indicator, obs_list))

    # Resolve all currency codes in one parallel batch
    all_countries = list({s[0] for s in series_data})
    currency_map  = resolve_currency_codes(all_countries, logger=logger)

    # Second pass - build rows
    rows = []
    for country_code, indicator, obs_list in series_data:
        base_currency = "USD" if indicator == "USD_XDC" else indicator.split("_")[-1]
        official_currency = currency_map.get(country_code)

        for time_period, value in obs_list:
            year_month = time_period.replace("-M", "")
            rows.append({
                "Country":       country_code,
                "Currency":      official_currency,
                "Date":          year_month,
                "Exchange_Rate": float(value),
                "Base_Currency": base_currency,
                "Timestamp":     fetch_ts,
            })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    df["Date"] = pd.to_datetime(df["Date"], format="%Y%m").dt.strftime("%Y%m")
    return df.sort_values(["Country", "Date"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def fetch_rates_for_month(year_month_api: str, logger=None,
                          force: bool = False) -> str:
    """
    Fetches exchange rates for a single month (format: 'YYYY-MM').
    Saves to data/exchange_rates_YYYY_MM.csv.
    Idempotent unless force=True.
    """
    ym_file = year_month_api.replace("-", "_")
    filename = f"exchange_rates_{ym_file}.csv"
    full_path = DATA_DIR / filename

    if full_path.exists() and not force:
        if logger:
            logger.info(f"File already exists (skipping fetch): {full_path}")
        return str(full_path)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if logger:
        logger.info(f"Fetching IMF exchange rates for {year_month_api}...")

    xml_data = get_currency_data_from_imf(year_month_api, year_month_api, logger=logger)
    if not xml_data:
        raise RuntimeError(f"IMF API returned no data for {year_month_api}")

    df = process_xml_to_dataframe(xml_data, logger=logger)
    if df.empty:
        raise RuntimeError(f"Parsed DataFrame is empty for {year_month_api}")

    df.to_csv(full_path, index=False, encoding="utf-8-sig")
    if logger:
        logger.info(f"Saved {len(df)} rows to {full_path}")

    return str(full_path)


def fetch_rates_for_range(start_api: str, end_api: str,
                          logger=None, force: bool = False,
                          timeout: int = 120) -> list[str]:
    """
    Fetches exchange rates for a date range via a single IMF API call.
    Splits the multi-month response into individual monthly CSV files.

    This is MUCH faster than calling month-by-month for large ranges.

    Args:
        start_api: 'YYYY-MM' start period
        end_api:   'YYYY-MM' end period
        force:     Overwrite existing files
        timeout:   API timeout in seconds (larger ranges need more time)

    Returns:
        List of paths to saved CSV files.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if logger:
        logger.info(f"Fetching IMF exchange rates for range {start_api} -> {end_api}...")

    xml_data = get_currency_data_from_imf(start_api, end_api,
                                           logger=logger, timeout=timeout)
    if not xml_data:
        raise RuntimeError(f"IMF API returned no data for {start_api}->{end_api}")

    df = process_xml_to_dataframe(xml_data, logger=logger)
    if df.empty:
        raise RuntimeError(f"Parsed DataFrame is empty for {start_api}->{end_api}")

    saved_files = []
    for date_key, month_df in df.groupby("Date"):
        ym_file = f"{date_key[:4]}_{date_key[4:]}"
        filepath = DATA_DIR / f"exchange_rates_{ym_file}.csv"

        if filepath.exists() and not force:
            if logger:
                logger.info(f"  Skip {ym_file}: already exists")
            saved_files.append(str(filepath))
            continue

        month_df.to_csv(filepath, index=False, encoding="utf-8-sig")
        saved_files.append(str(filepath))
        if logger:
            logger.info(f"  Saved {ym_file}: {len(month_df)} rows")

    return saved_files


def fetch_rates_chunked(start_year: int, start_month: int,
                        end_year: int = None, end_month: int = None,
                        chunk_size_months: int = 12,
                        logger=None, force: bool = False,
                        delay_between_chunks: float = 1.0) -> dict:
    """
    Fetches exchange rates in yearly (or custom) chunks from start to end.

    Instead of 300+ individual API calls (one per month from 2000-2025),
    this makes ~25 calls (one per year), each returning 12 months of data.

    Args:
        start_year, start_month: Start of range
        end_year, end_month:     End of range (default: last month)
        chunk_size_months:       Months per API call (default: 12 = yearly)
        force:                   Overwrite existing CSVs
        delay_between_chunks:    Seconds to wait between API calls

    Returns:
        dict with keys: saved_files, skipped, failed_chunks, total_months
    """
    months = build_month_list(start_year, start_month, end_year, end_month)
    if not months:
        return {"saved_files": [], "skipped": 0, "failed_chunks": [], "total_months": 0}

    # Build chunks
    chunks = []
    for i in range(0, len(months), chunk_size_months):
        chunk = months[i:i + chunk_size_months]
        chunk_start = f"{chunk[0][0]}-{chunk[0][1]:02d}"
        chunk_end = f"{chunk[-1][0]}-{chunk[-1][1]:02d}"
        chunks.append((chunk_start, chunk_end))

    if logger:
        logger.info(f"Fetching {len(months)} months in {len(chunks)} chunks "
                     f"({chunk_size_months} months each)...")

    all_saved = []
    failed_chunks = []
    skipped = 0

    for idx, (chunk_start, chunk_end) in enumerate(chunks, 1):
        if logger:
            logger.info(f"Chunk {idx}/{len(chunks)}: {chunk_start} -> {chunk_end}")

        try:
            saved = fetch_rates_for_range(
                chunk_start, chunk_end,
                logger=logger, force=force, timeout=120
            )
            all_saved.extend(saved)
        except Exception as exc:
            if logger:
                logger.error(f"  Chunk {chunk_start}->{chunk_end} failed: {exc}")
            failed_chunks.append({"start": chunk_start, "end": chunk_end, "error": str(exc)})

        if idx < len(chunks):
            time.sleep(delay_between_chunks)

    return {
        "saved_files": all_saved,
        "skipped": skipped,
        "failed_chunks": failed_chunks,
        "total_months": len(months),
    }


def fetch_last_month_rates(logger=None) -> str:
    """Convenience wrapper - fetches last month's rates."""
    return fetch_rates_for_month(last_month_api_str(), logger=logger)
