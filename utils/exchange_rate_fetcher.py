"""
exchange_rate_fetcher.py
------------------------
Fetches exchange rate data from the IMF SDMX 2.1 API and resolves
currency codes via the REST Countries API.

Key improvements over original:
  - Currency cache persisted to disk between runs (currency_cache.json)
  - REST Countries lookups run in parallel (ThreadPoolExecutor)
  - All output goes through Prefect logger (not print)
  - Single shared helper for "last month" date logic
  - Retry on REST Countries API failures (up to 3 attempts)
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


# Module-level cache — loaded once, flushed to disk after each fetch run
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


# ---------------------------------------------------------------------------
# REST Countries API — currency code lookup
# ---------------------------------------------------------------------------

def _fetch_currency_from_api(country_code: str) -> str | None:
    """
    Single REST Countries API call with up to 3 retries.
    Returns the ISO 4217 currency code or None.
    """
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
                time.sleep(1.5 ** attempt)   # 1s, 1.5s
    return None


def resolve_currency_codes(country_codes: list, logger=None) -> dict:
    """
    Resolves currency codes for a list of country codes.
    Uses the persistent cache; only hits the API for unknown codes.
    Parallel requests via ThreadPoolExecutor.

    Returns a dict: {country_code: currency_code_or_None}
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
                _currency_cache[code] = currency   # update in-memory cache

        # Persist updated cache to disk
        _save_cache(_currency_cache)

        if logger:
            failed = [c for c in to_fetch if result.get(c) is None]
            if failed:
                logger.warning(f"Could not resolve currency for: {failed}")

    return result


# ---------------------------------------------------------------------------
# IMF SDMX API
# ---------------------------------------------------------------------------

def get_currency_data_from_imf(start_date: str, end_date: str, logger=None) -> str | None:
    """
    Fetches XML exchange rate data from IMF SDMX 2.1 API.

    Args:
        start_date: 'YYYY-MM'
        end_date:   'YYYY-MM'

    Returns:
        Raw XML string, or None on failure.
    """
    url = (
        f"https://api.imf.org/external/sdmx/2.1/data/{IMF_FLOW_REF}/{IMF_KEY}"
        f"?startPeriod={start_date}&endPeriod={end_date}"
        f"&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
    )
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"}, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=IMF_API_TIMEOUT_SEC) as resp:
            if resp.getcode() == 200:
                return resp.read().decode("utf-8")
            if logger:
                logger.warning(f"IMF API returned HTTP {resp.getcode()} for {start_date}")
    except Exception as exc:
        if logger:
            logger.error(f"IMF API request failed for {start_date}: {exc}")
    return None


# ---------------------------------------------------------------------------
# XML → DataFrame
# ---------------------------------------------------------------------------

def process_xml_to_dataframe(xml_data: str, logger=None) -> pd.DataFrame:
    """
    Parses IMF SDMX XML into a clean DataFrame.
    Currency codes are resolved in parallel via the REST Countries API.
    """
    try:
        root = ET.fromstring(xml_data)
    except ET.ParseError as exc:
        if logger:
            logger.error(f"XML parse error: {exc}")
        return pd.DataFrame()

    fetch_ts = datetime.now().isoformat()
    rows = []

    # First pass — collect all (country, indicator, observations)
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

    # Second pass — build rows
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

def fetch_rates_for_month(year_month_api: str, logger=None) -> str:
    """
    Fetches exchange rates for a single month (format: 'YYYY-MM').
    Saves to data/exchange_rates_YYYY_MM.csv.
    Idempotent — skips if file already exists.

    Returns the path to the CSV file.
    """
    ym_file = year_month_api.replace("-", "_")
    filename = f"exchange_rates_{ym_file}.csv"
    full_path = DATA_DIR / filename

    if full_path.exists():
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


def fetch_last_month_rates(logger=None) -> str:
    """Convenience wrapper — fetches last month's rates."""
    return fetch_rates_for_month(last_month_api_str(), logger=logger)
