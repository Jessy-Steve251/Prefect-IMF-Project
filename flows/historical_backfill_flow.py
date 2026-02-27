"""
historical_backfill_flow.py
----------------------------
One-shot Prefect flow to backfill IMF exchange rate data from 2000 to
last month, month by month.

Features:
  - Skips months that already have a complete CSV (idempotent / resumable)
  - Detects incomplete months using a country-presence tracker
    (data/country_presence.json)
  - Rates API calls at 0.5s apart to be polite to the IMF endpoint
  - Produces a combined exchange_rates_ALL.csv at the end
  - Validates each freshly fetched month before saving

Run once manually:
    prefect deployment run historical-backfill/historical-backfill
  or locally:
    python flows/historical_backfill_flow.py
"""

import json
import time
import pandas as pd
from datetime import datetime
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from utils.exchange_rate_fetcher import (
    get_currency_data_from_imf,
    process_xml_to_dataframe,
)
from utils.config import (
    DATA_DIR, ensure_dirs,
    IMF_START_YEAR, EXPECTED_MIN_COUNTRY_COUNT,
)

COUNTRY_TRACKER_FILE = DATA_DIR / "country_presence.json"
COUNTRIES_OF_INTEREST = ["GHA", "NGA", "ZAF", "KEN", "EGY", "ETH"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_tracker() -> dict:
    if COUNTRY_TRACKER_FILE.exists():
        try:
            with open(COUNTRY_TRACKER_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_tracker(tracker: dict):
    with open(COUNTRY_TRACKER_FILE, "w", encoding="utf-8") as f:
        json.dump(tracker, f, indent=2, ensure_ascii=False)


def _build_month_list(start_year: int, start_month: int) -> list[tuple[int, int]]:
    """Returns list of (year, month) tuples from start up to last month."""
    today = datetime.now()
    end_year  = today.year
    end_month = today.month - 1
    if end_month == 0:
        end_year  -= 1
        end_month  = 12

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
# Per-month fetch task
# ---------------------------------------------------------------------------

@task(name="fetch_month", retries=2, retry_delay_seconds=10)
def fetch_month(year: int, month: int, tracker: dict, logger) -> dict:
    """
    Fetches and saves one month's exchange rates.
    Returns a result dict: {month_key, status, rows, countries, skipped}.
    """
    date_str  = f"{year}-{month:02d}"
    month_key = f"{year}_{month:02d}"
    filepath  = DATA_DIR / f"exchange_rates_{month_key}.csv"

    # ------------------------------------------------------------------
    # Decide whether to fetch
    # ------------------------------------------------------------------
    if filepath.exists():
        existing_df       = pd.read_csv(filepath, encoding="utf-8-sig")
        existing_countries = set(existing_df["Country"].unique())

        if month_key in tracker:
            expected = set(tracker[month_key].get("countries", []))
            missing  = expected - existing_countries
            if not missing:
                logger.info(f"  ⏭  {date_str}: complete ({len(existing_countries)} countries)")
                return {
                    "month_key": month_key, "status": "skipped",
                    "rows": len(existing_df), "countries": list(existing_countries),
                }
            else:
                logger.info(f"  ⚠  {date_str}: {len(missing)} countries missing — re-fetching")
        else:
            # No tracker entry yet — accept existing file, just register it
            logger.info(f"  ⏭  {date_str}: exists (first time tracking)")
            return {
                "month_key": month_key, "status": "skipped",
                "rows": len(existing_df), "countries": list(existing_countries),
            }

    # ------------------------------------------------------------------
    # Fetch from IMF
    # ------------------------------------------------------------------
    logger.info(f"  📥 {date_str}: fetching...")
    xml_data = get_currency_data_from_imf(date_str, date_str, logger=logger)

    if not xml_data:
        logger.warning(f"  ❌ {date_str}: IMF returned no data")
        return {"month_key": month_key, "status": "failed", "rows": 0, "countries": []}

    df = process_xml_to_dataframe(xml_data, logger=logger)

    if df.empty:
        logger.warning(f"  ❌ {date_str}: empty DataFrame after parsing")
        return {"month_key": month_key, "status": "failed", "rows": 0, "countries": []}

    new_countries = set(df["Country"].unique())

    if len(new_countries) < EXPECTED_MIN_COUNTRY_COUNT:
        logger.warning(
            f"  ⚠  {date_str}: only {len(new_countries)} countries (suspicious) — saved anyway"
        )

    # Back up old file if it existed
    if filepath.exists():
        filepath.rename(filepath.with_suffix(".csv.bak"))

    df["Date"] = df["Date"].astype(str)
    df.to_csv(filepath, index=False, encoding="utf-8-sig")

    # Log countries of interest
    for cc in COUNTRIES_OF_INTEREST:
        if cc in new_countries:
            rate = df.loc[df["Country"] == cc, "Exchange_Rate"].iloc[0]
            logger.info(f"     {cc}: {rate:.4f}")

    logger.info(f"  ✅ {date_str}: {len(df)} rows, {len(new_countries)} countries")
    time.sleep(0.5)   # be polite to IMF API

    return {
        "month_key": month_key, "status": "fetched",
        "rows": len(df), "countries": list(new_countries),
    }


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="historical_backfill_flow")
def historical_backfill_flow(start_year: int = IMF_START_YEAR, start_month: int = 1):
    """
    Backfills IMF exchange rate data from start_year/start_month to last month.
    Safe to re-run — already-complete months are skipped automatically.

    Args:
        start_year:  First year to fetch (default: 2000 from config).
        start_month: First month to fetch (default: 1).
    """
    logger = get_run_logger()
    ensure_dirs()

    tracker     = _load_tracker()
    month_list  = _build_month_list(start_year, start_month)
    total       = len(month_list)

    logger.info(f"Backfill: {start_year}-{start_month:02d} → present  ({total} months)")

    results   = []
    n_fetched = n_skipped = n_failed = 0

    for year, month in month_list:
        result = fetch_month(year, month, tracker, logger)
        results.append(result)

        # Update tracker
        if result["status"] in ("fetched", "skipped") and result["countries"]:
            mk = result["month_key"]
            existing_c = set(tracker.get(mk, {}).get("countries", []))
            updated_c  = existing_c | set(result["countries"])
            tracker[mk] = {"countries": list(updated_c), "count": len(updated_c)}

        if   result["status"] == "fetched":  n_fetched += 1
        elif result["status"] == "skipped":  n_skipped += 1
        else:                                n_failed  += 1

    # Save updated tracker
    _save_tracker(tracker)
    logger.info(f"Country presence tracker updated ({len(tracker)} months tracked)")

    # ------------------------------------------------------------------
    # Build combined CSV
    # ------------------------------------------------------------------
    logger.info("Building combined dataset...")
    all_files = sorted(DATA_DIR.glob("exchange_rates_[0-9][0-9][0-9][0-9]_[0-9][0-9].csv"))
    if all_files:
        combined = pd.concat(
            [pd.read_csv(f, encoding="utf-8-sig") for f in all_files],
            ignore_index=True,
        ).sort_values(["Country", "Date"])

        combined_path = DATA_DIR / "exchange_rates_ALL.csv"
        combined.to_csv(combined_path, index=False, encoding="utf-8-sig")
        logger.info(f"Combined file: {combined_path}  ({len(combined):,} rows)")

        total_rows    = len(combined)
        total_ctry    = combined["Country"].nunique()
        date_min      = combined["Date"].min()
        date_max      = combined["Date"].max()
    else:
        total_rows = total_ctry = 0
        date_min = date_max = "N/A"

    # ------------------------------------------------------------------
    # Summary artifact
    # ------------------------------------------------------------------
    failed_months = [r["month_key"] for r in results if r["status"] == "failed"]

    create_markdown_artifact(
        key="backfill-summary",
        markdown=f"""# Historical Backfill Summary

| | |
|---|---|
| Period | {start_year}-{start_month:02d} → present |
| Total months | {total} |
| Fetched (new/updated) | {n_fetched} |
| Skipped (already complete) | {n_skipped} |
| Failed | {n_failed} |

## Combined Dataset
| | |
|---|---|
| Total rows | {total_rows:,} |
| Countries | {total_ctry} |
| Date range | {date_min} → {date_max} |

{"## ⚠️ Failed Months" + chr(10) + chr(10).join(f"- {m}" for m in failed_months) if failed_months else "## ✅ No failures"}
""",
        description="IMF Historical Backfill Results",
    )

    logger.info(
        f"Backfill complete — fetched: {n_fetched}, skipped: {n_skipped}, failed: {n_failed}"
    )
    if failed_months:
        logger.warning(f"Failed months (re-run to retry): {failed_months}")

    return {
        "fetched": n_fetched,
        "skipped": n_skipped,
        "failed":  n_failed,
        "failed_months": failed_months,
    }


if __name__ == "__main__":
    historical_backfill_flow()
