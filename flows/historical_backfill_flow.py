"""
historical_backfill_flow.py
----------------------------
One-shot Prefect flow to backfill IMF exchange rate data from 2000 to
last month.

Enhanced features:
  - CHUNKED fetching: yearly API calls instead of 300+ monthly calls
    (2000-01 to 2000-12 in one call, etc.)
  - Per-month validation against live IMF values after fetch
  - Cross-validation of entire historical dataset
  - Resumable: skips months that already have complete CSVs
  - Force mode: re-fetches everything from scratch
  - Produces combined exchange_rates_ALL.csv

Run modes:
  1. Standard backfill (skip existing):
       python flows/historical_backfill_flow.py

  2. Force re-fetch everything:
       python flows/historical_backfill_flow.py --force

  3. Backfill + full cross-validation:
       python flows/historical_backfill_flow.py --validate-all

  4. Backfill specific range:
       python flows/historical_backfill_flow.py --start-year 2020 --start-month 1

  5. Cross-validate only (no fetch):
       python flows/historical_backfill_flow.py --cross-validate-only --sample 24
"""

import json
import time
import argparse
import pandas as pd
from datetime import datetime
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.artifacts import create_markdown_artifact

from utils.exchange_rate_fetcher import (
    fetch_rates_chunked,
    fetch_rates_for_range,
    build_month_list,
)
from utils.imf_data_validator import (
    cross_validate_historical,
    cross_validate_historical_task,
    save_report,
    print_cross_validation_summary,
)
from utils.config import (
    DATA_DIR, VALIDATION_DIR, ensure_dirs,
    IMF_START_YEAR, EXPECTED_MIN_COUNTRY_COUNT,
)

COUNTRY_TRACKER_FILE = DATA_DIR / "country_presence.json"


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


def _update_tracker_from_csvs(tracker: dict, logger=None) -> dict:
    """Scans all CSVs in data/ and updates the country tracker."""
    all_csvs = sorted(DATA_DIR.glob("exchange_rates_[0-9][0-9][0-9][0-9]_[0-9][0-9].csv"))
    updated = 0

    for csv_path in all_csvs:
        parts = csv_path.stem.split("_")
        try:
            month_key = f"{parts[-2]}_{parts[-1]}"
        except (ValueError, IndexError):
            continue

        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        countries = sorted(df["Country"].dropna().unique().tolist())

        existing = set(tracker.get(month_key, {}).get("countries", []))
        new_countries = set(countries)

        if new_countries != existing:
            tracker[month_key] = {"countries": countries, "count": len(countries)}
            updated += 1

    if logger and updated:
        logger.info(f"Updated tracker for {updated} months")

    return tracker


# ---------------------------------------------------------------------------
# Backfill task (chunked)
# ---------------------------------------------------------------------------

@task(name="chunked_backfill", retries=1, retry_delay_seconds=30)
def chunked_backfill(start_year: int, start_month: int,
                     end_year: int = None, end_month: int = None,
                     force: bool = False, chunk_size: int = 12,
                     logger=None) -> dict:
    """
    Fetches historical data in yearly chunks.
    Returns result dict with stats.
    """
    if logger is None:
        logger = get_run_logger()

    result = fetch_rates_chunked(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
        chunk_size_months=chunk_size,
        logger=logger,
        force=force,
        delay_between_chunks=1.0,
    )

    logger.info(
        f"Chunked backfill complete: {len(result['saved_files'])} files, "
        f"{len(result['failed_chunks'])} failed chunks"
    )
    return result


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="historical_backfill_flow")
def historical_backfill_flow(
    start_year: int = IMF_START_YEAR,
    start_month: int = 1,
    end_year: int | None = None,
    end_month: int | None = None,
    force: bool = False,
    validate_all: bool = False,
    cross_validate_only: bool = False,
    validation_sample: int | None = None,
    chunk_size: int = 12,
):
    """
    Backfills IMF exchange rate data from start_year/start_month to present.

    Modes:
      - Standard:           Fetch missing months, build combined CSV
      - force=True:         Re-fetch everything from scratch
      - validate_all=True:  After fetching, cross-validate against IMF
      - cross_validate_only: Skip fetch, just run cross-validation

    Args:
        start_year:           First year to fetch (default: 2000)
        start_month:          First month to fetch (default: 1)
        end_year/end_month:   End of range (default: last month)
        force:                Overwrite existing CSVs
        validate_all:         Run cross-validation after backfill
        cross_validate_only:  Skip backfill, only cross-validate
        validation_sample:    Number of random months to validate (None=all)
        chunk_size:           Months per API call (default: 12)
    """
    logger = get_run_logger()
    ensure_dirs()

    months = build_month_list(start_year, start_month, end_year, end_month)
    total_months = len(months)

    logger.info(f"Historical backfill: {start_year}-{start_month:02d} -> present "
                f"({total_months} months)")

    # ------------------------------------------------------------------
    # Mode: Cross-validate only (no fetching)
    # ------------------------------------------------------------------
    if cross_validate_only:
        logger.info("Cross-validate only mode - skipping fetch")
        start_api = f"{start_year}-{start_month:02d}" if start_year else None
        end_api = f"{end_year}-{end_month:02d}" if end_year and end_month else None

        cv_report = cross_validate_historical_task(
            start_api=start_api, end_api=end_api,
            sample_months=validation_sample,
        )
        _create_cv_artifact(cv_report, logger)
        return {"mode": "cross_validate_only", "report": cv_report}

    # ------------------------------------------------------------------
    # Step 1: Chunked fetch
    # ------------------------------------------------------------------
    logger.info(f"Fetching data in {chunk_size}-month chunks (force={force})...")

    backfill_result = chunked_backfill(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
        force=force,
        chunk_size=chunk_size,
        logger=logger,
    )

    n_saved = len(backfill_result["saved_files"])
    n_failed = len(backfill_result["failed_chunks"])

    # ------------------------------------------------------------------
    # Step 2: Update country tracker
    # ------------------------------------------------------------------
    tracker = _load_tracker()
    tracker = _update_tracker_from_csvs(tracker, logger=logger)
    _save_tracker(tracker)
    logger.info(f"Country presence tracker updated ({len(tracker)} months)")

    # ------------------------------------------------------------------
    # Step 3: Build combined CSV
    # ------------------------------------------------------------------
    logger.info("Building combined dataset...")
    all_files = sorted(DATA_DIR.glob("exchange_rates_[0-9][0-9][0-9][0-9]_[0-9][0-9].csv"))

    total_rows = total_ctry = 0
    date_min = date_max = "N/A"

    if all_files:
        combined = pd.concat(
            [pd.read_csv(f, encoding="utf-8-sig") for f in all_files],
            ignore_index=True,
        ).sort_values(["Country", "Date"])

        combined_path = DATA_DIR / "exchange_rates_ALL.csv"
        combined.to_csv(combined_path, index=False, encoding="utf-8-sig")
        total_rows = len(combined)
        total_ctry = combined["Country"].nunique()
        date_min   = combined["Date"].min()
        date_max   = combined["Date"].max()
        logger.info(f"Combined file: {combined_path}  ({total_rows:,} rows)")

    # ------------------------------------------------------------------
    # Step 4: Optional cross-validation
    # ------------------------------------------------------------------
    cv_report = None
    if validate_all:
        logger.info("Running cross-validation against live IMF data...")
        start_api = f"{start_year}-{start_month:02d}"
        end_api = None  # will auto-detect from available files

        cv_report = cross_validate_historical_task(
            start_api=start_api, end_api=end_api,
            sample_months=validation_sample,
        )
        _create_cv_artifact(cv_report, logger)

    # ------------------------------------------------------------------
    # Step 5: Summary artifact
    # ------------------------------------------------------------------
    failed_chunks_str = ""
    if backfill_result["failed_chunks"]:
        failed_chunks_str = "\n".join(
            f"- {c['start']} -> {c['end']}: {c['error']}"
            for c in backfill_result["failed_chunks"]
        )
        failed_section = f"## Failed Chunks\n{failed_chunks_str}"
    else:
        failed_section = "## No failures"

    cv_section = ""
    if cv_report:
        s = cv_report.get("summary", {})
        cv_section = f"""## Cross-Validation Results
| | |
|---|---|
| Months validated | {s.get('months_validated', 'N/A')} |
| Perfect months | {s.get('months_perfect', 'N/A')} |
| Total rates checked | {s.get('total_rates_checked', 'N/A'):,} |
| Overall accuracy | {s.get('overall_accuracy_pct', 'N/A')}% |
"""

    create_markdown_artifact(
        key="backfill-summary",
        markdown=f"""# Historical Backfill Summary

| | |
|---|---|
| Period | {start_year}-{start_month:02d} -> present |
| Total months | {total_months} |
| Files saved/updated | {n_saved} |
| Failed chunks | {n_failed} |
| Force mode | {force} |

## Combined Dataset
| | |
|---|---|
| Total rows | {total_rows:,} |
| Countries | {total_ctry} |
| Date range | {date_min} -> {date_max} |
| CSV count | {len(all_files)} |

{failed_section}

{cv_section}
""",
        description="IMF Historical Backfill Results",
    )

    logger.info(
        f"Backfill complete - saved: {n_saved}, failed chunks: {n_failed}"
    )

    return {
        "saved_files": n_saved,
        "failed_chunks": n_failed,
        "total_rows": total_rows,
        "total_countries": total_ctry,
        "cross_validation": cv_report,
    }


def _create_cv_artifact(cv_report: dict, logger):
    """Creates a Prefect artifact for cross-validation results."""
    try:
        s = cv_report.get("summary", {})
        status = cv_report.get("overall_status", "UNKNOWN")

        imperfect_lines = ""
        for m in cv_report.get("imperfect_months", [])[:20]:
            imperfect_lines += f"| {m['month']} | {m['mismatches']} | {m['accuracy']}% |\n"

        create_markdown_artifact(
            key="cross-validation-results",
            markdown=f"""# Cross-Validation Report  |  {status}

| Metric | Value |
|---|---|
| Months checked | {s.get('months_checked', 'N/A')} |
| Months validated | {s.get('months_validated', 'N/A')} |
| Perfect months | {s.get('months_perfect', 'N/A')} |
| Months with mismatches | {s.get('months_with_mismatches', 'N/A')} |
| Fetch failures | {s.get('fetch_failures', 'N/A')} |
| Total rates checked | {s.get('total_rates_checked', 0):,} |
| Total matches | {s.get('total_matches', 0):,} |
| Total mismatches | {s.get('total_mismatches', 0):,} |
| **Overall accuracy** | **{s.get('overall_accuracy_pct', 'N/A')}%** |

{"## Months with Issues" + chr(10) + "| Month | Mismatches | Accuracy |" + chr(10) + "|---|---|---|" + chr(10) + imperfect_lines if imperfect_lines else "## All validated months are perfect!"}
""",
            description="IMF Historical Cross-Validation Results",
        )
    except Exception as exc:
        logger.warning(f"Artifact creation failed (non-fatal): {exc}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="IMF Historical Backfill")
    ap.add_argument("--start-year", type=int, default=IMF_START_YEAR)
    ap.add_argument("--start-month", type=int, default=1)
    ap.add_argument("--end-year", type=int, default=None)
    ap.add_argument("--end-month", type=int, default=None)
    ap.add_argument("--force", action="store_true",
                    help="Re-fetch all months even if CSVs exist")
    ap.add_argument("--validate-all", action="store_true",
                    help="Cross-validate all fetched data against IMF after backfill")
    ap.add_argument("--cross-validate-only", action="store_true",
                    help="Skip fetch, just run cross-validation")
    ap.add_argument("--sample", type=int, default=None,
                    help="Validate a random sample of N months")
    ap.add_argument("--chunk-size", type=int, default=12,
                    help="Months per API call (default: 12)")
    args = ap.parse_args()

    historical_backfill_flow(
        start_year=args.start_year,
        start_month=args.start_month,
        end_year=args.end_year,
        end_month=args.end_month,
        force=args.force,
        validate_all=args.validate_all,
        cross_validate_only=args.cross_validate_only,
        validation_sample=args.sample,
        chunk_size=args.chunk_size,
    )
