"""
imf_data_validator.py
---------------------
Validates a scraped exchange rate CSV against the live IMF API.

Checks:
  1. Country coverage  — countries in IMF but missing from CSV
  2. Null currency codes — REST Countries API lookup failures
  3. Duplicate rows     — same Country + Date more than once
  4. Rate anomalies     — zero, negative, implausibly large/small
  5. Date correctness   — rows stamped with the wrong month
  6. Month-on-month     — rates that moved >30% vs prior file (flag only)

Usage as Prefect task (in a flow):
    from utils.imf_data_validator import validate_exchange_rate_data
    report = validate_exchange_rate_data(csv_path, fail_on_issues=True)

Usage from command line:
    python utils/imf_data_validator.py --csv data/exchange_rates_2025_01.csv
"""

import os
import json
import argparse
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from prefect import task, get_run_logger

from utils.config import (
    IMF_FLOW_REF, IMF_KEY, IMF_API_TIMEOUT_SEC,
    MAX_REASONABLE_RATE, MIN_REASONABLE_RATE,
    MAX_MONTH_ON_MONTH_CHANGE, EXPECTED_MIN_COUNTRY_COUNT,
    VALIDATION_DIR, IMF_AGGREGATE_CODES,
)


# ---------------------------------------------------------------------------
# Fetch live IMF country list for a given month
# ---------------------------------------------------------------------------

def fetch_imf_country_list(year_month_api: str) -> set:
    """Returns the set of country codes IMF has for 'YYYY-MM'. Empty set on failure."""
    url = (
        f"https://api.imf.org/external/sdmx/2.1/data/{IMF_FLOW_REF}/{IMF_KEY}"
        f"?startPeriod={year_month_api}&endPeriod={year_month_api}"
        f"&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
    )
    try:
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=IMF_API_TIMEOUT_SEC) as resp:
            if resp.getcode() != 200:
                return set()
            root = ET.fromstring(resp.read().decode("utf-8"))
            return {s.get("COUNTRY") for s in root.findall(".//Series") if s.get("COUNTRY")}
    except Exception:
        return set()


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------

def _check_coverage(df: pd.DataFrame, imf_countries: set) -> dict:
    local  = set(df["Country"].dropna().unique())
    missing = imf_countries - local
    extra   = local - imf_countries
    overlap = local & imf_countries
    return {
        "imf_count":               len(imf_countries),
        "csv_count":               len(local),
        "coverage_pct":            round(len(overlap) / max(len(imf_countries), 1) * 100, 2),
        "missing_from_csv":        sorted(missing),
        "extra_in_csv_not_in_imf": sorted(extra),
    }


def _check_null_currencies(df: pd.DataFrame) -> dict:
    """Flags rows with no currency code, excluding known IMF aggregate codes
    which are not real countries and will never have a currency."""
    null_mask = df["Currency"].isna() | (df["Currency"].astype(str).str.strip().isin(["None", ""]))
    bad = df[null_mask]
    # Exclude known IMF aggregate/regional codes — these are expected to have no currency
    bad = bad[~bad["Country"].isin(IMF_AGGREGATE_CODES)]
    return {"count": len(bad), "countries": sorted(bad["Country"].unique().tolist())}


def _check_duplicates(df: pd.DataFrame) -> dict:
    dupes = df[df.duplicated(subset=["Country", "Date"], keep=False)]
    return {
        "count":   len(dupes),
        "records": dupes[["Country", "Date", "Exchange_Rate"]].to_dict("records"),
    }


def _check_anomalous_rates(df: pd.DataFrame) -> dict:
    null_r    = df[df["Exchange_Rate"].isna()]
    zero_neg  = df[df["Exchange_Rate"] <= 0]
    too_large = df[df["Exchange_Rate"] > MAX_REASONABLE_RATE]
    too_small = df[(df["Exchange_Rate"] > 0) & (df["Exchange_Rate"] < MIN_REASONABLE_RATE)]
    return {
        "null_count":             len(null_r),
        "null_countries":         sorted(null_r["Country"].unique().tolist()),
        "zero_or_neg_count":      len(zero_neg),
        "zero_or_neg":            zero_neg[["Country", "Exchange_Rate"]].to_dict("records"),
        "implausibly_large_count": len(too_large),
        "implausibly_large":       too_large[["Country", "Exchange_Rate"]].to_dict("records"),
        "implausibly_small_count": len(too_small),
        "implausibly_small":       too_small[["Country", "Exchange_Rate"]].to_dict("records"),
    }


def _check_date_coverage(df: pd.DataFrame, expected_ym: str) -> dict:
    """expected_ym format: 'YYYYMM'"""
    wrong = df[df["Date"].astype(str) != expected_ym]
    return {
        "expected":        expected_ym,
        "wrong_count":     len(wrong),
        "wrong_sample":    wrong[["Country", "Date"]].head(10).to_dict("records"),
    }


def _check_mom_changes(csv_path: str) -> dict:
    """Compares current month rates to previous month's file."""
    p = Path(csv_path)
    parts = p.stem.split("_")
    try:
        year, month = int(parts[-2]), int(parts[-1])
    except (ValueError, IndexError):
        return {"skipped": "Filename does not match exchange_rates_YYYY_MM.csv pattern"}

    prev_date = datetime(year, month, 1) - timedelta(days=1)
    prev_path = p.parent / f"exchange_rates_{prev_date.strftime('%Y_%m')}.csv"

    if not prev_path.exists():
        return {"skipped": f"No prior file at {prev_path}"}

    curr = pd.read_csv(csv_path, encoding="utf-8-sig")[["Country", "Exchange_Rate"]]
    prev = pd.read_csv(prev_path, encoding="utf-8-sig")[["Country", "Exchange_Rate"]]
    merged = curr.merge(prev, on="Country", suffixes=("_curr", "_prev")).dropna()
    merged = merged[merged["Exchange_Rate_prev"] != 0].copy()
    merged["pct_change"] = (
        (merged["Exchange_Rate_curr"] - merged["Exchange_Rate_prev"]).abs()
        / merged["Exchange_Rate_prev"]
    )
    flagged = merged[merged["pct_change"] > MAX_MONTH_ON_MONTH_CHANGE].copy()
    flagged["pct_change"] = (flagged["pct_change"] * 100).round(2)

    return {
        "compared_against":    str(prev_path),
        "large_movers_count":  len(flagged),
        "large_movers": flagged[
            ["Country", "Exchange_Rate_curr", "Exchange_Rate_prev", "pct_change"]
        ].to_dict("records"),
    }


# ---------------------------------------------------------------------------
# Build full report
# ---------------------------------------------------------------------------

def build_report(csv_path: str, imf_countries: set, expected_ym: str, year_month_api: str) -> dict:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    required = {"Country", "Currency", "Date", "Exchange_Rate", "Base_Currency"}
    missing_cols = required - set(df.columns)
    if missing_cols:
        raise ValueError(f"CSV missing columns: {missing_cols}")
    df["Exchange_Rate"] = pd.to_numeric(df["Exchange_Rate"], errors="coerce")

    checks = {
        "country_coverage":       _check_coverage(df, imf_countries),
        "null_currency_codes":    _check_null_currencies(df),
        "duplicate_records":      _check_duplicates(df),
        "anomalous_rates":        _check_anomalous_rates(df),
        "date_coverage":          _check_date_coverage(df, expected_ym),
        "month_on_month_changes": _check_mom_changes(csv_path),
    }

    issues = []
    if checks["country_coverage"]["missing_from_csv"]:
        n = len(checks["country_coverage"]["missing_from_csv"])
        issues.append(f"{n} countries present in IMF but missing from CSV")
    if checks["null_currency_codes"]["count"]:
        issues.append(f"{checks['null_currency_codes']['count']} rows have no currency code")
    if checks["duplicate_records"]["count"]:
        issues.append(f"{checks['duplicate_records']['count']} duplicate Country+Date rows")
    an = checks["anomalous_rates"]
    if an["null_count"]:       issues.append(f"{an['null_count']} null exchange rates")
    if an["zero_or_neg_count"]: issues.append(f"{an['zero_or_neg_count']} zero/negative rates")
    if checks["date_coverage"]["wrong_count"]:
        issues.append(f"{checks['date_coverage']['wrong_count']} rows with wrong date stamp")

    # MoM is informational only — logged as warning, never causes FAIL
    movers = checks["month_on_month_changes"]
    mom_note = None
    if "large_movers_count" in movers and movers["large_movers_count"] > 0:
        mom_note = f"{movers['large_movers_count']} currencies moved >{MAX_MONTH_ON_MONTH_CHANGE*100:.0f}% MoM (review recommended)"

    return {
        "validated_at":    datetime.now().isoformat(),
        "csv_file":        os.path.basename(csv_path),
        "year_month":      year_month_api,
        "total_rows":      len(df),
        "overall_status":  "PASS" if not issues else "FAIL",
        "issues":          issues,
        "mom_note":        mom_note,
        "checks":          checks,
    }


def save_report(report: dict) -> str:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = VALIDATION_DIR / f"validation_{report['year_month'].replace('-','_')}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return str(path)


def print_summary(report: dict):
    icon = "✅" if report["overall_status"] == "PASS" else "❌"
    cov  = report["checks"]["country_coverage"]
    an   = report["checks"]["anomalous_rates"]
    mom  = report["checks"]["month_on_month_changes"]

    print(f"\n{'='*60}")
    print(f"  IMF Validation  {icon} {report['overall_status']}  |  {report['csv_file']}")
    print(f"{'='*60}")
    print(f"  Rows: {report['total_rows']}  |  Coverage: {cov['csv_count']}/{cov['imf_count']} ({cov['coverage_pct']}%)")
    if cov["missing_from_csv"]:
        print(f"  Missing countries : {', '.join(cov['missing_from_csv'])}")
    null_c = report["checks"]["null_currency_codes"]
    if null_c["count"]:
        print(f"  No currency code  : {', '.join(null_c['countries'])}")
    print(f"  Null rates: {an['null_count']}  |  Zero/neg: {an['zero_or_neg_count']}  |  Dupes: {report['checks']['duplicate_records']['count']}")
    if "large_movers_count" in mom:
        print(f"  MoM large movers  : {mom['large_movers_count']}")
    if report["issues"]:
        print(f"\n  Issues:")
        for i in report["issues"]:
            print(f"    • {i}")
    if report.get("mom_note"):
        print(f"  ⚠  {report['mom_note']}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Prefect task wrapper
# ---------------------------------------------------------------------------

@task(name="validate_exchange_rate_data", retries=1, retry_delay_seconds=5)
def validate_exchange_rate_data(csv_path: str, fail_on_issues: bool = True) -> dict:
    """
    Prefect task — validates scraped CSV against live IMF data.
    Set fail_on_issues=True to block the pipeline on bad data.
    """
    logger = get_run_logger()
    logger.info(f"Validating: {csv_path}")

    stem  = Path(csv_path).stem
    parts = stem.split("_")
    try:
        year_month_api = f"{parts[-2]}-{parts[-1]}"
        expected_ym    = parts[-2] + parts[-1]
    except IndexError:
        lm = (datetime.today().replace(day=1) - timedelta(days=1))
        year_month_api = lm.strftime("%Y-%m")
        expected_ym    = lm.strftime("%Y%m")

    logger.info(f"Fetching IMF country list for {year_month_api}...")
    imf_countries = fetch_imf_country_list(year_month_api)
    if not imf_countries:
        logger.warning("IMF country list unavailable — coverage check skipped")

    report      = build_report(csv_path, imf_countries, expected_ym, year_month_api)
    report_path = save_report(report)
    logger.info(f"Report saved: {report_path}")

    for issue in report["issues"]:
        logger.error(f"Validation issue: {issue}")
    if report.get("mom_note"):
        logger.warning(report["mom_note"])

    if report["overall_status"] == "PASS":
        logger.info(f"✅ Validation PASSED — {report['total_rows']} rows, "
                    f"{report['checks']['country_coverage']['coverage_pct']}% coverage")
    else:
        msg = f"❌ Validation FAILED — {len(report['issues'])} issue(s)"
        logger.error(msg)
        if fail_on_issues:
            raise ValueError(msg)

    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True)
    ap.add_argument("--fail-on-issues", action="store_true", default=False)
    args = ap.parse_args()

    stem  = Path(args.csv).stem
    parts = stem.split("_")
    try:
        year_month_api = f"{parts[-2]}-{parts[-1]}"
        expected_ym    = parts[-2] + parts[-1]
    except IndexError:
        lm = (datetime.today().replace(day=1) - timedelta(days=1))
        year_month_api = lm.strftime("%Y-%m")
        expected_ym    = lm.strftime("%Y%m")

    print(f"Fetching IMF country list for {year_month_api}...")
    imf_countries = fetch_imf_country_list(year_month_api)

    report      = build_report(args.csv, imf_countries, expected_ym, year_month_api)
    report_path = save_report(report)
    print_summary(report)
    print(f"Full report: {report_path}")

    if args.fail_on_issues and report["overall_status"] == "FAIL":
        raise SystemExit(1)
