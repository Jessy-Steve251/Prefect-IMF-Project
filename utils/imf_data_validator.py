"""
imf_data_validator.py
---------------------
Validates scraped exchange rate CSVs against the live IMF API.

Two modes:
  1. Single-month validation  (validate_exchange_rate_data)
     - Country coverage, null currencies, duplicates, rate anomalies,
       date correctness, month-on-month changes

  2. Historical cross-validation  (cross_validate_historical)
     - Re-fetches data from IMF for a sample (or all) months
     - Compares stored CSV values against live API values
     - Detects: missing countries, rate mismatches, stale data
     - Produces a detailed JSON + summary report

Usage as Prefect task (in a flow):
    from utils.imf_data_validator import validate_exchange_rate_data
    report = validate_exchange_rate_data(csv_path, fail_on_issues=True)

Cross-validation from CLI:
    python utils/imf_data_validator.py --cross-validate --start 2020-01 --end 2024-12
    python utils/imf_data_validator.py --cross-validate --sample 24  # random 24 months

Single month from CLI:
    python utils/imf_data_validator.py --csv data/exchange_rates_2025_01.csv
"""

import os
import json
import random
import argparse
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from utils.config import (
    DATA_DIR,
    IMF_FLOW_REF, IMF_KEY, IMF_API_TIMEOUT_SEC,
    MAX_REASONABLE_RATE, MIN_REASONABLE_RATE,
    MAX_MONTH_ON_MONTH_CHANGE, EXPECTED_MIN_COUNTRY_COUNT,
    VALIDATION_DIR, IMF_AGGREGATE_CODES, IMF_START_YEAR,
)


# ---------------------------------------------------------------------------
# Fetch live IMF data
# ---------------------------------------------------------------------------

def fetch_imf_country_list(year_month_api: str) -> set:
    """Returns the set of country codes IMF has for 'YYYY-MM'."""
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


def fetch_imf_rates_for_month(year_month_api: str, timeout: int = 60) -> dict:
    """
    Fetches actual exchange rate values from IMF for a single month.

    Returns:
        dict: {country_code: exchange_rate} or empty dict on failure.
    """
    url = (
        f"https://api.imf.org/external/sdmx/2.1/data/{IMF_FLOW_REF}/{IMF_KEY}"
        f"?startPeriod={year_month_api}&endPeriod={year_month_api}"
        f"&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
    )
    try:
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.getcode() != 200:
                return {}
            root = ET.fromstring(resp.read().decode("utf-8"))

            rates = {}
            for series in root.findall(".//Series"):
                country = series.get("COUNTRY")
                if not country:
                    continue
                for obs in series.findall("Obs"):
                    val = obs.get("OBS_VALUE")
                    if val is not None:
                        try:
                            rates[country] = float(val)
                        except ValueError:
                            pass
            return rates
    except Exception:
        return {}


def fetch_imf_rates_for_range(start_api: str, end_api: str,
                               timeout: int = 120) -> dict:
    """
    Fetches actual exchange rate values from IMF for a date range.

    Returns:
        dict: {(country_code, 'YYYYMM'): exchange_rate}
    """
    url = (
        f"https://api.imf.org/external/sdmx/2.1/data/{IMF_FLOW_REF}/{IMF_KEY}"
        f"?startPeriod={start_api}&endPeriod={end_api}"
        f"&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
    )
    try:
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.getcode() != 200:
                return {}
            root = ET.fromstring(resp.read().decode("utf-8"))

            rates = {}
            for series in root.findall(".//Series"):
                country = series.get("COUNTRY")
                if not country:
                    continue
                for obs in series.findall("Obs"):
                    period = obs.get("TIME_PERIOD", "").replace("-M", "")
                    val = obs.get("OBS_VALUE")
                    if val is not None:
                        try:
                            rates[(country, period)] = float(val)
                        except ValueError:
                            pass
            return rates
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Individual checks (single-month validation)
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
    null_mask = df["Currency"].isna() | (df["Currency"].astype(str).str.strip().isin(["None", ""]))
    bad = df[null_mask]
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
    wrong = df[df["Date"].astype(str) != expected_ym]
    return {
        "expected":        expected_ym,
        "wrong_count":     len(wrong),
        "wrong_sample":    wrong[["Country", "Date"]].head(10).to_dict("records"),
    }


def _check_mom_changes(csv_path: str) -> dict:
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


def _check_rate_accuracy(df: pd.DataFrame, imf_rates: dict,
                          tolerance: float = 0.001) -> dict:
    """
    Compares CSV exchange rates against live IMF API values.
    Flags mismatches beyond the tolerance threshold.

    Args:
        df:         DataFrame from the CSV
        imf_rates:  dict {country_code: rate} from live IMF API
        tolerance:  Relative tolerance for mismatch (default 0.1%)

    Returns:
        dict with match stats and list of mismatches
    """
    if not imf_rates:
        return {"skipped": "IMF rates unavailable for comparison"}

    matches = 0
    mismatches = []
    checked = 0

    for _, row in df.iterrows():
        country = row["Country"]
        csv_rate = row["Exchange_Rate"]

        if country not in imf_rates:
            continue

        checked += 1
        imf_rate = imf_rates[country]

        if imf_rate == 0 and csv_rate == 0:
            matches += 1
            continue

        if imf_rate == 0:
            mismatches.append({
                "country": country,
                "csv_rate": csv_rate,
                "imf_rate": imf_rate,
                "diff_pct": "N/A (IMF=0)",
            })
            continue

        diff_pct = abs(csv_rate - imf_rate) / abs(imf_rate)

        if diff_pct <= tolerance:
            matches += 1
        else:
            mismatches.append({
                "country": country,
                "csv_rate": round(csv_rate, 6),
                "imf_rate": round(imf_rate, 6),
                "diff_pct": round(diff_pct * 100, 4),
            })

    return {
        "checked": checked,
        "matches": matches,
        "mismatches_count": len(mismatches),
        "accuracy_pct": round(matches / max(checked, 1) * 100, 2),
        "mismatches": sorted(mismatches, key=lambda x: x.get("diff_pct", 0)
                              if isinstance(x.get("diff_pct"), (int, float)) else 999,
                              reverse=True),
    }


# ---------------------------------------------------------------------------
# Build full single-month report
# ---------------------------------------------------------------------------

def build_report(csv_path: str, imf_countries: set, expected_ym: str,
                 year_month_api: str, include_rate_check: bool = True) -> dict:
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

    # Rate accuracy check against live IMF values
    if include_rate_check:
        imf_rates = fetch_imf_rates_for_month(year_month_api)
        checks["rate_accuracy"] = _check_rate_accuracy(df, imf_rates)

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

    # Rate accuracy issues
    if "rate_accuracy" in checks and isinstance(checks["rate_accuracy"], dict):
        ra = checks["rate_accuracy"]
        if ra.get("mismatches_count", 0) > 0:
            issues.append(f"{ra['mismatches_count']} rate mismatches vs live IMF data")

    # MoM is informational only
    movers = checks["month_on_month_changes"]
    mom_note = None
    if "large_movers_count" in movers and movers["large_movers_count"] > 0:
        mom_note = (f"{movers['large_movers_count']} currencies moved "
                    f">{MAX_MONTH_ON_MONTH_CHANGE*100:.0f}% MoM (review recommended)")

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


def save_report(report: dict, prefix: str = "validation") -> str:
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    ym = report.get("year_month", "unknown").replace("-", "_")
    path = VALIDATION_DIR / f"{prefix}_{ym}_{ts}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    return str(path)


def print_summary(report: dict):
    icon = "PASS" if report["overall_status"] == "PASS" else "FAIL"
    cov  = report["checks"]["country_coverage"]
    an   = report["checks"]["anomalous_rates"]
    mom  = report["checks"]["month_on_month_changes"]

    print(f"\n{'='*60}")
    print(f"  IMF Validation  {icon}  |  {report['csv_file']}")
    print(f"{'='*60}")
    print(f"  Rows: {report['total_rows']}  |  Coverage: {cov['csv_count']}/{cov['imf_count']} ({cov['coverage_pct']}%)")
    if cov["missing_from_csv"]:
        print(f"  Missing countries : {', '.join(cov['missing_from_csv'])}")
    null_c = report["checks"]["null_currency_codes"]
    if null_c["count"]:
        print(f"  No currency code  : {', '.join(null_c['countries'])}")
    print(f"  Null rates: {an['null_count']}  |  Zero/neg: {an['zero_or_neg_count']}  |  Dupes: {report['checks']['duplicate_records']['count']}")

    # Rate accuracy
    if "rate_accuracy" in report["checks"]:
        ra = report["checks"]["rate_accuracy"]
        if "checked" in ra:
            print(f"  Rate accuracy: {ra['accuracy_pct']}% ({ra['matches']}/{ra['checked']} match IMF)")
            if ra["mismatches_count"]:
                print(f"  Rate mismatches: {ra['mismatches_count']}")
                for m in ra["mismatches"][:5]:
                    print(f"    {m['country']}: CSV={m['csv_rate']}, IMF={m['imf_rate']} ({m['diff_pct']}% diff)")

    if "large_movers_count" in mom:
        print(f"  MoM large movers  : {mom['large_movers_count']}")
    if report["issues"]:
        print(f"\n  Issues:")
        for i in report["issues"]:
            print(f"    - {i}")
    if report.get("mom_note"):
        print(f"  Warning: {report['mom_note']}")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Historical cross-validation
# ---------------------------------------------------------------------------

def cross_validate_historical(start_api: str = None, end_api: str = None,
                               sample_months: int = None,
                               tolerance: float = 0.001,
                               logger=None) -> dict:
    """
    Cross-validates stored CSV files against live IMF API data.

    Modes:
      - Specific range: start_api='2020-01', end_api='2024-12'
      - Random sample:  sample_months=24 (picks 24 random months from available CSVs)
      - All available:  no params (validates every CSV in data/)

    For each month:
      1. Loads the stored CSV
      2. Fetches live rates from IMF API
      3. Compares every country's rate
      4. Flags mismatches beyond tolerance

    Returns a comprehensive cross-validation report.
    """
    import time

    # Find all available CSVs
    all_csvs = sorted(DATA_DIR.glob("exchange_rates_[0-9][0-9][0-9][0-9]_[0-9][0-9].csv"))
    if not all_csvs:
        return {"error": "No exchange rate CSVs found in data/"}

    # Filter by date range if specified
    csv_months = {}
    for csv_path in all_csvs:
        parts = csv_path.stem.split("_")
        try:
            ym_api = f"{parts[-2]}-{parts[-1]}"
            csv_months[ym_api] = csv_path
        except (ValueError, IndexError):
            continue

    if start_api and end_api:
        filtered = {k: v for k, v in csv_months.items()
                    if start_api <= k <= end_api}
    else:
        filtered = csv_months

    # Sample if requested
    months_to_check = list(filtered.keys())
    if sample_months and sample_months < len(months_to_check):
        months_to_check = sorted(random.sample(months_to_check, sample_months))

    if logger:
        logger.info(f"Cross-validating {len(months_to_check)} months against IMF API...")

    results = []
    total_checked = 0
    total_matches = 0
    total_mismatches = 0
    failed_fetches = []

    for idx, ym_api in enumerate(months_to_check, 1):
        csv_path = filtered[ym_api]
        if logger:
            logger.info(f"  [{idx}/{len(months_to_check)}] Validating {ym_api}...")

        # Load local CSV
        try:
            df = pd.read_csv(csv_path, encoding="utf-8-sig")
            df["Exchange_Rate"] = pd.to_numeric(df["Exchange_Rate"], errors="coerce")
        except Exception as exc:
            results.append({"month": ym_api, "status": "csv_error", "error": str(exc)})
            continue

        # Fetch live rates from IMF
        imf_rates = fetch_imf_rates_for_month(ym_api, timeout=60)
        if not imf_rates:
            failed_fetches.append(ym_api)
            results.append({"month": ym_api, "status": "imf_fetch_failed"})
            continue

        # Compare
        accuracy = _check_rate_accuracy(df, imf_rates, tolerance=tolerance)

        month_result = {
            "month": ym_api,
            "status": "validated",
            "csv_rows": len(df),
            "csv_countries": df["Country"].nunique(),
            "imf_countries": len(imf_rates),
            "checked": accuracy["checked"],
            "matches": accuracy["matches"],
            "mismatches_count": accuracy["mismatches_count"],
            "accuracy_pct": accuracy["accuracy_pct"],
        }

        if accuracy["mismatches"]:
            month_result["mismatches"] = accuracy["mismatches"][:10]  # top 10

        results.append(month_result)
        total_checked += accuracy["checked"]
        total_matches += accuracy["matches"]
        total_mismatches += accuracy["mismatches_count"]

        # Rate limit
        if idx < len(months_to_check):
            time.sleep(0.5)

    # Build summary
    validated = [r for r in results if r["status"] == "validated"]
    perfect_months = [r["month"] for r in validated if r["mismatches_count"] == 0]
    imperfect_months = [r for r in validated if r["mismatches_count"] > 0]

    report = {
        "cross_validation_at": datetime.now().isoformat(),
        "tolerance_pct": tolerance * 100,
        "summary": {
            "months_checked": len(months_to_check),
            "months_validated": len(validated),
            "months_perfect": len(perfect_months),
            "months_with_mismatches": len(imperfect_months),
            "fetch_failures": len(failed_fetches),
            "total_rates_checked": total_checked,
            "total_matches": total_matches,
            "total_mismatches": total_mismatches,
            "overall_accuracy_pct": round(total_matches / max(total_checked, 1) * 100, 4),
        },
        "overall_status": "PASS" if total_mismatches == 0 and not failed_fetches else "REVIEW",
        "failed_fetches": failed_fetches,
        "imperfect_months": [
            {"month": r["month"], "mismatches": r.get("mismatches_count", 0),
             "accuracy": r.get("accuracy_pct", 0)}
            for r in imperfect_months
        ],
        "monthly_details": results,
    }

    return report


def print_cross_validation_summary(report: dict):
    s = report["summary"]
    status = report["overall_status"]
    icon = "PASS" if status == "PASS" else "REVIEW NEEDED"

    print(f"\n{'='*65}")
    print(f"  CROSS-VALIDATION REPORT  |  {icon}")
    print(f"{'='*65}")
    print(f"  Months checked:      {s['months_checked']}")
    print(f"  Months validated:    {s['months_validated']}")
    print(f"  Perfect months:      {s['months_perfect']}")
    print(f"  Months w/ issues:    {s['months_with_mismatches']}")
    print(f"  IMF fetch failures:  {s['fetch_failures']}")
    print(f"  -----------------------------------------------")
    print(f"  Total rates checked: {s['total_rates_checked']:,}")
    print(f"  Matches:             {s['total_matches']:,}")
    print(f"  Mismatches:          {s['total_mismatches']:,}")
    print(f"  Overall accuracy:    {s['overall_accuracy_pct']}%")

    if report["imperfect_months"]:
        print(f"\n  Months with mismatches:")
        for m in report["imperfect_months"][:20]:
            print(f"    {m['month']}: {m['mismatches']} mismatches ({m['accuracy']}% accurate)")

    if report["failed_fetches"]:
        print(f"\n  Failed to fetch from IMF:")
        for f in report["failed_fetches"]:
            print(f"    - {f}")

    print(f"{'='*65}\n")


# ---------------------------------------------------------------------------
# Prefect task wrappers
# ---------------------------------------------------------------------------

try:
    from prefect import task, get_run_logger

    @task(name="validate_exchange_rate_data", retries=1, retry_delay_seconds=5)
    def validate_exchange_rate_data(csv_path: str, fail_on_issues: bool = True,
                                     include_rate_check: bool = True) -> dict:
        """
        Prefect task - validates scraped CSV against live IMF data.
        Set fail_on_issues=True to block the pipeline on bad data.
        Set include_rate_check=True to also compare actual rate values.
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
            logger.warning("IMF country list unavailable - coverage check skipped")

        report      = build_report(csv_path, imf_countries, expected_ym,
                                    year_month_api, include_rate_check=include_rate_check)
        report_path = save_report(report)
        logger.info(f"Report saved: {report_path}")

        for issue in report["issues"]:
            logger.error(f"Validation issue: {issue}")
        if report.get("mom_note"):
            logger.warning(report["mom_note"])

        if report["overall_status"] == "PASS":
            logger.info(f"Validation PASSED - {report['total_rows']} rows, "
                        f"{report['checks']['country_coverage']['coverage_pct']}% coverage")
        else:
            msg = f"Validation FAILED - {len(report['issues'])} issue(s)"
            logger.error(msg)
            if fail_on_issues:
                raise ValueError(msg)

        return report


    @task(name="cross_validate_historical_task", retries=0)
    def cross_validate_historical_task(start_api: str = None, end_api: str = None,
                                        sample_months: int = None,
                                        tolerance: float = 0.001) -> dict:
        """
        Prefect task wrapper for historical cross-validation.
        """
        logger = get_run_logger()
        report = cross_validate_historical(
            start_api=start_api, end_api=end_api,
            sample_months=sample_months, tolerance=tolerance,
            logger=logger
        )
        report_path = save_report(report, prefix="cross_validation")
        logger.info(f"Cross-validation report saved: {report_path}")
        return report

except ImportError:
    # Allow running without Prefect installed (CLI mode)
    def validate_exchange_rate_data(csv_path: str, fail_on_issues: bool = True,
                                     include_rate_check: bool = True) -> dict:
        stem  = Path(csv_path).stem
        parts = stem.split("_")
        try:
            year_month_api = f"{parts[-2]}-{parts[-1]}"
            expected_ym    = parts[-2] + parts[-1]
        except IndexError:
            lm = (datetime.today().replace(day=1) - timedelta(days=1))
            year_month_api = lm.strftime("%Y-%m")
            expected_ym    = lm.strftime("%Y%m")

        imf_countries = fetch_imf_country_list(year_month_api)
        return build_report(csv_path, imf_countries, expected_ym,
                            year_month_api, include_rate_check=include_rate_check)

    def cross_validate_historical_task(*args, **kwargs):
        return cross_validate_historical(*args, **kwargs)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="IMF Exchange Rate Data Validator")
    ap.add_argument("--csv", help="Path to a single CSV to validate")
    ap.add_argument("--fail-on-issues", action="store_true", default=False)
    ap.add_argument("--no-rate-check", action="store_true", default=False,
                    help="Skip rate accuracy check against live IMF")

    # Cross-validation options
    ap.add_argument("--cross-validate", action="store_true",
                    help="Run historical cross-validation")
    ap.add_argument("--start", help="Start period for cross-validation (YYYY-MM)")
    ap.add_argument("--end", help="End period for cross-validation (YYYY-MM)")
    ap.add_argument("--sample", type=int,
                    help="Validate a random sample of N months")
    ap.add_argument("--tolerance", type=float, default=0.001,
                    help="Rate mismatch tolerance (default: 0.001 = 0.1%%)")
    args = ap.parse_args()

    if args.cross_validate:
        print(f"Running cross-validation...")
        report = cross_validate_historical(
            start_api=args.start, end_api=args.end,
            sample_months=args.sample, tolerance=args.tolerance,
        )
        report_path = save_report(report, prefix="cross_validation")
        print_cross_validation_summary(report)
        print(f"Full report: {report_path}")

    elif args.csv:
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

        report = build_report(args.csv, imf_countries, expected_ym,
                              year_month_api,
                              include_rate_check=not args.no_rate_check)
        report_path = save_report(report)
        print_summary(report)
        print(f"Full report: {report_path}")

        if args.fail_on_issues and report["overall_status"] == "FAIL":
            raise SystemExit(1)
    else:
        ap.print_help()
