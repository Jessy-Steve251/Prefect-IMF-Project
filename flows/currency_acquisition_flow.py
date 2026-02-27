"""
currency_acquisition_flow.py
-----------------------------
Flow 1 of 3 in the hybrid execution model.

Execution:  Local Python via Prefect worker (triggered by Windows Task Scheduler)
Monitoring: Prefect Cloud (logs, artifacts, run history)

On success, directly calls prepare_batch_flow() so the chain runs
locally — no cloud scheduling involved, no time-gap race condition.
"""

import os
import pandas as pd
from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact

from utils.exchange_rate_fetcher import fetch_last_month_rates, last_month_api_str
from utils.imf_data_validator import validate_exchange_rate_data
from utils.config import ensure_dirs


@flow(name="currency_acquisition_flow", retries=2, retry_delay_seconds=60)
def currency_acquisition_flow():
    """
    Step 1: Fetch last month's IMF exchange rates and validate completeness.
    On success, chains directly into prepare_batch_flow.

    Triggered by: Windows Task Scheduler via run_CurrencyAcquisition.bat
    Monitored by: Prefect Cloud
    """
    logger = get_run_logger()
    ensure_dirs()

    year_month = last_month_api_str()
    logger.info(f"Starting FX acquisition for {year_month}...")

    # ------------------------------------------------------------------
    # Step 1: Fetch from IMF API
    # ------------------------------------------------------------------
    fx_path = fetch_last_month_rates(logger=logger)
    logger.info(f"Acquisition complete: {fx_path}")

    # ------------------------------------------------------------------
    # Step 2: Validate — raises ValueError if data is incomplete/corrupt,
    # which stops the chain before bad data reaches the batch pipeline
    # ------------------------------------------------------------------
    report   = validate_exchange_rate_data(fx_path, fail_on_issues=True)
    coverage = report["checks"]["country_coverage"]["coverage_pct"]
    logger.info(
        f"Validation passed — {report['total_rows']} rows, "
        f"{coverage}% IMF country coverage"
    )

    # ------------------------------------------------------------------
    # Step 3: Prefect Cloud artifacts (visible in Cloud UI)
    # ------------------------------------------------------------------
    try:
        df = pd.read_csv(fx_path, encoding="utf-8-sig")

        create_markdown_artifact(
            key="exchange-rates-summary",
            markdown=f"""# Currency Acquisition — {year_month}
| Field | Value |
|---|---|
| File | {os.path.basename(fx_path)} |
| Rows | {len(df)} |
| Countries | {df['Country'].nunique()} |
| Currencies | {df['Currency'].nunique()} |
| IMF Coverage | {coverage}% |
| Validation | ✅ PASSED |
| Run at | {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')} |
""",
            description="Monthly FX Acquisition Summary",
        )

        create_table_artifact(
            key="exchange-rates-data",
            table=df.to_dict("records"),
            description=f"Exchange Rates — {year_month}",
        )
        logger.info("Prefect Cloud artifacts created.")

    except Exception as exc:
        logger.warning(f"Artifact creation failed (non-fatal): {exc}")

    # ------------------------------------------------------------------
    # Step 4: Chain to Flow 2 — direct local call.
    # Prefect records this as a child flow run in Cloud UI.
    # Only reached if fetch + validation both passed.
    # ------------------------------------------------------------------
    logger.info("Handing off to prepare_batch_flow...")
    from flows.prepare_batch_flow import prepare_batch_flow
    prepare_batch_flow()

    return fx_path


if __name__ == "__main__":
    currency_acquisition_flow()
