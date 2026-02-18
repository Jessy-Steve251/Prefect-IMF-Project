import os
import pandas as pd
from prefect import flow, get_run_logger
from prefect.artifacts import create_table_artifact, create_markdown_artifact
from utils.exchange_rate_fetcher import fetch_last_month_rates


@flow(name="currency_acquisition_flow")
def currency_acquisition_flow():
    """
    Fetch last month's FX rates (idempotent).
    Scheduled monthly via Prefect Cloud.
    """
    logger = get_run_logger()
    logger.info("Running monthly FX acquisition task...")

    fx_path = fetch_last_month_rates()

    logger.info(f"FX acquisition complete: {fx_path}")

    # Create Prefect Artifact
    try:
        df = pd.read_csv(fx_path)
        filename = os.path.basename(fx_path)
        row_count = len(df)
        logger.info(f"Data loaded. Rows: {row_count}")
        
        # 1. Create a simple Markdown Summary (Lightweight, should always appear)
        summary_md = f"""# Currency Acquisition Report
- **File**: {filename}
- **Date**: {pd.Timestamp.now()}
- **Total Rows**: {row_count}
"""
        create_markdown_artifact(
            key="exchange-rates-summary",
            markdown=summary_md,
            description="Execution Summary"
        )

        # 2. Create a Table Artifact (Native Data View)
        # Convert DataFrame to list of dictionaries
        table_data = df.to_dict('records')
        
        create_table_artifact(
            key="exchange-rates-data",
            table=table_data,
            description=f"Exchange Rates Data ({filename})"
        )
        logger.info("Successfully created table and markdown artifacts.")
        
    except Exception as e:
        logger.error(f"Failed to create artifact: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return fx_path


if __name__ == "__main__":
    currency_acquisition_flow()
