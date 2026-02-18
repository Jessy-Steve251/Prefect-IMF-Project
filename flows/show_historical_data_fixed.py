# flows/show_historical_data_fixed.py
from prefect import flow, task
from prefect.artifacts import create_table_artifact, create_markdown_artifact
import pandas as pd
from pathlib import Path

@task
def load_historical_data():
    """Load the combined historical data"""
    data_file = Path("data") / "exchange_rates_ALL.csv"
    if not data_file.exists():
        return None, "No historical data found"
    
    df = pd.read_csv(data_file)
    # Ensure Date is string
    df['Date'] = df['Date'].astype(str)
    return df, f"Loaded {len(df)} rows of historical data"

@task
def create_summary_artifact(df):
    """Create a markdown summary artifact"""
    summary = f"""# Historical Exchange Rates Summary
- **Total Rows**: {len(df):,}
- **Date Range**: {df['Date'].min()} to {df['Date'].max()}
- **Countries**: {df['Country'].nunique()}
- **Months**: {df['Date'].nunique()}

## Data by Month
"""
    monthly = df.groupby('Date').size()
    for date, count in monthly.items():
        summary += f"- {date}: {count} rows\n"
    
    create_markdown_artifact(
        key="historical-summary",
        markdown=summary,
        description="Historical Exchange Rates Summary"
    )
    return summary

@task
def create_table_artifact_task(df):
    """Create a table artifact with sample data"""
    # Take first 100 rows for the table (to avoid overwhelming the UI)
    sample_df = df.head(100)
    
    # Convert to records for the artifact
    table_data = sample_df.to_dict('records')
    
    create_table_artifact(
        key="historical-data-sample",
        table=table_data,
        description="Historical Exchange Rates (First 100 rows)"
    )
    print(f"✅ Created table artifact with {len(sample_df)} rows")

@flow(name="show-historical-data")
def show_historical_flow():
    """Flow to display historical data in Prefect UI"""
    print("📊 Loading historical exchange rates...")
    
    df, message = load_historical_data()
    
    if df is None:
        print(f"❌ {message}")
        return
    
    print(f"✅ {message}")
    print(f"   Date range: {df['Date'].min()} to {df['Date'].max()}")
    
    # Create artifacts
    create_summary_artifact(df)
    create_table_artifact_task(df)
    
    print("\n✅ Artifacts created! Check the Prefect UI:")
    print("   http://127.0.0.1:4200")
    print("   Look for the flow run 'calculating-locust' and click on it")
    print("   Then go to the 'Artifacts' tab")

if __name__ == "__main__":
    show_historical_flow()