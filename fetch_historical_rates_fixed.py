# fetch_historical_rates_fixed.py
"""
Fetch ALL historical exchange rates from IMF (2024 to present)
Fixed version with proper date handling
"""

from utils.exchange_rate_fetcher import get_currency_data_from_imf, process_xml_to_dataframe
import pandas as pd
import os
from datetime import datetime
from pathlib import Path

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Configuration
START_YEAR = 2024
START_MONTH = 1

# Calculate end date (last month)
today = datetime.now()
end_year = today.year
end_month = today.month - 1

if end_month == 0:
    end_year -= 1
    end_month = 12

print("=" * 70)
print(f"FETCHING HISTORICAL EXCHANGE RATES")
print("=" * 70)
print(f"From: {START_YEAR}-{START_MONTH:02d}")
print(f"To:   {end_year}-{end_month:02d}")
print("=" * 70)

all_data = []
months_fetched = 0
months_skipped = 0

# Loop through each month
for year in range(START_YEAR, end_year + 1):
    # Determine month range for this year
    month_start = START_MONTH if year == START_YEAR else 1
    month_end = end_month if year == end_year else 12
    
    for month in range(month_start, month_end + 1):
        # Format the date
        date_str = f"{year}-{month:02d}"
        filename = f"exchange_rates_{year}_{month:02d}.csv"
        filepath = DATA_DIR / filename
        
        # Skip if already exists
        if filepath.exists():
            print(f"⏭️  {date_str}: Already exists, loading...")
            df = pd.read_csv(filepath)
            # Ensure Date is string
            df['Date'] = df['Date'].astype(str)
            all_data.append(df)
            months_skipped += 1
            continue
        
        # Fetch from IMF
        print(f"📥 {date_str}: Fetching from IMF...", end=" ", flush=True)
        
        try:
            xml_data = get_currency_data_from_imf(date_str, date_str)
            
            if xml_data:
                df = process_xml_to_dataframe(xml_data)
                
                if not df.empty:
                    # Ensure Date is string
                    df['Date'] = df['Date'].astype(str)
                    df.to_csv(filepath, index=False, encoding='utf-8-sig')
                    print(f"✅ Saved {len(df)} rows")
                    all_data.append(df)
                    months_fetched += 1
                else:
                    print("❌ No data in response")
            else:
                print("❌ Failed to fetch")
        except Exception as e:
            print(f"❌ Error: {e}")

print("=" * 70)
print(f"\n✅ Complete!")
print(f"   - Months fetched: {months_fetched}")
print(f"   - Months already existed: {months_skipped}")
print(f"   - Total months: {months_fetched + months_skipped}")

# Create combined file
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['Country', 'Date'])
    
    combined_file = DATA_DIR / "exchange_rates_ALL.csv"
    combined_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
    
    print(f"\n📊 Combined file created: {combined_file}")
    print(f"   Total rows: {len(combined_df)}")
    print(f"   Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
    print(f"   Countries: {combined_df['Country'].nunique()}")
    
    # Show monthly counts
    print("\n📅 Data by month:")
    monthly = combined_df.groupby('Date').size()
    for date, count in monthly.items():
        print(f"   {date}: {count} rows")
    
    # Show sample
    print("\n👀 First 5 rows:")
    print(combined_df.head())
    
    # Save a summary
    summary_file = DATA_DIR / "data_summary.txt"
    with open(summary_file, 'w') as f:
        f.write(f"Total rows: {len(combined_df)}\n")
        f.write(f"Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}\n")
        f.write(f"Countries: {combined_df['Country'].nunique()}\n")
        f.write("\nMonthly counts:\n")
        for date, count in monthly.items():
            f.write(f"  {date}: {count}\n")
    print(f"\n📝 Summary saved to: {summary_file}")
    
else:
    print("\n❌ No data was fetched or loaded")

print("\n👉 Next steps:")
print("   1. Check the 'data' folder for individual monthly files")
print(f"   2. View the combined file: {DATA_DIR / 'exchange_rates_ALL.csv'}")
print("   3. Use this data in your pipeline or analysis")