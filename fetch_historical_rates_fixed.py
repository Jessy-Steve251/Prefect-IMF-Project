# fetch_historical_rates_fixed.py
"""
Fetch ALL historical exchange rates from IMF (2000 to present)
Fixed version with proper date handling
"""

from utils.exchange_rate_fetcher import get_currency_data_from_imf, process_xml_to_dataframe
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import time

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Configuration - CHANGED FROM 2024 TO 2000
START_YEAR = 2000
START_MONTH = 1

# Calculate end date (last month)
today = datetime.now()
end_year = today.year
end_month = today.month - 1

if end_month == 0:
    end_year -= 1
    end_month = 12

print("=" * 80)
print(f"📥 FETCHING HISTORICAL EXCHANGE RATES 2000-{end_year}")
print("=" * 80)
print(f"From: {START_YEAR}-{START_MONTH:02d}")
print(f"To:   {end_year}-{end_month:02d}")
print(f"Total months to process: {(end_year - START_YEAR) * 12 + (end_month - START_MONTH + 1)}")
print("=" * 80)

all_data = []
months_fetched = 0
months_skipped = 0
months_failed = 0

# Loop through each month
for year in range(START_YEAR, end_year + 1):
    # Determine month range for this year
    month_start = START_MONTH if year == START_YEAR else 1
    month_end = end_month if year == end_year else 12
    
    print(f"\n📅 Year {year} (Months {month_start}-{month_end})")
    print("-" * 40)
    
    for month in range(month_start, month_end + 1):
        # Format the date
        date_str = f"{year}-{month:02d}"
        filename = f"exchange_rates_{year}_{month:02d}.csv"
        filepath = DATA_DIR / filename
        
        # Skip if already exists
        if filepath.exists():
            print(f"  ⏭️  {date_str}: Already exists, loading...")
            df = pd.read_csv(filepath)
            # Ensure Date is string
            df['Date'] = df['Date'].astype(str)
            all_data.append(df)
            months_skipped += 1
            continue
        
        # Fetch from IMF
        print(f"  📥 {date_str}: Fetching...", end=" ", flush=True)
        
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
                    months_failed += 1
            else:
                print("❌ Failed to fetch")
                months_failed += 1
                
            # Small delay to avoid rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ Error: {e}")
            months_failed += 1

print("\n" + "=" * 80)
print(f"✅ FETCH COMPLETE!")
print("=" * 80)
print(f"   - Months fetched: {months_fetched}")
print(f"   - Months already existed: {months_skipped}")
print(f"   - Months failed: {months_failed}")
print(f"   - Total months processed: {months_fetched + months_skipped + months_failed}")

# Create combined file
if all_data:
    print("\n📊 Creating combined dataset...")
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['Country', 'Date'])
    
    # Save as ALL.csv
    combined_file = DATA_DIR / "exchange_rates_ALL.csv"
    combined_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ Created: {combined_file}")
    
    # Save as 2000_to_present.csv (more descriptive)
    combined_file_2000 = DATA_DIR / "exchange_rates_2000_to_present.csv"
    combined_df.to_csv(combined_file_2000, index=False, encoding='utf-8-sig')
    print(f"   ✅ Created: {combined_file_2000}")
    
    # Statistics
    print(f"\n📊 DATASET STATISTICS:")
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
    print(f"   Countries: {combined_df['Country'].nunique():,}")
    print(f"   Months: {combined_df['Date'].nunique():,}")
    print(f"   Years: {len(combined_df['Date'].str[:4].unique())}")
    
    # Yearly summary
    print("\n📅 DATA BY YEAR:")
    combined_df['Year'] = combined_df['Date'].str[:4]
    yearly = combined_df.groupby('Year').size()
    for year, count in yearly.items():
        months_in_year = combined_df[combined_df['Year'] == year]['Date'].nunique()
        print(f"   {year}: {count:6,} rows across {months_in_year} months")
    
    # Save detailed summary
    summary_file = DATA_DIR / "data_summary_2000_to_present.txt"
    with open(summary_file, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("HISTORICAL EXCHANGE RATES SUMMARY (2000-PRESENT)\n")
        f.write("=" * 60 + "\n\n")
        f.write(f"Total rows: {len(combined_df):,}\n")
        f.write(f"Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}\n")
        f.write(f"Countries: {combined_df['Country'].nunique():,}\n")
        f.write(f"Months: {combined_df['Date'].nunique():,}\n")
        f.write(f"Years: {len(combined_df['Year'].unique())}\n\n")
        f.write("Monthly counts by year:\n")
        for year, count in yearly.items():
            months_in_year = combined_df[combined_df['Year'] == year]['Date'].nunique()
            f.write(f"  {year}: {count:6,} rows ({months_in_year} months)\n")
    
    print(f"\n📝 Detailed summary saved to: {summary_file}")
    
    # Sample data
    print("\n👀 First 5 rows of data:")
    print(combined_df[['Country', 'Currency', 'Date', 'Exchange_Rate']].head())
    
else:
    print("\n❌ No data was fetched or loaded")

print("\n" + "=" * 80)
print("✅ PROCESSING COMPLETE!")
print("=" * 80)
print("\n👉 Next steps:")
print("   1. Check the 'data' folder for individual monthly files")
print("   2. View combined files: data/exchange_rates_ALL.csv")
print("   3. View 2000-present file: data/exchange_rates_2000_to_present.csv")
print("   4. Run export_data.bat to create analysis-ready dataset")
print("   5. View in Prefect UI: python -m flows.show_historical_data_fixed")