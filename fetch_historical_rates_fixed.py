# fetch_historical_rates_fixed.py (improved version)
"""
Fetch ALL historical exchange rates from IMF (2000 to present)
Enhanced version with country-based completeness checking
"""

from utils.exchange_rate_fetcher import get_currency_data_from_imf, process_xml_to_dataframe
import pandas as pd
import os
from datetime import datetime
from pathlib import Path
import time
import json

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Track country presence across all months
COUNTRY_TRACKER_FILE = DATA_DIR / "country_presence.json"

# Load existing country tracker if available
if COUNTRY_TRACKER_FILE.exists():
    with open(COUNTRY_TRACKER_FILE, 'r') as f:
        country_presence = json.load(f)
    print(f"📊 Loaded country presence data for {len(country_presence)} countries")
else:
    country_presence = {}

# Configuration - 2000 to present
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
months_refetched = 0
suspicious_months = []

# Track countries per month for this run
current_run_countries = {}

# Loop through each month
for year in range(START_YEAR, end_year + 1):
    month_start = START_MONTH if year == START_YEAR else 1
    month_end = end_month if year == end_year else 12
    
    print(f"\n📅 Year {year} (Months {month_start}-{month_end})")
    print("-" * 40)
    
    for month in range(month_start, month_end + 1):
        date_str = f"{year}-{month:02d}"
        month_key = f"{year}_{month:02d}"
        filename = f"exchange_rates_{year}_{month:02d}.csv"
        filepath = DATA_DIR / filename
        
        should_fetch = False
        reason = ""
        
        # Check if file exists
        if filepath.exists():
            df_existing = pd.read_csv(filepath)
            existing_countries = set(df_existing['Country'].unique())
            
            # Compare with historical presence for this month
            if month_key in country_presence:
                expected_countries = set(country_presence[month_key].get('countries', []))
                
                # Check if we're missing any countries that usually appear
                missing_countries = expected_countries - existing_countries
                
                if missing_countries:
                    should_fetch = True
                    reason = f"missing {len(missing_countries)} countries that usually appear"
                    print(f"  ⚠️  {date_str}: {reason}")
                    print(f"     Missing: {sorted(missing_countries)[:5]}...")
                else:
                    print(f"  ⏭️  {date_str}: Already exists ({len(df_existing)} rows, {len(existing_countries)} countries)")
                    df_existing['Date'] = df_existing['Date'].astype(str)
                    all_data.append(df_existing)
                    months_skipped += 1
                    # Track countries for this month
                    current_run_countries[month_key] = list(existing_countries)
                    continue
            else:
                # First time seeing this month, accept as is
                print(f"  ⏭️  {date_str}: Already exists (first time tracking)")
                df_existing['Date'] = df_existing['Date'].astype(str)
                all_data.append(df_existing)
                months_skipped += 1
                current_run_countries[month_key] = list(existing_countries)
                continue
        else:
            should_fetch = True
            reason = "file doesn't exist"
        
        # Fetch from IMF
        if should_fetch:
            print(f"  📥 {date_str}: Fetching ({reason})...", end=" ", flush=True)
            
            try:
                xml_data = get_currency_data_from_imf(date_str, date_str)
                
                if xml_data:
                    df = process_xml_to_dataframe(xml_data)
                    
                    if not df.empty:
                        df['Date'] = df['Date'].astype(str)
                        new_countries = set(df['Country'].unique())
                        
                        # Check if we got reasonable data
                        if len(new_countries) > 50:  # Basic sanity check
                            # Backup old file if it existed
                            if filepath.exists():
                                backup_path = filepath.with_suffix('.csv.bak')
                                filepath.rename(backup_path)
                                print(f"\n     📦 Backed up old file")
                                months_refetched += 1
                            else:
                                months_fetched += 1
                            
                            # Save new file
                            df.to_csv(filepath, index=False, encoding='utf-8-sig')
                            print(f"✅ Saved {len(df)} rows, {len(new_countries)} countries")
                            all_data.append(df)
                            
                            # Track countries for this month
                            current_run_countries[month_key] = list(new_countries)
                            
                            # Check for specific countries of interest
                            for country in ['GHA', 'NGA', 'ZAF', 'KEN']:
                                if country in new_countries:
                                    rate = df[df['Country'] == country]['Exchange_Rate'].iloc[0]
                                    print(f"     ✅ {country}: {rate}")
                        else:
                            print(f"⚠️  Only got {len(new_countries)} countries (suspicious)")
                            suspicious_months.append(date_str)
                            months_failed += 1
                    else:
                        print("❌ No data")
                        months_failed += 1
                else:
                    print("❌ Failed")
                    months_failed += 1
                    
                time.sleep(0.5)
                
            except Exception as e:
                print(f"❌ Error: {e}")
                months_failed += 1

# Update country presence database
print("\n" + "=" * 80)
print("📊 UPDATING COUNTRY PRESENCE DATABASE")
print("=" * 80)

# Merge with existing data
for month_key, countries in current_run_countries.items():
    if month_key not in country_presence:
        country_presence[month_key] = {'countries': countries, 'count': len(countries)}
    else:
        # Update with any new countries
        existing = set(country_presence[month_key].get('countries', []))
        updated = existing.union(set(countries))
        country_presence[month_key] = {
            'countries': list(updated),
            'count': len(updated)
        }

# Save tracker
with open(COUNTRY_TRACKER_FILE, 'w') as f:
    json.dump(country_presence, f, indent=2)
print(f"✅ Saved country presence data for {len(country_presence)} months")

print("\n" + "=" * 80)
print(f"✅ FETCH COMPLETE!")
print("=" * 80)
print(f"   - New months fetched: {months_fetched}")
print(f"   - Months already existed: {months_skipped}")
print(f"   - Months re-fetched (incomplete): {months_refetched}")
print(f"   - Months failed: {months_failed}")
print(f"   - Suspicious months (low country count): {len(suspicious_months)}")
if suspicious_months:
    print(f"   - Check these months: {suspicious_months[:10]}")

# Create combined file
if all_data:
    print("\n📊 Creating combined dataset...")
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df = combined_df.sort_values(['Country', 'Date'])
    
    # Save combined files
    combined_file = DATA_DIR / "exchange_rates_ALL.csv"
    combined_df.to_csv(combined_file, index=False, encoding='utf-8-sig')
    print(f"   ✅ Created: {combined_file}")
    
    combined_file_2000 = DATA_DIR / "exchange_rates_2000_to_present.csv"
    combined_df.to_csv(combined_file_2000, index=False, encoding='utf-8-sig')
    print(f"   ✅ Created: {combined_file_2000}")
    
    # Statistics
    print(f"\n📊 DATASET STATISTICS:")
    print(f"   Total rows: {len(combined_df):,}")
    print(f"   Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
    print(f"   Countries: {combined_df['Country'].nunique():,}")
    
    # Country consistency report
    print(f"\n📋 COUNTRY CONSISTENCY REPORT:")
    country_month_counts = combined_df.groupby('Country').size()
    always_present = country_month_counts[country_month_counts == len(current_run_countries)].index.tolist()
    print(f"   Countries appearing every month: {len(always_present)}")
    print(f"   Examples: {always_present[:10]}")

print("\n" + "=" * 80)
print("✅ PROCESSING COMPLETE!")
print("=" * 80)