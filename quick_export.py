# quick_export.py
import pandas as pd
import glob
import os
from datetime import datetime

print("Loading all exchange rate files...")
all_files = sorted(glob.glob("data/exchange_rates_*.csv"))
dfs = []

# Filter out combined files
valid_files = [f for f in all_files if "ALL" not in f and "2000" not in f and "complete" not in f]

for file in valid_files:
    df = pd.read_csv(file)
    dfs.append(df)
    print(f"  Loaded: {os.path.basename(file)} ({len(df)} rows)")

# Combine
complete_df = pd.concat(dfs, ignore_index=True)
complete_df = complete_df.sort_values(['Country', 'Date'])

print(f"\n✅ Complete dataset: {len(complete_df):,} rows")
print(f"   Date range: {complete_df['Date'].min()} to {complete_df['Date'].max()}")
print(f"   Countries: {complete_df['Country'].nunique()}")

# Create timestamp for filename
timestamp = datetime.now().strftime("%Y%m%d")

# Export with different encodings
output_base = f"data/complete_dataset_{timestamp}"

# UTF-8 with BOM (Excel friendly)
complete_df.to_csv(f"{output_base}_excel.csv", index=False, encoding='utf-8-sig')
print(f"\n📁 Excel-friendly (UTF-8-BOM): {output_base}_excel.csv")

# UTF-8 without BOM (Python/R friendly)
complete_df.to_csv(f"{output_base}_utf8.csv", index=False, encoding='utf-8')
print(f"📁 Python/R-friendly (UTF-8): {output_base}_utf8.csv")

# Also save a copy as the default
complete_df.to_csv("data/complete_dataset_for_analysis.csv", index=False, encoding='utf-8-sig')
print(f"📁 Default: data/complete_dataset_for_analysis.csv")

# Create a metadata file
with open(f"{output_base}_metadata.txt", 'w') as f:
    f.write(f"Dataset generated: {datetime.now()}\n")
    f.write(f"Total rows: {len(complete_df):,}\n")
    f.write(f"Date range: {complete_df['Date'].min()} to {complete_df['Date'].max()}\n")
    f.write(f"Countries: {complete_df['Country'].nunique()}\n")
    f.write(f"Months: {complete_df['Date'].nunique()}\n")
    f.write(f"Files combined: {len(valid_files)}\n")
    f.write(f"Encoding: UTF-8-BOM (Excel), UTF-8 (Python/R)\n")

print(f"\n📝 Metadata saved: {output_base}_metadata.txt")

# Preview
print("\n📊 Preview (first 5 rows):")
print(complete_df[['Country', 'Currency', 'Date', 'Exchange_Rate']].head())