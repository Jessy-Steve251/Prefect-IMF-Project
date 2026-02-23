# export_complete_dataset.py
"""
Export complete historical dataset for analysis
Supports multiple formats and filtering options
"""

import pandas as pd
import glob
import os
from datetime import datetime
import argparse

def load_complete_dataset(data_source="auto"):
    """
    Load the complete dataset from various sources
    
    Args:
        data_source: "auto", "combined", "monthly", or specific file path
    """
    print("=" * 60)
    print("📊 LOADING COMPLETE DATASET")
    print("=" * 60)
    
    # Try different sources
    if data_source == "auto":
        # Try combined files first
        combined_files = [
            "data/exchange_rates_2000_to_present.csv",
            "data/exchange_rates_ALL.csv",
            "data/exchange_rates_combined.csv"
        ]
        
        for file in combined_files:
            if os.path.exists(file):
                print(f"📂 Found combined file: {file}")
                df = pd.read_csv(file)
                df['Date'] = df['Date'].astype(str)
                print(f"✅ Loaded {len(df):,} rows from {file}")
                return df, file
        
        # Fall back to monthly files
        print("📂 No combined file found. Loading all monthly files...")
        monthly_files = sorted(glob.glob("data/exchange_rates_2*.csv"))
        
        if not monthly_files:
            print("❌ No data files found!")
            return None, None
        
        print(f"📁 Found {len(monthly_files)} monthly files")
        
        dfs = []
        for i, file in enumerate(monthly_files):
            df = pd.read_csv(file)
            df['Date'] = df['Date'].astype(str)
            dfs.append(df)
            print(f"  Progress: {i+1}/{len(monthly_files)} - {os.path.basename(file)} ({len(df):,} rows)")
        
        combined_df = pd.concat(dfs, ignore_index=True)
        combined_df = combined_df.sort_values(['Country', 'Date'])
        print(f"\n✅ Combined {len(monthly_files)} files into {len(combined_df):,} rows")
        return combined_df, "monthly_combined"
    
    elif os.path.exists(data_source):
        # Load specific file
        print(f"📂 Loading specified file: {data_source}")
        df = pd.read_csv(data_source)
        df['Date'] = df['Date'].astype(str)
        print(f"✅ Loaded {len(df):,} rows")
        return df, data_source
    
    else:
        print(f"❌ File not found: {data_source}")
        return None, None

def export_to_csv(df, filename=None, encoding='utf-8-sig'):
    """Export to CSV with proper encoding"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/export_complete_dataset_{timestamp}.csv"
    
    # Save with UTF-8-BOM encoding (best for Excel)
    df.to_csv(filename, index=False, encoding='utf-8-sig')
    
    # Also save a version with different encodings for compatibility
    base_name = filename.replace('.csv', '')
    
    # UTF-8 (without BOM) - good for Python/R
    df.to_csv(f"{base_name}_utf8.csv", index=False, encoding='utf-8')
    
    # Latin-1 - good for older systems
    df.to_csv(f"{base_name}_latin1.csv", index=False, encoding='latin-1')
    
    print(f"\n✅ Exported with multiple encodings:")
    print(f"   - UTF-8-BOM (Excel): {filename}")
    print(f"   - UTF-8 (Python/R): {base_name}_utf8.csv")
    print(f"   - Latin-1 (Legacy): {base_name}_latin1.csv")
    return filename

def export_to_excel(df, filename=None):
    """Export to Excel with multiple sheets"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/export_complete_dataset_{timestamp}.xlsx"
    
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:
        # Main data sheet
        df.to_excel(writer, sheet_name='All Data', index=False)
        
        # Summary sheet
        summary = pd.DataFrame({
            'Metric': ['Total Rows', 'Date Range', 'Countries', 'Months', 'Years'],
            'Value': [
                f"{len(df):,}",
                f"{df['Date'].min()} to {df['Date'].max()}",
                f"{df['Country'].nunique():,}",
                f"{df['Date'].nunique():,}",
                f"{len(df['Date'].str[:4].unique())}"
            ]
        })
        summary.to_excel(writer, sheet_name='Summary', index=False)
        
        # Pivot table by year and country count
        pivot = df.groupby([df['Date'].str[:4], 'Country']).size().unstack().fillna(0)
        pivot.to_excel(writer, sheet_name='Yearly Matrix')
    
    print(f"\n✅ Exported to Excel: {filename}")
    return filename

def export_to_parquet(df, filename=None):
    """Export to Parquet (efficient for large datasets)"""
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data/export_complete_dataset_{timestamp}.parquet"
    
    df.to_parquet(filename, index=False)
    print(f"\n✅ Exported {len(df):,} rows to Parquet: {filename}")
    return filename

def generate_analysis_report(df):
    """Generate comprehensive analysis report"""
    print("\n" + "=" * 60)
    print("📊 COMPLETE DATASET ANALYSIS")
    print("=" * 60)
    
    # Basic stats
    print(f"\n📋 Dataset Overview:")
    print(f"   Total Rows: {len(df):,}")
    print(f"   Total Columns: {len(df.columns)}")
    print(f"   Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Temporal coverage
    years = sorted(df['Date'].str[:4].unique())
    months = sorted(df['Date'].unique())
    print(f"\n📅 Temporal Coverage:")
    print(f"   Date Range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"   Years: {len(years)} ({min(years)}-{max(years)})")
    print(f"   Months: {len(months):,}")
    
    # Geographic coverage
    countries = df['Country'].nunique()
    print(f"\n🌍 Geographic Coverage:")
    print(f"   Countries: {countries:,}")
    
    # Currency distribution
    top_currencies = df['Currency'].value_counts().head(10)
    print(f"\n💰 Top 10 Currencies:")
    for currency, count in top_currencies.items():
        pct = (count / len(df)) * 100
        print(f"   {currency}: {count:,} rows ({pct:.1f}%)")
    
    # Monthly averages
    avg_per_month = len(df) / len(months)
    print(f"\n📈 Averages:")
    print(f"   Avg rows per month: {avg_per_month:.0f}")
    print(f"   Avg rows per country: {len(df)/countries:.0f}")
    
    # Data quality
    missing = df.isnull().sum()
    if missing.sum() > 0:
        print(f"\n⚠️ Missing Data:")
        for col, count in missing[missing > 0].items():
            print(f"   {col}: {count:,} missing ({count/len(df)*100:.1f}%)")
    else:
        print(f"\n✅ No missing data detected")
    
    return {
        'rows': len(df),
        'countries': countries,
        'months': len(months),
        'years': len(years),
        'date_min': df['Date'].min(),
        'date_max': df['Date'].max()
    }

def main():
    parser = argparse.ArgumentParser(description='Export complete dataset for analysis')
    parser.add_argument('--source', type=str, default='auto',
                       help='Data source (auto, combined file path, or "monthly")')
    parser.add_argument('--format', type=str, default='csv',
                       choices=['csv', 'excel', 'parquet', 'all'],
                       help='Export format')
    parser.add_argument('--output', type=str, default=None,
                       help='Output filename')
    
    args = parser.parse_args()
    
    # Load complete dataset
    df, source = load_complete_dataset(args.source)
    
    if df is None:
        return
    
    # Generate analysis report
    stats = generate_analysis_report(df)
    
    # Export based on format
    if args.format == 'csv' or args.format == 'all':
        export_to_csv(df, args.output)
    
    if args.format == 'excel' or args.format == 'all':
        export_to_excel(df, args.output)
    
    if args.format == 'parquet' or args.format == 'all':
        export_to_parquet(df, args.output)
    
    print("\n" + "=" * 60)
    print("✅ EXPORT COMPLETE!")
    print("=" * 60)

if __name__ == "__main__":
    main()