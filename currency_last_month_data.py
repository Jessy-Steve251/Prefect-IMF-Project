import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import json

# --- Global cache for country currency codes ---
# This dictionary will store country codes and their currency to avoid repeated API calls.
currency_cache = {}

# --- Hardcoded overrides for specific country/region codes ---
# Requirement 1 & 2: Handle special cases for specific codes.
SPECIAL_CURRENCY_OVERRIDES = {
    'CUW': 'XCG',  # Curaçao
    'SXM': 'XCG',  # Sint Maarten
    'G163': 'EUR'  # G163 Group, Actually Eurozone
}

def get_official_currency(country_code):
    """
    Fetches the official currency code for a given country using the REST Countries API.
    Uses a local cache and special overrides to avoid redundant or incorrect API calls.
    
    :param country_code: The 3-letter ISO code for the country.
    :return: The 3-letter currency code (e.g., 'AWG') or None if not found.
    """
    # First, check for special overrides
    if country_code in SPECIAL_CURRENCY_OVERRIDES:
        return SPECIAL_CURRENCY_OVERRIDES[country_code]

    # Second, check cache
    if country_code in currency_cache:
        return currency_cache[country_code]

    try:
        url = f"https://restcountries.com/v3.1/alpha/{country_code}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Python-Currency-App/1.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            if response.getcode() == 200:
                data = json.loads(response.read().decode("utf-8"))
                if data and 'currencies' in data[0]:
                    # Get the first currency code from the currencies object
                    currency_code_3_letter = list(data[0]['currencies'].keys())[0]
                    # Store in cache
                    currency_cache[country_code] = currency_code_3_letter
                    return currency_code_3_letter
    except Exception as e:
        print(f"Warning: Could not fetch currency for {country_code}. Error: {e}")
    
    # Cache the failure to avoid retrying
    currency_cache[country_code] = None
    return None

def get_currency_data(start_date, end_date):
    """
    Fetches exchange rate data from the IMF API for a specified time range.
    """
    flowRef = 'IMF.STA,ER'
    # This key gets data for all countries against the USD/SDR rate.
    key = '.USD_XDC.PA_RT.M'
    
    try:
        url = f"https://api.imf.org/external/sdmx/2.1/data/{flowRef}/{key}?startPeriod={start_date}&endPeriod={end_date}&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
        
        hdr = {
            'Cache-Control': 'no-cache',
        }

        req = urllib.request.Request(url, headers=hdr, method='GET')
        response = urllib.request.urlopen(req)
        
        if response.getcode() == 200:
            data = response.read().decode("utf-8")
            return data
            
    except Exception as e:
        print(f"Error: {e}")
        return None

def process_xml_to_csv(xml_data):
    """
    Parses XML data and converts it into a Pandas DataFrame, adding new columns.
    """
    # Parse XML data
    root = ET.fromstring(xml_data)
    
    # Prepare data list
    data_list = []
    
    # Get the timestamp for when the data is being processed
    fetch_timestamp = datetime.now().isoformat()
    
    # Iterate over all Series nodes
    for series in root.findall(".//Series"):
        country_code = series.get('COUNTRY')
        indicator = series.get('INDICATOR') # e.g., 'USD_XDC'
        
        # Requirement 1: Get the country's official currency
        official_currency = get_official_currency(country_code)
        
        # Requirement 2: Determine the base currency from the indicator
        # For 'USD_XDC', the rate is of the country's currency vs USD.
        base_currency = 'USD' if indicator == 'USD_XDC' else indicator.split('_')[-1]

        # Get all observations for this country
        for obs in series.findall('Obs'):
            time_period = obs.get('TIME_PERIOD')
            value = obs.get('OBS_VALUE')
            
            year_month = time_period.replace('-M', '')
            
            data_list.append({
                'Country': country_code,
                'Currency': official_currency, # New Column 1
                'Date': year_month,
                'Exchange_Rate': float(value),
                'Base_Currency': base_currency, # New Column 2
                'Timestamp': fetch_timestamp    # New Column 3
            })
    
    # Create DataFrame
    df = pd.DataFrame(data_list)
    
    if df.empty:
        return df
    
    # Convert date format, keeping only year and month
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m').dt.strftime('%Y%m')
    
    # Sort by Country and Date
    df = df.sort_values(['Country', 'Date'])
    
    return df

def save_data(data, output_dir, year_month_str):
    """
    Saves the raw XML data and the processed CSV data.
    """
    # Create output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Save raw XML data
    xml_file = os.path.join(output_dir, f"raw_exchange_rates_{year_month_str}.xml")
    with open(xml_file, "w", encoding="utf-8") as f:
        f.write(data)
    print(f"Raw XML data saved to: {xml_file}")
    
    # Process and save CSV data
    df = process_xml_to_csv(data)
    if df.empty:
        print("No data found for last month, not generating CSV file.")
        return
        
    csv_file = os.path.join(output_dir, f"exchange_rates_{year_month_str}.csv")
    # Add encoding and separator parameters
    df.to_csv(csv_file, index=False, encoding='utf-8-sig', sep=',')
    print(f"Processed CSV data saved to: {csv_file}")
    
    # Modify the part for saving individual country data
    for country in df['Country'].unique():
        country_df = df[df['Country'] == country]
        country_file = os.path.join(output_dir, f"exchange_rates_{year_month_str}_{country}.csv")
        # Add encoding and separator parameters
        country_df.to_csv(country_file, index=False, encoding='utf-8-sig', sep=',')
        print(f"Exchange rate data for {country} saved to: {country_file}")

def main():
    """
    Main function to execute the entire process of data fetching, processing, and saving.
    """
    # Dynamically calculate the start and end dates of the previous month
    today = datetime.now()
    first_day_of_current_month = today.replace(day=1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    
    start_date = last_day_of_previous_month.strftime("%Y-%m")
    end_date = last_day_of_previous_month.strftime("%Y-%m")
    
    # Create a year_month string for file and folder names, e.g., '2025_08'
    year_month_str = last_day_of_previous_month.strftime("%Y_%m")
    
    print(f"Preparing to fetch exchange rate data for {start_date}...")
    
    # Set output directory, including year and month
    output_dir = f"currency_data_{year_month_str}"
    
    # Fetch data
    print("Fetching data...")
    data = get_currency_data(start_date, end_date)
    
    if data:
        print("Data fetched successfully, starting processing...")
        save_data(data, output_dir, year_month_str)
        print("All data processing complete!")
    else:
        print("Failed to fetch data!")

if __name__ == "__main__":
    main()