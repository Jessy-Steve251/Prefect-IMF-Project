import os
import json
import urllib.request
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

BASE_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# Global cache for country currency codes
currency_cache = {}

# Hardcoded overrides for specific country/region codes
SPECIAL_CURRENCY_OVERRIDES = {
    'CUW': 'XCG',  # Curaçao
    'SXM': 'XCG',  # Sint Maarten
    'G163': 'EUR'  # G163 Group, Actually Eurozone
}


def get_official_currency(country_code):
    """
    Fetches the official currency code for a given country using the REST Countries API.
    Uses a local cache and special overrides to avoid redundant or incorrect API calls.
    
    Args:
        country_code: The 3-letter ISO code for the country.
    
    Returns:
        The 3-letter currency code (e.g., 'AWG') or None if not found.
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


def get_currency_data_from_imf(start_date, end_date):
    """
    Fetches exchange rate data from the IMF API for a specified time range.
    
    Args:
        start_date: Start date in format 'YYYY-MM'
        end_date: End date in format 'YYYY-MM'
    
    Returns:
        XML data as string, or None if request failed
    """
    flowRef = 'IMF.STA,ER'
    key = '.USD_XDC.PA_RT.M'
    
    try:
        url = f"https://api.imf.org/external/sdmx/2.1/data/{flowRef}/{key}?startPeriod={start_date}&endPeriod={end_date}&dimensionAtObservation=TIME_PERIOD&detail=dataonly&includeHistory=false"
        
        hdr = {
            'Cache-Control': 'no-cache',
        }

        req = urllib.request.Request(url, headers=hdr, method='GET')
        response = urllib.request.urlopen(req, timeout=10)
        
        if response.getcode() == 200:
            data = response.read().decode("utf-8")
            return data
            
    except Exception as e:
        print(f"Error fetching from IMF API: {e}")
        return None


def process_xml_to_dataframe(xml_data):
    """
    Parses XML data and converts it into a Pandas DataFrame.
    
    Args:
        xml_data: XML string from IMF API
    
    Returns:
        Pandas DataFrame with processed exchange rate data
    """
    try:
        root = ET.fromstring(xml_data)
    except Exception as e:
        print(f"Error parsing XML: {e}")
        return pd.DataFrame()
    
    # Prepare data list
    data_list = []
    
    # Get the timestamp for when the data is being processed
    fetch_timestamp = datetime.now().isoformat()
    
    # Iterate over all Series nodes
    for series in root.findall(".//Series"):
        country_code = series.get('COUNTRY')
        indicator = series.get('INDICATOR')  # e.g., 'USD_XDC'
        
        # Get the country's official currency
        official_currency = get_official_currency(country_code)
        
        # Determine the base currency from the indicator
        base_currency = 'USD' if indicator == 'USD_XDC' else indicator.split('_')[-1]

        # Get all observations for this country
        for obs in series.findall('Obs'):
            time_period = obs.get('TIME_PERIOD')
            value = obs.get('OBS_VALUE')
            
            if value is None:
                continue
            
            year_month = time_period.replace('-M', '')
            
            data_list.append({
                'Country': country_code,
                'Currency': official_currency,
                'Date': year_month,
                'Exchange_Rate': float(value),
                'Base_Currency': base_currency,
                'Timestamp': fetch_timestamp
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


def last_month_year_month():
    """Get last month's year_month string in format YYYY_MM"""
    today = datetime.today()
    first = today.replace(day=1)
    last_month = first - timedelta(days=1)
    return last_month.strftime("%Y_%m")


def fetch_last_month_rates():
    """
    Fetches last month's exchange rates from IMF API and saves them as CSV.
    
    Returns:
        Path to the saved CSV file
    """
    ym = last_month_year_month()
    filename = f"exchange_rates_{ym}.csv"
    full_path = os.path.join(BASE_DIR, filename)

    # If file exists → idempotent (don't re-fetch)
    if os.path.exists(full_path):
        print(f"Exchange rate file already exists: {full_path}")
        return full_path

    os.makedirs(BASE_DIR, exist_ok=True)

    # Calculate date range for last month
    today = datetime.today()
    first = today.replace(day=1)
    last_month_date = first - timedelta(days=1)
    
    start_date = last_month_date.strftime("%Y-%m")
    end_date = last_month_date.strftime("%Y-%m")
    
    print(f"Fetching exchange rate data for {start_date}...")
    
    # Fetch data from IMF API
    xml_data = get_currency_data_from_imf(start_date, end_date)
    
    if not xml_data:
        raise Exception("Failed to fetch exchange rate data from IMF API")
    
    # Process XML to DataFrame
    df = process_xml_to_dataframe(xml_data)
    
    if df.empty:
        raise Exception("No exchange rate data found for the specified period")
    
    # Save to CSV
    df.to_csv(full_path, index=False, encoding='utf-8-sig', sep=',')
    print(f"Exchange rate data saved to: {full_path}")
    
    return full_path
