"""
Data Processing Module for Air Quality Data Pipeline

This module handles:
1. Fetching air quality data from OpenAQ API v3
2. Cleaning and preprocessing the data using Pandas
"""

import os
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
load_dotenv()


def get_api_key() -> str:
    """
    Retrieves the OpenAQ API key from multiple sources.
    
    Priority order:
    1. Streamlit secrets (for Streamlit Cloud deployment)
    2. Environment variables (for local development or other platforms)
    
    Returns:
        str: API key
    
    Raises:
        ValueError: If API key is not found in any source
    """
    api_key = None
    
    # Try Streamlit secrets first (for Streamlit Cloud)
    try:
        import streamlit as st
        if hasattr(st, 'secrets') and 'OPENAQ_API_KEY' in st.secrets:
            api_key = st.secrets['OPENAQ_API_KEY']
    except (ImportError, AttributeError):
        # Streamlit not available or secrets not configured
        pass
    
    # Fallback to environment variable (for local .env file or other platforms)
    if not api_key:
        api_key = os.getenv('OPENAQ_API_KEY')
    
    # Validate API key
    if not api_key or api_key == 'your_key_here' or api_key.strip() == '':
        error_msg = (
            "API key not found. Please set OPENAQ_API_KEY:\n"
            "- For Streamlit Cloud: Add it in Settings > Secrets\n"
            "- For local development: Add it to your .env file\n"
            "Get your API key from: https://openaq.org/"
        )
        raise ValueError(error_msg)
    
    return api_key.strip()


def fetch_aqi_data(city: str) -> pd.DataFrame:
    """
    Fetches air quality data from OpenAQ API v3 for a specified city.
    
    Uses the measurements endpoint directly, which is more reliable than
    the locations endpoint for city-based queries.
    
    Args:
        city (str): Name of the city to fetch data for (e.g., 'London', 'Los Angeles')
    
    Returns:
        pd.DataFrame: DataFrame containing air quality measurements with columns:
                     - datetime: Timestamp of measurement
                     - parameter: Type of measurement (PM2.5, NO2, etc.)
                     - value: Measurement value
                     - unit: Unit of measurement
                     - location: Location name
    """
    # Load API key from multiple sources (Streamlit secrets or environment variables)
    api_key = get_api_key()
    
    # Use the direct measurements endpoint (more reliable)
    return fetch_measurements_direct(city, api_key)


def fetch_measurements_direct(city: str, api_key: str) -> pd.DataFrame:
    """
    Fetches measurements directly from OpenAQ API v3 measurements endpoint.
    
    Uses a multi-step approach:
    1. First, try to get locations for the city
    2. Then fetch measurements for those specific locations
    3. Fallback to direct measurements query if locations approach fails
    
    Args:
        city (str): City name
        api_key (str): OpenAQ API key
    
    Returns:
        pd.DataFrame: DataFrame with air quality measurements
    """
    headers = {
        'X-API-Key': api_key,
        'Content-Type': 'application/json'
    }
    
    # Strategy 1: Get locations first, then measurements for those locations
    # This is more reliable because measurements endpoint often requires location_id
    try:
        locations = _get_locations_for_city(city, api_key, headers)
        if locations:
            return _get_measurements_for_locations(locations, api_key, headers)
    except Exception as e:
        # If location-based approach fails, try direct measurements
        pass
    
    # Strategy 2: Try direct measurements endpoint with various parameter combinations
    measurements_url = "https://api.openaq.org/v3/measurements"
    
    # Try different parameter combinations
    strategies = [
        # Try 1: Just limit (most basic)
        {'limit': 50},
        # Try 2: Limit with date_from (UTC format)
        {
            'limit': 50,
            'date_from': (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        },
        # Try 3: Limit with date_from and date_to
        {
            'limit': 50,
            'date_from': (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'date_to': pd.Timestamp.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        },
    ]
    
    for i, params in enumerate(strategies, 1):
        try:
            response = requests.get(measurements_url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                measurements = data.get('results', [])
                if measurements:
                    df = pd.DataFrame(measurements)
                    df_filtered = filter_by_city(df, city)
                    if not df_filtered.empty:
                        return df_filtered
                    # Return all if city filter finds nothing
                    return df
            
            elif response.status_code == 422:
                # Capture full error details for the last attempt
                if i == len(strategies):
                    error_data = response.json()
                    error_details = error_data.get('errors', [])
                    error_msg = f"API validation error (422): {error_data.get('message', 'Invalid parameters')}"
                    if error_details:
                        error_msg += f"\n\nDetailed Errors:\n{error_details}"
                    error_msg += f"\n\nAttempted parameters: {params}"
                    error_msg += f"\n\nFull response: {response.text[:500]}"
                    raise ValueError(error_msg)
                # Otherwise, try next strategy
                continue
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            if i == len(strategies):
                # Last attempt failed, raise with full details
                error_msg = f"Failed to fetch measurements after {len(strategies)} attempts: {str(e)}"
                if hasattr(e, 'response') and e.response is not None:
                    try:
                        error_data = e.response.json()
                        error_msg += f"\nAPI Response: {error_data}"
                    except:
                        error_msg += f"\nResponse text: {e.response.text[:300]}"
                raise ConnectionError(error_msg)
            continue
    
    # If all strategies failed
    return pd.DataFrame(columns=['datetime', 'parameter', 'value', 'unit', 'location'])


def _get_locations_for_city(city: str, api_key: str, headers: dict) -> list:
    """
    Helper function to get location IDs for a city.
    Uses locations endpoint with minimal parameters.
    """
    locations_url = "https://api.openaq.org/v3/locations"
    
    # Try with just limit first
    params = {'limit': 20}
    
    try:
        response = requests.get(locations_url, headers=headers, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            locations = data.get('results', [])
            # Filter locations by city name in the response
            city_locations = []
            for loc in locations:
                # Check various possible city fields
                city_name = None
                if isinstance(loc, dict):
                    city_name = loc.get('city') or loc.get('name') or str(loc)
                if city_name and city.lower() in str(city_name).lower():
                    city_locations.append(loc)
            return city_locations[:5]  # Limit to 5 locations
    except:
        pass
    
    return []


def _get_measurements_for_locations(locations: list, api_key: str, headers: dict) -> pd.DataFrame:
    """
    Helper function to get measurements for specific locations.
    """
    measurements_url = "https://api.openaq.org/v3/measurements"
    all_measurements = []
    
    date_from = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    for loc in locations:
        location_id = loc.get('id') if isinstance(loc, dict) else loc
        if not location_id:
            continue
            
        params = {
            'location_id': location_id,
            'limit': 100,
            'date_from': date_from
        }
        
        try:
            response = requests.get(measurements_url, headers=headers, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                measurements = data.get('results', [])
                all_measurements.extend(measurements)
        except:
            continue
    
    if all_measurements:
        return pd.DataFrame(all_measurements)
    
    return pd.DataFrame(columns=['datetime', 'parameter', 'value', 'unit', 'location'])


def filter_by_city(df: pd.DataFrame, city: str) -> pd.DataFrame:
    """
    Filters DataFrame by city name in location information.
    
    Handles different possible structures of location data in the API response.
    
    Args:
        df: DataFrame with measurements
        city: City name to filter by
    
    Returns:
        Filtered DataFrame
    """
    if df.empty:
        return df
    
    # Try different possible location field structures
    city_lower = city.lower()
    
    # Check if location is a nested dictionary
    if 'location' in df.columns and len(df) > 0:
        first_location = df['location'].iloc[0]
        if isinstance(first_location, dict):
            # Extract location name from nested dict
            if 'name' in first_location:
                city_filter = df['location'].apply(
                    lambda x: city_lower in str(x.get('name', '')).lower() if isinstance(x, dict) else False
                )
                return df[city_filter]
            elif 'city' in first_location:
                city_filter = df['location'].apply(
                    lambda x: city_lower in str(x.get('city', '')).lower() if isinstance(x, dict) else False
                )
                return df[city_filter]
    
    # Check for locationName column
    if 'locationName' in df.columns:
        city_filter = df['locationName'].astype(str).str.lower().str.contains(city_lower, na=False)
        return df[city_filter]
    
    # Check if location is a simple string column
    if 'location' in df.columns:
        city_filter = df['location'].astype(str).str.lower().str.contains(city_lower, na=False)
        return df[city_filter]
    
    # If no location filtering possible, return original DataFrame
    return df


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and preprocesses air quality data.
    
    Cleaning steps:
    1. Remove duplicate timestamps (keep first occurrence)
    2. Convert date columns to Python datetime objects
    3. Handle missing values using mean imputation
    
    Args:
        df (pd.DataFrame): Raw DataFrame from API
    
    Returns:
        pd.DataFrame: Cleaned DataFrame ready for visualization
    """
    if df.empty:
        return df
    
    # Create a copy to avoid modifying original DataFrame
    df_clean = df.copy()
    
    # Step 1: Remove duplicate timestamps
    # This ensures we have one measurement per timestamp, avoiding data redundancy
    if 'datetime' in df_clean.columns:
        # Convert to datetime first to properly identify duplicates
        df_clean['datetime'] = pd.to_datetime(df_clean['datetime'], errors='coerce')
        # Remove duplicates based on datetime and parameter (if available)
        if 'parameter' in df_clean.columns:
            df_clean = df_clean.drop_duplicates(
                subset=['datetime', 'parameter'], 
                keep='first'
            )
        else:
            df_clean = df_clean.drop_duplicates(subset=['datetime'], keep='first')
    
    # Step 2: Convert date columns to Python datetime objects
    # This enables time-based operations and filtering
    date_columns = ['datetime', 'date', 'date.utc', 'dateLocal']
    for col in date_columns:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
    
    # Step 3: Handle missing values using mean imputation
    # This preserves the dataset size while filling gaps in numeric columns
    numeric_columns = df_clean.select_dtypes(include=['float64', 'int64']).columns
    
    for col in numeric_columns:
        if df_clean[col].isna().any():
            # Calculate mean of the column (excluding NaN values)
            mean_value = df_clean[col].mean()
            # Fill missing values with the mean
            df_clean[col].fillna(mean_value, inplace=True)
    
    # Sort by datetime for better visualization
    if 'datetime' in df_clean.columns:
        df_clean = df_clean.sort_values('datetime', ascending=True)
    
    return df_clean

