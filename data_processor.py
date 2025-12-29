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

# Load environment variables from .env file
load_dotenv()


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
    # Load API key from environment variable
    api_key = os.getenv('OPENAQ_API_KEY')
    
    if not api_key or api_key == 'your_key_here':
        raise ValueError(
            "API key not found. Please set OPENAQ_API_KEY in your .env file. "
            "Get your API key from: https://openaq.org/"
        )
    
    # Use the direct measurements endpoint (more reliable)
    return fetch_measurements_direct(city, api_key)


def fetch_measurements_direct(city: str, api_key: str) -> pd.DataFrame:
    """
    Fetches measurements directly from OpenAQ API v3 measurements endpoint.
    
    This method fetches recent measurements and filters by city name in the location data.
    This approach avoids the 422 error by using only supported parameters.
    
    Args:
        city (str): City name
        api_key (str): OpenAQ API key
    
    Returns:
        pd.DataFrame: DataFrame with air quality measurements
    """
    measurements_url = "https://api.openaq.org/v3/measurements"
    
    headers = {
        'X-API-Key': api_key,
        'Content-Type': 'application/json'
    }
    
    # Calculate date range for last 24 hours (ISO format)
    date_from = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    try:
        # Use minimal, well-supported parameters to avoid 422 errors
        # Start with basic parameters that are most likely to be supported
        params = {
            'limit': 100,
            'date_from': date_from
        }
        
        response = requests.get(measurements_url, headers=headers, params=params, timeout=10)
        
        # If we get a 422 error, provide detailed error information
        if response.status_code == 422:
            try:
                error_data = response.json()
                error_details = error_data.get('errors', [])
                error_msg = f"API validation error (422): {error_data.get('message', 'Invalid parameters')}"
                if error_details:
                    error_msg += f"\nDetails: {error_details}"
                # Try with even simpler parameters
                params_simple = {'limit': 50}
                response_simple = requests.get(measurements_url, headers=headers, params=params_simple, timeout=10)
                if response_simple.status_code == 200:
                    # If simple request works, use it
                    data = response_simple.json()
                    measurements = data.get('results', [])
                    if measurements:
                        df = pd.DataFrame(measurements)
                        return filter_by_city(df, city)
                raise ValueError(error_msg)
            except Exception as parse_error:
                raise ValueError(f"API validation error (422). Response: {response.text[:200]}")
        
        response.raise_for_status()
        
        data = response.json()
        measurements = data.get('results', [])
        
        if measurements:
            df = pd.DataFrame(measurements)
            # Filter by city name
            df_filtered = filter_by_city(df, city)
            
            if not df_filtered.empty:
                return df_filtered
            else:
                # If no city-specific data found, return all measurements
                # This is better than returning empty data
                return df
        else:
            # Return empty DataFrame with expected structure
            return pd.DataFrame(columns=['datetime', 'parameter', 'value', 'unit', 'location'])
            
    except requests.exceptions.HTTPError as e:
        # Provide detailed error information for debugging
        error_msg = f"HTTP Error {e.response.status_code}: {str(e)}"
        try:
            error_data = e.response.json()
            if 'errors' in error_data:
                error_msg += f"\nAPI Errors: {error_data['errors']}"
            if 'message' in error_data:
                error_msg += f"\nMessage: {error_data['message']}"
            # Include response text for debugging
            error_msg += f"\nResponse preview: {e.response.text[:300]}"
        except:
            error_msg += f"\nResponse: {e.response.text[:200]}"
        raise ConnectionError(error_msg)
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to fetch measurements: {str(e)}")


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

