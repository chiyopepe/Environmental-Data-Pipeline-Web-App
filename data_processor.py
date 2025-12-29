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
from typing import Optional

# Load environment variables from .env file
load_dotenv()


def fetch_aqi_data(city: str) -> pd.DataFrame:
    """
    Fetches air quality data from OpenAQ API v3 for a specified city.
    
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
    
    # OpenAQ API v3 endpoint for latest measurements
    base_url = "https://api.openaq.org/v3/locations"
    
    # Prepare headers with API key authentication
    headers = {
        'X-API-Key': api_key,
        'Content-Type': 'application/json'
    }
    
    # Parameters for API request
    params = {
        'limit': 100,  # Maximum number of results
        'page': 1,
        'order_by': 'lastUpdated',
        'sort': 'desc'
    }
    
    # Search for locations matching the city name
    params['city'] = city
    
    try:
        # Make API request
        response = requests.get(base_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        
        # Extract locations
        locations = data.get('results', [])
        
        if not locations:
            # If no locations found, try fetching measurements directly
            return fetch_measurements_direct(city, api_key)
        
        # Get location IDs and fetch measurements for each
        location_ids = [loc['id'] for loc in locations[:5]]  # Limit to first 5 locations
        
        # Fetch measurements for these locations
        measurements_url = "https://api.openaq.org/v3/measurements"
        all_measurements = []
        
        for loc_id in location_ids:
            # Format date_from as ISO string for API
            date_from = (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
            meas_params = {
                'location_id': loc_id,
                'limit': 100,
                'date_from': date_from,  # Last 24 hours
                'parameter': 'pm25,no2',  # Focus on PM2.5 and NO2
                'order_by': 'datetime',
                'sort': 'desc'
            }
            
            meas_response = requests.get(
                measurements_url, 
                headers=headers, 
                params=meas_params, 
                timeout=10
            )
            
            if meas_response.status_code == 200:
                meas_data = meas_response.json()
                all_measurements.extend(meas_data.get('results', []))
        
        # Convert to DataFrame
        if all_measurements:
            df = pd.DataFrame(all_measurements)
            return df
        else:
            # Fallback: fetch measurements directly
            return fetch_measurements_direct(city, api_key)
            
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to fetch data from OpenAQ API: {str(e)}")


def fetch_measurements_direct(city: str, api_key: str) -> pd.DataFrame:
    """
    Alternative method to fetch measurements directly by city name.
    
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
    
    # Parameters for measurements endpoint
    # OpenAQ API v3 supports city parameter directly in measurements endpoint
    params = {
        'city': city,
        'limit': 100,
        'date_from': date_from,
        'parameter': 'pm25,no2',  # Focus on PM2.5 and NO2 as specified
        'order_by': 'datetime',
        'sort': 'desc'
    }
    
    try:
        response = requests.get(measurements_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        measurements = data.get('results', [])
        
        if measurements:
            df = pd.DataFrame(measurements)
            return df
        else:
            # Return empty DataFrame with expected structure
            return pd.DataFrame(columns=['datetime', 'parameter', 'value', 'unit', 'location'])
            
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to fetch measurements: {str(e)}")


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

