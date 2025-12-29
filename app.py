"""
Air Quality Monitoring Web Application

A Streamlit-based web app that fetches and visualizes real-time air quality data
from the OpenAQ API, demonstrating data engineering best practices.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from data_processor import fetch_aqi_data, clean_data
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Air Quality Monitor",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    </style>
""", unsafe_allow_html=True)

# Title and header
st.markdown('<h1 class="main-header">🌍 Air Quality Monitor</h1>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar for city selection
st.sidebar.header("📍 City Selection")

# List of popular cities for air quality monitoring
cities = [
    "London",
    "Los Angeles",
    "Paris",
    "New York",
    "Tokyo",
    "Delhi",
    "Beijing",
    "Mumbai",
    "Berlin",
    "Madrid"
]

# City selector
selected_city = st.sidebar.selectbox(
    "Select a city:",
    cities,
    index=0
)

# Custom city input option
custom_city = st.sidebar.text_input("Or enter a custom city name:")

# Use custom city if provided, otherwise use selected city
city_to_fetch = custom_city.strip() if custom_city.strip() else selected_city

# Fetch Data button
fetch_button = st.sidebar.button("🔍 Fetch Data", type="primary", use_container_width=True)

# Sidebar information
st.sidebar.markdown("---")
st.sidebar.info(
    "This app fetches real-time air quality data from the OpenAQ API. "
    "Data includes PM2.5 and NO2 measurements from the last 24 hours."
)

# Main content area
if fetch_button or 'data_fetched' in st.session_state:
    try:
        # Show loading spinner
        with st.spinner(f"Fetching air quality data for {city_to_fetch}..."):
            # Use st.cache_data to cache API calls and avoid spamming the API
            @st.cache_data(ttl=300)  # Cache for 5 minutes
            def get_cached_data(city):
                """Cached function to fetch and clean data"""
                raw_data = fetch_aqi_data(city)
                cleaned_data = clean_data(raw_data)
                return cleaned_data
            
            # Fetch and clean data
            df = get_cached_data(city_to_fetch)
            st.session_state['data_fetched'] = True
            st.session_state['current_city'] = city_to_fetch
            st.session_state['current_data'] = df
        
        # Check if data was successfully fetched
        if df.empty:
            st.warning(
                f"⚠️ No air quality data found for {city_to_fetch}. "
                "Please try a different city or check your API key."
            )
        else:
            # Display success message
            st.success(f"✅ Successfully fetched data for {city_to_fetch}!")
            
            # Display data summary
            st.subheader("📊 Data Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Measurements", len(df))
            
            with col2:
                if 'parameter' in df.columns:
                    unique_params = df['parameter'].nunique()
                    st.metric("Parameters", unique_params)
                else:
                    st.metric("Parameters", "N/A")
            
            with col3:
                if 'datetime' in df.columns:
                    latest_time = df['datetime'].max()
                    if pd.notna(latest_time):
                        st.metric("Latest Update", latest_time.strftime("%H:%M"))
                    else:
                        st.metric("Latest Update", "N/A")
                else:
                    st.metric("Latest Update", "N/A")
            
            with col4:
                if 'location' in df.columns:
                    unique_locations = df['location'].nunique()
                    st.metric("Locations", unique_locations)
                else:
                    st.metric("Locations", "N/A")
            
            st.markdown("---")
            
            # High-level metrics section
            st.subheader("📈 Current Air Quality Metrics")
            
            # Filter for PM2.5 and NO2 if available
            if 'parameter' in df.columns and 'value' in df.columns:
                # Get latest values for each parameter
                metrics_col1, metrics_col2, metrics_col3, metrics_col4 = st.columns(4)
                
                # PM2.5 metric
                pm25_data = df[df['parameter'] == 'pm25'] if 'pm25' in df['parameter'].values else None
                if pm25_data is not None and not pm25_data.empty:
                    latest_pm25 = pm25_data['value'].iloc[-1] if 'value' in pm25_data.columns else None
                    if latest_pm25 is not None:
                        with metrics_col1:
                            st.metric(
                                "PM2.5 (μg/m³)",
                                f"{latest_pm25:.2f}",
                                help="Particulate matter 2.5 micrometers or smaller"
                            )
                
                # NO2 metric
                no2_data = df[df['parameter'] == 'no2'] if 'no2' in df['parameter'].values else None
                if no2_data is not None and not no2_data.empty:
                    latest_no2 = no2_data['value'].iloc[-1] if 'value' in no2_data.columns else None
                    if latest_no2 is not None:
                        with metrics_col2:
                            st.metric(
                                "NO₂ (μg/m³)",
                                f"{latest_no2:.2f}",
                                help="Nitrogen dioxide"
                            )
                
                # Display other available parameters
                available_params = df['parameter'].unique()[:2]
                param_idx = 0
                for param in available_params:
                    if param not in ['pm25', 'no2']:
                        param_data = df[df['parameter'] == param]
                        if not param_data.empty and 'value' in param_data.columns:
                            latest_value = param_data['value'].iloc[-1]
                            if param_idx == 0:
                                with metrics_col3:
                                    st.metric(
                                        f"{param.upper()}",
                                        f"{latest_value:.2f}"
                                    )
                                param_idx += 1
                            elif param_idx == 1:
                                with metrics_col4:
                                    st.metric(
                                        f"{param.upper()}",
                                        f"{latest_value:.2f}"
                                    )
                                param_idx += 1
            else:
                st.info("Parameter information not available in the dataset.")
            
            st.markdown("---")
            
            # Visualization section
            st.subheader("📉 Air Quality Trends (Last 24 Hours)")
            
            # Create line chart
            if 'datetime' in df.columns and 'value' in df.columns:
                # Prepare data for visualization
                viz_df = df.copy()
                
                # Filter to last 24 hours if datetime is available
                if 'datetime' in viz_df.columns:
                    viz_df['datetime'] = pd.to_datetime(viz_df['datetime'])
                    cutoff_time = pd.Timestamp.now() - pd.Timedelta(hours=24)
                    viz_df = viz_df[viz_df['datetime'] >= cutoff_time]
                
                # Create visualization
                if not viz_df.empty and 'parameter' in viz_df.columns:
                    # Group by parameter for multi-line chart
                    fig = px.line(
                        viz_df,
                        x='datetime',
                        y='value',
                        color='parameter',
                        title=f'Air Quality Trends in {city_to_fetch}',
                        labels={
                            'datetime': 'Time',
                            'value': 'Concentration (μg/m³)',
                            'parameter': 'Parameter'
                        },
                        markers=True
                    )
                    
                    fig.update_layout(
                        hovermode='x unified',
                        height=500,
                        xaxis_title="Time",
                        yaxis_title="Concentration (μg/m³)",
                        legend_title="Parameter"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                elif not viz_df.empty:
                    # Single parameter visualization
                    fig = px.line(
                        viz_df,
                        x='datetime',
                        y='value',
                        title=f'Air Quality Trends in {city_to_fetch}',
                        labels={
                            'datetime': 'Time',
                            'value': 'Concentration (μg/m³)'
                        },
                        markers=True
                    )
                    
                    fig.update_layout(
                        hovermode='x unified',
                        height=500,
                        xaxis_title="Time",
                        yaxis_title="Concentration (μg/m³)"
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No data available for the last 24 hours.")
            else:
                st.warning("Cannot create visualization: missing datetime or value columns.")
            
            st.markdown("---")
            
            # Raw data section (collapsible)
            with st.expander("🔍 View Raw Data"):
                st.dataframe(df, use_container_width=True)
                
                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Data as CSV",
                    data=csv,
                    file_name=f"air_quality_{city_to_fetch}_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv"
                )
    
    except ValueError as e:
        st.error(f"❌ Configuration Error: {str(e)}")
        st.info("💡 Please check your .env file and ensure OPENAQ_API_KEY is set correctly.")
    
    except ConnectionError as e:
        st.error(f"❌ Connection Error: {str(e)}")
        st.info("💡 Please check your internet connection and API key validity.")
    
    except Exception as e:
        st.error(f"❌ Unexpected Error: {str(e)}")
        st.exception(e)

else:
    # Welcome message when app first loads
    st.info("👈 Please select a city from the sidebar and click 'Fetch Data' to begin.")
    
    # Display instructions
    st.markdown("""
    ### 🚀 Getting Started
    
    1. **Set up your API key**: 
       - Create a `.env` file in the project root
       - Add your OpenAQ API key: `OPENAQ_API_KEY=your_key_here`
       - Get your API key from [OpenAQ.org](https://openaq.org/)
    
    2. **Select a city**: Use the sidebar to choose from popular cities or enter a custom city name
    
    3. **Fetch data**: Click the "Fetch Data" button to retrieve air quality measurements
    
    4. **Explore**: View metrics, trends, and download the data for analysis
    
    ### 📋 Features
    
    - ✅ Real-time air quality data from OpenAQ API
    - ✅ Data cleaning and preprocessing (duplicate removal, datetime conversion, missing value imputation)
    - ✅ Interactive visualizations with Plotly
    - ✅ Cached API calls to prevent rate limiting
    - ✅ Secure API key management with environment variables
    """)

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666;'>"
    "Built with ❤️ using Streamlit, Pandas, and OpenAQ API"
    "</div>",
    unsafe_allow_html=True
)

