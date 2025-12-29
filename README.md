# 🌍 Air Quality Monitoring Web Application

A professional Python-based web application that fetches real-time Air Quality data (PM2.5, NO₂) from the OpenAQ API, cleans the data using Pandas, and visualizes it using Streamlit. This project demonstrates secure API key management and professional data engineering practices.

## 🎯 Project Overview

This application provides a user-friendly interface to monitor air quality metrics across different cities worldwide. It showcases:

- **Data Engineering**: API integration, data cleaning, and preprocessing
- **Security**: Environment variable management for API keys
- **Data Visualization**: Interactive charts and real-time metrics
- **Best Practices**: Caching, error handling, and clean code structure

## 🛠️ Tech Stack

- **Language**: Python 3.x
- **Data Handling**: Pandas, NumPy
- **API Requests**: requests
- **Web Framework**: Streamlit
- **Visualization**: Plotly
- **Environment Management**: python-dotenv

## 📁 Project Structure

```
air-quality-app/
├── .env                # Private: Store API keys here (DO NOT COMMIT)
├── .gitignore          # Tells Git to ignore .env and __pycache__
├── requirements.txt    # List of libraries to install
├── app.py              # Main Streamlit application
├── data_processor.py   # Logic for API calls and cleaning
└── README.md           # Documentation for recruiters
```

## 🚀 Installation & Setup

### Prerequisites

- Python 3.8 or higher
- OpenAQ API key ([Get one here](https://openaq.org/))

### Step 1: Clone or Download the Project

```bash
cd "Air Quality Fetcher"
```

### Step 2: Create Virtual Environment (Recommended)

```bash
python -m venv venv

# On Windows
venv\Scripts\activate

# On macOS/Linux
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure API Key

1. Create a `.env` file in the project root directory
2. Add your OpenAQ API key:

```env
OPENAQ_API_KEY=your_actual_api_key_here
```

**⚠️ Important**: Never commit the `.env` file to version control. It's already included in `.gitignore`.

### Step 5: Run the Application

```bash
streamlit run app.py
```

The application will open in your default web browser at `http://localhost:8501`

## 📊 Features

### 1. City Selection
- Pre-configured list of popular cities (London, Los Angeles, Paris, etc.)
- Custom city input option
- Easy-to-use sidebar interface

### 2. Data Fetching
- Real-time air quality data from OpenAQ API v3
- Uses `X-API-Key` header for authentication
- Cached API calls (`st.cache_data`) to prevent rate limiting
- Automatic data refresh capability

### 3. Data Cleaning Pipeline

The `clean_data()` function performs three key cleaning steps:

#### Step 1: Remove Duplicate Timestamps
- Identifies and removes duplicate entries based on datetime and parameter
- Keeps the first occurrence of each duplicate
- **Why**: Prevents data redundancy and ensures accurate measurements

#### Step 2: Convert Date Columns to Datetime Objects
- Converts string dates to Python datetime objects
- Handles multiple date column formats (`datetime`, `date`, `date.utc`, `dateLocal`)
- **Why**: Enables time-based operations, filtering, and proper visualization

#### Step 3: Handle Missing Values (Mean Imputation)
- Identifies numeric columns with missing values
- Fills NaN values with the column mean
- **Why**: Preserves dataset size while filling gaps, maintaining statistical properties

### 4. Visualization
- Interactive line charts showing pollution trends over 24 hours
- Multi-parameter visualization (PM2.5, NO₂, etc.)
- Real-time metrics display
- Hover tooltips for detailed information

### 5. Data Export
- View raw data in expandable section
- Download data as CSV file
- Timestamped filenames for organization

## 🔧 Code Architecture

### `data_processor.py`

**`fetch_aqi_data(city: str) -> pd.DataFrame`**
- Loads API key from environment variables
- Makes authenticated requests to OpenAQ API v3
- Handles errors and edge cases
- Returns raw DataFrame with air quality measurements

**`clean_data(df: pd.DataFrame) -> pd.DataFrame`**
- Implements data cleaning pipeline
- Well-commented code explaining each cleaning step
- Returns cleaned DataFrame ready for visualization

### `app.py`

- Streamlit-based user interface
- Sidebar for city selection
- Main content area for metrics and visualizations
- Error handling and user feedback
- Caching mechanism for API calls

## 🔒 Security Best Practices

1. **API Key Management**: Uses `python-dotenv` to load keys from `.env` file
2. **Git Ignore**: `.env` file is excluded from version control
3. **Header Authentication**: API key sent via `X-API-Key` header (not URL parameters)
4. **Error Handling**: Graceful error messages without exposing sensitive information

## 📈 Data Engineering Highlights

This project demonstrates several data engineering best practices:

1. **ETL Pipeline**: Extract (API), Transform (Clean), Load (Visualize)
2. **Data Quality**: Duplicate detection and removal
3. **Data Imputation**: Handling missing values intelligently
4. **Caching**: Reducing API calls and improving performance
5. **Error Handling**: Robust exception handling for production readiness
6. **Documentation**: Clear comments explaining data transformations

## 🎓 Interview Talking Points

When discussing this project in interviews, highlight:

1. **API Integration**: Understanding of REST APIs, authentication, and error handling
2. **Data Cleaning**: Knowledge of common data quality issues and solutions
3. **Pandas Proficiency**: DataFrame operations, datetime handling, data imputation
4. **Security Awareness**: Environment variables, API key management
5. **Performance Optimization**: Caching strategies to reduce API load
6. **User Experience**: Intuitive UI design and informative error messages

## 🐛 Troubleshooting

### API Key Issues
- Ensure `.env` file exists in the project root
- Verify `OPENAQ_API_KEY` is set correctly (no quotes needed)
- Check that your API key is valid at [OpenAQ.org](https://openaq.org/)

### No Data Returned
- Try a different city name
- Check your internet connection
- Verify API key has proper permissions

### Import Errors
- Ensure all dependencies are installed: `pip install -r requirements.txt`
- Activate your virtual environment if using one

## 📝 License

This project is open source and available for educational purposes.

## 🙏 Acknowledgments

- [OpenAQ](https://openaq.org/) for providing the air quality API
- Streamlit team for the excellent web framework
- Pandas community for powerful data manipulation tools

## 📧 Contact

For questions or feedback about this project, please open an issue or contact the repository maintainer.

---

**Built with ❤️ using Streamlit, Pandas, and OpenAQ API**

