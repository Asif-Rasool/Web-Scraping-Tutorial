
# Web Scraping Tutorial for BLS Data

Written by:
Asif Rasool, Ph.D.
Research Economist
Business Research Center
Southeastern Louisiana University
1514 Martens Drive, Hammond, LA 70401
Phone: 985-549-3831
Email: asif.rasool@southeastern.edu

This Python tutorial demonstrates how to scrape and analyze economic data from the U.S. Bureau of Labor Statistics (BLS) API. The script fetches national and county-level employment statistics such as labor force, unemployment, and employment rates for analysis and visualization.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Code Explanation](#code-explanation)
  - [BLS API Key](#bls-api-key)
  - [National Data Fetching](#national-data-fetching)
  - [County Data Fetching](#county-data-fetching)
  - [Data Merging](#data-merging)
- [License](#license)

## Overview

This project is designed as a web scraping tutorial to help you learn how to collect and process economic data from the U.S. Bureau of Labor Statistics (BLS). The script uses Python to interact with the BLS API, fetch various economic indicators, and process this data into an easy-to-use format.

## Prerequisites

Before running this script, you will need to have the following installed:

- Python 3.x
- pandas
- requests
- toml

You can install the required Python packages using the following command:

```bash
pip install pandas requests toml
```

## Setup Instructions

1. **Clone this repository:**

   ```bash
   git clone https://github.com/yourusername/your-repo-name.git
   cd your-repo-name
   ```

2. **Obtain a BLS API Key:**

   To access the BLS API, you'll need a valid API key. You can obtain one by registering at [BLS API Registration](https://www.bls.gov/developers/).

3. **Store the API Key:**

   After receiving your API key, save it in a `.toml` file. The script looks for the key in the following format:
   
   ```toml
   [BLS]
   api_key = "your_api_key_here"
   ```

   Place this `.toml` file in the correct directory where your script can read it. Make sure the path in the script matches where your `.toml` file is located.

## Code Explanation

### BLS API Key

The script uses a method to load the BLS API key securely from a `.toml` file. The `load_bls_api_key()` function fetches the API key that is used to authenticate the requests made to the BLS API.

```python
def load_bls_api_key() -> str:
    config_path = Path(r"C:\Users\yourpath\secrets.toml")
    config = toml.load(config_path)
    return config["BLS"]["api_key"]
```

### National Data Fetching

The `get_national_data()` function fetches national-level data for labor force, employment, unemployment, and unemployment rates over a specified date range. The data is returned in a structured pandas DataFrame for easy analysis.

```python
def get_national_data(start_year: str, end_year: str) -> pd.DataFrame:
    # Calls the BLS API to fetch national data
    ...
```

### County Data Fetching

The script also fetches county-level data, particularly for Louisiana parishes. The `fetch_all_county_data()` function pulls data for each parish over a given date range. It handles pagination and rate limits by making requests in batches.

```python
def fetch_all_county_data(api_key: str, series_map: dict, start_year: str, end_year: str) -> pd.DataFrame:
    # Fetches county-level data for each parish
    ...
```

### Data Merging

Once national and county-level data are fetched, the `merge_datasets()` function merges the two datasets on the "date" column, creating a single DataFrame that contains both national and county-level metrics for comparison.

```python
def merge_datasets(national_df: pd.DataFrame, county_df: pd.DataFrame) -> pd.DataFrame:
    # Merges national and county-level data
    ...
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
