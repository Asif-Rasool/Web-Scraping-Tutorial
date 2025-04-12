import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import toml


def load_bls_api_key() -> str:
    """
    Load the BLS API key from the secrets TOML file.

    Returns:
        str: The API key.
    """
    config_path = Path(r"C:\Users\w0800598\Documents\Data-Science-Projects\Economics Dash\.streamlit\secrets.toml")
    config = toml.load(config_path)
    return config["BLS"]["api_key"]


def get_national_data(start_year: str, end_year: str) -> pd.DataFrame:
    """
    Fetch national-level data from the BLS API for a given time segment.

    The function returns a DataFrame with a 'date' column and one column per metric.

    Args:
        start_year (str): The starting year of the data.
        end_year (str): The ending year of the data.

    Returns:
        pd.DataFrame: DataFrame containing the national data.
    """
    api_key = load_bls_api_key()
    series_ids = {
        "labor_force": "LNS11000000",
        "employment": "LNS12000000",
        "unemployment": "LNS13000000",
        "unemployment_rate": "LNS14000000",
    }
    headers = {"Content-type": "application/json"}
    payload = {
        "seriesid": list(series_ids.values()),
        "startyear": start_year,
        "endyear": end_year,
        "registrationKey": api_key,
    }
    data = json.dumps(payload)
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    response = requests.post(url, data=data, headers=headers)
    json_data = response.json()
    if "Results" not in json_data:
        print("Error fetching national data:", json.dumps(json_data, indent=2))
        return pd.DataFrame()

    # Process each series and build a dictionary of DataFrames.
    dfs = {}
    for series in json_data["Results"]["series"]:
        sid = series["seriesID"]
        # Determine the metric from our mapping.
        metric = next((k for k, v in series_ids.items() if v == sid), None)
        if not metric:
            continue
        df = pd.DataFrame(series["data"])
        # Use only monthly data.
        df = df[df["period"].str.startswith("M")]
        df["year"] = df["year"].astype(str)
        df["month"] = df["period"].str[1:].str.zfill(2)
        df["date"] = pd.to_datetime(df["year"] + "-" + df["month"] + "-01")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df[["date", "value"]].rename(columns={"value": metric})
        dfs[metric] = df

    # Merge all national metrics on date.
    national_df = None
    for df in dfs.values():
        if national_df is None:
            national_df = df
        else:
            national_df = pd.merge(national_df, df, on="date", how="inner")
    if national_df is not None:
        national_df = national_df.sort_values("date")
    return national_df


def get_la_county_fips() -> dict:
    """
    Return a dictionary mapping Louisiana parish names to their county FIPS codes.
    
    Returns:
        dict: Parish names as keys and three-digit county FIPS codes as values.
    """
    return {
        "Acadia Parish": "001",
        "Allen Parish": "003",
        "Ascension Parish": "005",
        "Assumption Parish": "007",
        "Avoyelles Parish": "009",
        "Beauregard Parish": "011",
        "Bienville Parish": "013",
        "Bossier Parish": "015",
        "Caddo Parish": "017",
        "Calcasieu Parish": "019",
        "Caldwell Parish": "021",
        "Cameron Parish": "023",
        "Catahoula Parish": "025",
        "Claiborne Parish": "027",
        "Concordia Parish": "029",
        "De Soto Parish": "031",
        "East Baton Rouge Parish": "033",
        "East Carroll Parish": "035",
        "East Feliciana Parish": "037",
        "Evangeline Parish": "039",
        "Franklin Parish": "041",
        "Grant Parish": "043",
        "Iberia Parish": "045",
        "Iberville Parish": "047",
        "Jackson Parish": "049",
        "Jefferson Parish": "051",
        "Jefferson Davis Parish": "053",
        "Lafayette Parish": "055",
        "Lafourche Parish": "057",
        "La Salle Parish": "059",
        "Lincoln Parish": "061",
        "Livingston Parish": "063",
        "Madison Parish": "065",
        "Morehouse Parish": "067",
        "Natchitoches Parish": "069",
        "Orleans Parish": "071",
        "Ouachita Parish": "073",
        "Plaquemines Parish": "075",
        "Pointe Coupee Parish": "077",
        "Rapides Parish": "079",
        "Red River Parish": "081",
        "Richland Parish": "083",
        "Sabine Parish": "085",
        "St. Bernard Parish": "087",
        "St. Charles Parish": "089",
        "St. Helena Parish": "091",
        "St. James Parish": "093",
        "St. John the Baptist Parish": "095",
        "St. Landry Parish": "097",
        "St. Martin Parish": "099",
        "St. Mary Parish": "101",
        "St. Tammany Parish": "103",
        "Tangipahoa Parish": "105",
        "Tensas Parish": "107",
        "Terrebonne Parish": "109",
        "Union Parish": "111",
        "Vermilion Parish": "113",
        "Vernon Parish": "115",
        "Washington Parish": "117",
        "Webster Parish": "119",
        "West Baton Rouge Parish": "121",
        "West Carroll Parish": "123",
        "West Feliciana Parish": "125",
        "Winn Parish": "127",
    }


def build_all_county_series_ids() -> dict:
    """
    Build a dictionary mapping full series IDs for all four metrics to parish and metric info.
    
    The series format for LA counties is:
      "LAUCN" + state FIPS ("22") + county FIPS (3 digits) + "000000" + metric code (4 digits)
      
    Returns:
        dict: Mapping of series ID to a dict with parish and metric.
    """
    fips = get_la_county_fips()
    metrics = {
        "labor_force": "0006",
        "employment": "0005",
        "unemployment": "0004",
        "unemployment_rate": "0003",
    }
    series_map = {}
    for parish, county_fips in fips.items():
        for metric, code in metrics.items():
            series_id = f"LAUCN22{county_fips}000000{code}"
            series_map[series_id] = {"parish": parish, "metric": metric}
    return series_map


def fetch_all_county_data(api_key: str, series_map: dict, start_year: str, end_year: str) -> pd.DataFrame:
    """
    Fetch county-level data from the BLS API for a given time segment.
    
    Args:
        api_key (str): The BLS API key.
        series_map (dict): Mapping of series IDs to parish and metric info.
        start_year (str): The starting year.
        end_year (str): The ending year.
    
    Returns:
        pd.DataFrame: Raw county-level data.
    """
    all_data = []
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    headers = {"Content-type": "application/json"}
    series_ids = list(series_map.keys())

    # Process in batches (max 50 series per request).
    for i in range(0, len(series_ids), 50):
        batch_ids = series_ids[i:i + 50]
        payload = {
            "seriesid": batch_ids,
            "startyear": start_year,
            "endyear": end_year,
            "registrationKey": api_key,
        }
        data = json.dumps(payload)
        response = requests.post(url, data=data, headers=headers)
        result = response.json()
        if "Results" not in result:
            print(f"⚠️ Error in batch {i}: {json.dumps(result, indent=2)}")
            continue
        for series in result["Results"]["series"]:
            sid = series["seriesID"]
            meta = series_map[sid]
            parish = meta["parish"]
            metric = meta["metric"]
            for entry in series["data"]:
                if entry["period"].startswith("M") and entry["value"] != "-":
                    month = entry["period"][1:].zfill(2)
                    try:
                        date = pd.to_datetime(f"{entry['year']}-{month}-01")
                    except Exception as e:
                        print(f"Error parsing date for {sid}: {e}")
                        continue
                    try:
                        value = float(entry["value"])
                    except ValueError:
                        continue
                    all_data.append({
                        "parish": parish,
                        "date": date,
                        "metric": metric,
                        "value": value,
                    })
        time.sleep(1)  # Respect API rate limits
    return pd.DataFrame(all_data)


def reshape_county_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot the county-level data so that each row (unique parish and date)
    has columns for all four metrics.
    
    Args:
        df (pd.DataFrame): Raw county-level data.
    
    Returns:
        pd.DataFrame: Pivoted DataFrame.
    """
    if df.empty:
        return df
    pivot_df = df.pivot_table(index=["parish", "date"], columns="metric", values="value").reset_index()
    desired_order = ["parish", "date", "labor_force", "employment", "unemployment", "unemployment_rate"]
    pivot_df = pivot_df.reindex(columns=desired_order)
    return pivot_df.sort_values(["parish", "date"])


def merge_datasets(national_df: pd.DataFrame, county_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge the national and county datasets.
    
    National dataset columns are renamed with a "national_" prefix before merging on "date".
    
    Args:
        national_df (pd.DataFrame): DataFrame with national-level data.
        county_df (pd.DataFrame): DataFrame with county-level data.
    
    Returns:
        pd.DataFrame: Merged DataFrame.
    """
    # Rename national metric columns.
    national_df = national_df.rename(columns={
        "labor_force": "national_labor_force",
        "employment": "national_employment",
        "unemployment": "national_unemployment",
        "unemployment_rate": "national_unemployment_rate",
    })
    merged = pd.merge(county_df, national_df, on="date", how="left")
    return merged.sort_values(["parish", "date"])


def main() -> None:
    # Fetch and combine national-level data for two periods.
    print("Fetching national-level data (1990-2009)...")
    national_df_1 = get_national_data("1990", "2009")
    print("Fetching national-level data (2010-2025)...")
    national_df_2 = get_national_data("2010", "2025")
    national_df = pd.concat([national_df_1, national_df_2]).sort_values("date")
    print("National data range:", national_df["date"].min(), "to", national_df["date"].max())

    # Fetch county-level data for two periods.
    print("Fetching county-level data (1990-2009)...")
    api_key = load_bls_api_key()
    series_map = build_all_county_series_ids()
    raw_county_df_1 = fetch_all_county_data(api_key, series_map, "1990", "2009")
    print("Fetching county-level data (2010-2025)...")
    raw_county_df_2 = fetch_all_county_data(api_key, series_map, "2010", "2025")
    raw_county_df = pd.concat([raw_county_df_1, raw_county_df_2])
    county_df = reshape_county_data(raw_county_df)

    if not county_df.empty:
        print("County-level data sample:")
        print(county_df.head())
        print("County data range by parish:")
        print(county_df.groupby("parish")["date"].agg(["min", "max"]))
    else:
        print("No county-level data returned.")

    # Merge the national and county data.
    merged_df = merge_datasets(national_df, county_df)
    print("Merged dataset sample:")
    print(merged_df.head())

    # Save the final merged dataset to a CSV file.
    merged_df.to_csv("merged_national_county_1990_2025.csv", index=False)
    print("✅ Saved merged dataset to 'merged_national_county_1990_2025.csv'")


if __name__ == "__main__":
    main()
