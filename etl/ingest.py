import pandas as pd
import os

RAW_DATA_PATH = 'data/raw/'


def load_csv(file_name):
    """Load a CSV file into a Pandas DataFrame."""
    file_path = os.path.join(RAW_DATA_PATH, file_name)
    return pd.read_csv(file_path)


def load_all_data():
    """Load all raw data files into a dictionary of DataFrames."""
    data_files = ['facebook_ads.csv', 'google_ads.csv', 'email_campaigns.csv', 'customer_transactions.csv', 'website_visits.csv']
    data = {}
    for file in data_files:
        data[file] = load_csv(file)
    return data


if __name__ == "__main__":
    data = load_all_data()
    for file_name, df in data.items():
        print(f"Loaded {file_name} with {len(df)} records.")
