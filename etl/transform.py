import pandas as pd


def standardize_date_format(df, date_column):
    """Standardize the date format in the specified column."""
    df[date_column] = pd.to_datetime(df[date_column])
    return df


def standardize_cost_format(df, cost_column):
    """Standardize the cost format in the specified column."""
    df[cost_column] = df[cost_column].astype(float)
    return df


def standardize_clicks_format(df, clicks_column):
    """Standardize the clicks format in the specified column."""
    df[clicks_column] = df[clicks_column].astype(int)
    return df


def transform_data(data):
    """Apply transformations to all data files."""
    for file_name, df in data.items():
        if 'timestamp' in df.columns:
            df = standardize_date_format(df, 'timestamp')
        if 'cost' in df.columns:
            df = standardize_cost_format(df, 'cost')
        if 'clicks' in df.columns:
            df = standardize_clicks_format(df, 'clicks')
        data[file_name] = df
    return data


if __name__ == "__main__":
    from ingest import load_all_data
    raw_data = load_all_data()
    cleaned_data = transform_data(raw_data)
    for file_name, df in cleaned_data.items():
        print(f"Transformed {file_name} with {len(df)} records.")
