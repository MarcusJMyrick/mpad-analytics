import os

CLEANED_DATA_PATH = 'data/cleaned/'


def save_csv(df, file_name):
    """Save a DataFrame to a CSV file."""
    file_path = os.path.join(CLEANED_DATA_PATH, file_name)
    df.to_csv(file_path, index=False)


def save_all_data(data):
    """Save all processed data files to the cleaned directory."""
    for file_name, df in data.items():
        save_csv(df, file_name)


if __name__ == "__main__":
    from transform import transform_data
    from ingest import load_all_data
    raw_data = load_all_data()
    cleaned_data = transform_data(raw_data)
    save_all_data(cleaned_data)
    print("All data has been saved to the cleaned directory.")
