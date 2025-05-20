import pandas as pd


def linear_attribution(df, conversion_column='purchase_amount'):
    """Apply linear attribution model to the data."""
    df['linear_attribution'] = df[conversion_column] / df[conversion_column].count()
    return df


def time_decay_attribution(df, conversion_column='purchase_amount', decay_factor=0.5):
    """Apply time-decay attribution model to the data."""
    df = df.sort_values(by='timestamp')
    df['time_decay_attribution'] = df[conversion_column].expanding().apply(lambda x: (x * (decay_factor ** (len(x) - x.index))).sum())
    return df


if __name__ == "__main__":
    from etl.load import save_all_data
    from etl.ingest import load_all_data
    from etl.transform import transform_data
    raw_data = load_all_data()
    cleaned_data = transform_data(raw_data)
    for file_name, df in cleaned_data.items():
        if 'purchase_amount' in df.columns:
            df = linear_attribution(df)
            df = time_decay_attribution(df)
            print(f"Applied attribution models to {file_name}.")
    save_all_data(cleaned_data)
