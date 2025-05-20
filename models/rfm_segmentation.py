import pandas as pd


def calculate_rfm(df, customer_id_col='customer_id', date_col='timestamp', monetary_col='purchase_amount'):
    """Calculate RFM (Recency, Frequency, Monetary) scores for each customer."""
    # Recency
    df['Recency'] = (df[date_col].max() - df[date_col]).dt.days
    # Frequency
    frequency = df.groupby(customer_id_col).size()
    # Monetary
    monetary = df.groupby(customer_id_col)[monetary_col].sum()
    # Combine RFM
    rfm = pd.DataFrame({'Recency': df.groupby(customer_id_col)['Recency'].min(),
                        'Frequency': frequency,
                        'Monetary': monetary})
    return rfm


if __name__ == "__main__":
    from etl.load import save_all_data
    from etl.ingest import load_all_data
    from etl.transform import transform_data
    raw_data = load_all_data()
    cleaned_data = transform_data(raw_data)
    for file_name, df in cleaned_data.items():
        if 'customer_id' in df.columns and 'purchase_amount' in df.columns:
            rfm_scores = calculate_rfm(df)
            print(f"Calculated RFM scores for {file_name}.")
            print(rfm_scores.head())
