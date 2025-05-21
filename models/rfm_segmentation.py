# models/rfm_segmentation.py

import pandas as pd
from datetime import datetime
from typing import Optional, Union

def calculate_rfm(
    df: pd.DataFrame,
    snapshot_date: Optional[Union[str, datetime]] = None,
    customer_id_col='customer_id',
    date_col='purchase_date',
    monetary_col='purchase_amount'
) -> pd.DataFrame:
    """
    Compute basic RFM metrics:
      - Recency (days since last purchase)
      - Frequency (# of purchases)
      - Monetary (sum of purchase_amount)

    Args:
        df: DataFrame with columns:
            - 'customer_id'
            - either 'timestamp' or 'purchase_date'
            - 'purchase_amount'
        snapshot_date: Optional date to calculate recency against.
                       If None, uses max(date_col) + 1 day.

    Returns:
        DataFrame with columns ['customer_id', 'Recency', 'Frequency', 'Monetary']
    """
    data = df.copy()

    # pick date column
    if "timestamp" in data.columns:
        date_col = "timestamp"
    elif "purchase_date" in data.columns:
        date_col = "purchase_date"
    else:
        raise KeyError("No date column found; expected 'timestamp' or 'purchase_date'")

    # parse dates
    data[date_col] = pd.to_datetime(data[date_col])

    # determine snapshot
    if snapshot_date is None:
        snapshot = data[date_col].max() + pd.Timedelta(days=1)
    else:
        snapshot = pd.to_datetime(snapshot_date)

    # group and compute RFM
    rfm = (
        data
        .groupby("customer_id")
        .agg(
            Recency   = (date_col, lambda x: (snapshot - x.max()).days),
            Frequency = (date_col, "count"),
            Monetary  = (monetary_col, "sum"),
        )
        .reset_index()
    )
    return rfm
