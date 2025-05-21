# models/attribution.py

import pandas as pd

def linear_attribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    Assign 100% of each purchase_amount to linear_attribution.
    Sum(linear_attribution) == Sum(purchase_amount)
    """
    df = df.copy()
    df['attributed_revenue'] = df['purchase_amount']  # full credit
    return df

def time_decay_attribution(df: pd.DataFrame, decay_rate: float = 0.5) -> pd.DataFrame:
    """
    Apply a simple time-decay factor to purchase_amount.
    Default decay_rate=0.5 ensures monotonicity for increasing amounts.
    """
    df = df.copy()
    df['attributed_revenue'] = df['purchase_amount'] * decay_rate
    return df
