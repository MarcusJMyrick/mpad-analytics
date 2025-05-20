# etl/transform.py

from pathlib import Path
import pandas as pd
import logging
from typing import Dict

# set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# Define which numeric columns we expect across our files
NUMERIC_COLS = {
    "cost": float,
    "clicks": int,
    "impressions": int,
    "opens": int,
    "amount": float,
    "page_views": int,
    "session_duration": int
}

def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert any column whose name contains 'date' or 'Date' to datetime.
    """
    date_cols = [c for c in df.columns if "date" in c.lower()]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="raise")
        logger.info(f"Parsed dates in column: {col}")
    return df

def cast_numerics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cast any known numeric columns to their proper dtype.
    """
    for col, dtype in NUMERIC_COLS.items():
        if col in df.columns:
            df[col] = df[col].astype(dtype)
            logger.info(f"Casted column '{col}' to {dtype.__name__}")
    return df

def transform_dataset(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply a standard suite of transforms to a single dataset.
    
    Args:
        name: key or filename (e.g. 'facebook_ads')
        df: raw DataFrame
    Returns:
        transformed DataFrame
    """
    logger.info(f"--- Transforming {name} (shape={df.shape}) ---")
    df = parse_dates(df)
    df = cast_numerics(df)
    # any dataset-specific logic can go here:
    # e.g. rename columns, drop duplicates, fill NAs
    if name.startswith("email_campaigns"):
        df = df.rename(columns={"send_date": "date"})
    return df

def transform_all(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Transform every DataFrame in the provided dict.
    
    Args:
        data: dict of name -> raw DataFrame
    Returns:
        dict of name -> transformed DataFrame
    """
    transformed: Dict[str, pd.DataFrame] = {}
    for name, df in data.items():
        try:
            transformed[name] = transform_dataset(name, df.copy())
        except Exception as e:
            logger.exception(f"Failed to transform {name}: {e}")
            raise
    return transformed

if __name__ == "__main__":
    from etl.ingest import load_all_data
    raw = load_all_data()
    clean = transform_all(raw)
    for name, df in clean.items():
        logger.info(f"{name}: transformed shape {df.shape}")
