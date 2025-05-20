# etl/load.py

from pathlib import Path
import pandas as pd
import logging
from typing import Dict

# configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

CLEANED_DATA_DIR = Path("data/cleaned")


def ensure_clean_dir():
    """Create the cleaned data directory if it doesn't exist."""
    if not CLEANED_DATA_DIR.exists():
        CLEANED_DATA_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {CLEANED_DATA_DIR}")


def save_csv(df: pd.DataFrame, file_name: str) -> None:
    """
    Save a DataFrame to a CSV in the cleaned directory.

    Args:
        df: DataFrame to save.
        file_name: Base name (with or without .csv) for the output file.
    """
    ensure_clean_dir()
    # Ensure .csv extension
    path = CLEANED_DATA_DIR / (file_name if file_name.endswith(".csv") else f"{file_name}.csv")
    try:
        df.to_csv(path, index=False)
        logger.info(f"Saved {path.name} ({len(df):,} rows)")
    except Exception as e:
        logger.exception(f"Failed to save {path.name}: {e}")
        raise


def save_all_data(data: Dict[str, pd.DataFrame]) -> None:
    """
    Save every DataFrame in `data` to the cleaned directory.

    Args:
        data: Dict mapping base filename (or key) to DataFrame.
    """
    for key, df in data.items():
        save_csv(df, key)


if __name__ == "__main__":
    from etl.ingest import load_all_data
    from etl.transform import transform_all

    raw = load_all_data()
    clean = transform_all(raw)
    save_all_data(clean)
    logger.info("All cleaned data files saved successfully.")
