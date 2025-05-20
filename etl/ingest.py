# etl/ingest.py

from pathlib import Path
import pandas as pd
import logging
from typing import Dict

# configure module-level logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("data/raw")


def load_csv(file_path: Path) -> pd.DataFrame:
    """
    Load a single CSV into a DataFrame.
    
    Args:
        file_path: Path to a CSV file.
    Returns:
        DataFrame of the CSV contents.
    Raises:
        FileNotFoundError: if the file does not exist.
        pd.errors.ParserError: if pandas fails to parse it.
    """
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"No such file: {file_path}")
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Loaded {file_path.name} ({len(df):,} rows)")
        return df
    except pd.errors.ParserError as e:
        logger.exception(f"Parsing failed for {file_path.name}")
        raise


def load_all_data(raw_dir: Path = RAW_DATA_DIR) -> Dict[str, pd.DataFrame]:
    """
    Discover and load all CSV files in the raw data directory.
    
    Args:
        raw_dir: Directory containing raw CSVs.
    Returns:
        A dict mapping <basename> -> DataFrame, where basename is 
        the filename without extension (e.g., 'facebook_ads').
    """
    data: Dict[str, pd.DataFrame] = {}
    csv_files = list(raw_dir.glob("*.csv"))
    
    if not csv_files:
        logger.warning(f"No CSV files found in {raw_dir}")
    
    for file_path in csv_files:
        key = file_path.stem  # 'facebook_ads' instead of 'facebook_ads.csv'
        data[key] = load_csv(file_path)
    return data


if __name__ == "__main__":
    datasets = load_all_data()
    for name, df in datasets.items():
        logger.info(f"Dataset '{name}' shape: {df.shape}")
