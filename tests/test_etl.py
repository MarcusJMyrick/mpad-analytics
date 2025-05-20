# tests/test_etl.py

import pytest
import pandas as pd
from pathlib import Path

from etl.ingest import load_all_data
from etl.transform import transform_all
import etl.load as load_module  # to monkeypatch CLEANED_DATA_DIR


@pytest.fixture
def raw_data():
    """Load the raw CSVs as a dict of DataFrames."""
    return load_all_data()


def test_load_all_data(raw_data):
    assert isinstance(raw_data, dict)
    # We expect at least these keys
    expected = {"facebook_ads", "google_ads", "email_campaigns",
                "customer_transactions", "website_visits"}
    assert expected.issubset(raw_data.keys())
    for name, df in raw_data.items():
        assert isinstance(df, pd.DataFrame)
        assert not df.empty, f"{name} should not be empty"


def test_transform_all(raw_data):
    cleaned = transform_all(raw_data)
    assert isinstance(cleaned, dict)
    for name, df in cleaned.items():
        # 1) Check date columns got parsed
        date_cols = [c for c in df.columns if "date" in c.lower()]
        assert date_cols, f"No date columns found in {name}"
        for col in date_cols:
            assert pd.api.types.is_datetime64_any_dtype(df[col]), (
                f"Column {col} in {name} is not datetime"
            )

        # 2) Check numeric casts on known numeric columns
        for col in ("cost", "clicks", "impressions", "opens",
                    "amount", "page_views", "session_duration"):
            if col in df.columns:
                assert df[col].dtype in (int, float), (
                    f"Column {col} in {name} has wrong dtype {df[col].dtype}"
                )

        # 3) No nulls introduced
        assert not df.isna().any().any(), f"Nulls found in {name} after transform"


def test_save_all_data(tmp_path, monkeypatch, raw_data):
    # Prepare cleaned data
    cleaned = transform_all(raw_data)

    # Monkey-patch CLEANED_DATA_DIR to our temp folder
    monkeypatch.setattr(load_module, "CLEANED_DATA_DIR", Path(tmp_path))

    # Call save_all_data, which should write one CSV per key
    load_module.save_all_data(cleaned)

    # Verify each file exists
    for name in cleaned.keys():
        path = Path(tmp_path) / f"{name}.csv"
        assert path.exists(), f"{path} was not created"
        # And that its row count matches the DataFrame
        df = pd.read_csv(path)
        assert len(df) == len(cleaned[name]), (
            f"Row count mismatch in {path}: {len(df)} vs {len(cleaned[name])}"
        )
