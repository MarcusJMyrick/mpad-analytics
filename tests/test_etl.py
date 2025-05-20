import pytest
from etl.ingest import load_all_data
from etl.transform import transform_data
from etl.load import save_all_data
import os
import pandas as pd


@pytest.fixture

def raw_data():
    return load_all_data()


def test_load_all_data(raw_data):
    assert isinstance(raw_data, dict)
    assert all(isinstance(df, pd.DataFrame) for df in raw_data.values())


def test_transform_data(raw_data):
    cleaned_data = transform_data(raw_data)
    for df in cleaned_data.values():
        assert 'timestamp' in df.columns
        assert df['timestamp'].dtype == 'datetime64[ns]'
        if 'cost' in df.columns:
            assert df['cost'].dtype == float
        if 'clicks' in df.columns:
            assert df['clicks'].dtype == int


def test_save_all_data(raw_data):
    cleaned_data = transform_data(raw_data)
    save_all_data(cleaned_data)
    for file_name in raw_data.keys():
        file_path = os.path.join('data/cleaned/', file_name)
        assert os.path.exists(file_path)
