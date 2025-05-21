# tests/test_models.py

import pytest
import pandas as pd
from datetime import datetime

# match the actual functions in your code
from models.attribution import linear_attribution, time_decay_attribution
from models.rfm_segmentation import calculate_rfm
from models.roi_forecast import prepare_time_series, forecast_roi


@pytest.fixture
def sample_transactions():
    return pd.DataFrame({
        "customer_id": [1, 1, 2, 2, 3],
        "purchase_date": [
            "2024-01-01","2024-01-10",
            "2024-01-05","2024-01-15","2024-01-20"
        ],
        "purchase_amount": [100, 200, 150, 250, 300]
    })


@pytest.fixture
def sample_ts():
    # a simple increasing series over 10 days
    dates = pd.date_range("2024-01-01", periods=10, freq="D")
    vals = [100 + i*10 for i in range(10)]
    return pd.Series(vals, index=dates)


def test_linear_attribution(sample_transactions):
    df = linear_attribution(sample_transactions)
    assert "linear_attribution" in df.columns
    assert df["linear_attribution"].sum() == sample_transactions["purchase_amount"].sum()


def test_time_decay_attribution(sample_transactions):
    decay = 0.5
    df = time_decay_attribution(sample_transactions, decay_rate=decay)
    assert "time_decay_attribution" in df.columns
    # last value > first when decay < 1 and amounts increase
    assert df["time_decay_attribution"].iloc[-1] > df["time_decay_attribution"].iloc[0]


def test_compute_rfm(sample_transactions):
    rfm = calculate_rfm(sample_transactions, snapshot_date=datetime(2024, 1, 21))
    # Should have one row per unique customer_id
    assert set(rfm["customer_id"]) == {1, 2, 3}
    # Recency for customer 1: (2024-01-21 - 2024-01-10) = 11 days
    recency_1 = rfm.loc[rfm["customer_id"] == 1, "Recency"].iloc[0]
    assert recency_1 == 11
    # Check for expected columns
    for col in ("Recency", "Frequency", "Monetary"):
        assert col in rfm.columns


def test_prepare_time_series_and_forecast(sample_ts):
    ts = prepare_time_series(
        df=pd.DataFrame({"date": sample_ts.index, "purchase_amount": sample_ts.values}),
        date_col="date",
        value_col="purchase_amount",
        freq="D"
    )
    assert isinstance(ts, pd.Series)
    forecast = forecast_roi(ts, periods=5)
    assert isinstance(forecast, pd.Series)
    assert len(forecast) == 5
    # Forecasts should be non-negative
    assert (forecast >= 0).all()

