import pytest
from models.attribution import linear_attribution, time_decay_attribution
from models.rfm_segmentation import calculate_rfm
from models.roi_forecast import prepare_data_for_prophet, forecast_roi
import pandas as pd


@pytest.fixture

def sample_data():
    data = {
        'timestamp': pd.date_range(start='2023-01-01', periods=5, freq='D'),
        'purchase_amount': [100, 200, 300, 400, 500],
        'customer_id': [1, 2, 3, 4, 5]
    }
    return pd.DataFrame(data)


def test_linear_attribution(sample_data):
    df = linear_attribution(sample_data)
    assert 'linear_attribution' in df.columns
    assert df['linear_attribution'].sum() == sample_data['purchase_amount'].sum()


def test_time_decay_attribution(sample_data):
    df = time_decay_attribution(sample_data)
    assert 'time_decay_attribution' in df.columns
    assert df['time_decay_attribution'].iloc[-1] > df['time_decay_attribution'].iloc[0]


def test_calculate_rfm(sample_data):
    rfm = calculate_rfm(sample_data)
    assert 'Recency' in rfm.columns
    assert 'Frequency' in rfm.columns
    assert 'Monetary' in rfm.columns


def test_forecast_roi(sample_data):
    prophet_data = prepare_data_for_prophet(sample_data)
    forecast = forecast_roi(prophet_data)
    assert 'yhat' in forecast.columns
    assert len(forecast) > len(sample_data)
