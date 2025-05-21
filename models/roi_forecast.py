# models/roi_forecast.py

import logging
import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from typing import Optional

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def prepare_time_series(
    df: pd.DataFrame,
    date_col: str = "date",
    value_col: str = "purchase_amount",
    freq: str = "D",
) -> pd.Series:
    """
    Aggregate df by date and return a time series with specified frequency.
    Missing dates are filled with zeros.
    """
    ts = (
        df.groupby(pd.to_datetime(df[date_col]))[value_col]
        .sum()
        .asfreq(freq)
        .fillna(0)
    )
    logger.info(f"Prepared time series from {value_col} with freq='{freq}'")
    return ts


def forecast_roi(
    ts: pd.Series,
    periods: int = 30,
    trend: str = "add",
    seasonal: Optional[str] = None,
    seasonal_periods: Optional[int] = None
) -> pd.Series:
    """
    Forecast future values using Holt-Winters Exponential Smoothing.
    Returns a Series of length `periods`.
    """
    model = ExponentialSmoothing(ts, trend=trend, seasonal=seasonal, seasonal_periods=seasonal_periods)
    fit = model.fit(optimized=True)
    forecast = fit.forecast(periods)
    logger.info(f"Forecasted next {periods} periods using Holt-Winters")
    return forecast
