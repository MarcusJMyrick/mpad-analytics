from prophet import Prophet
import pandas as pd


def prepare_data_for_prophet(df, date_col='timestamp', value_col='purchase_amount'):
    """Prepare data for Prophet model."""
    df = df[[date_col, value_col]].rename(columns={date_col: 'ds', value_col: 'y'})
    return df


def forecast_roi(df, periods=30):
    """Forecast ROI using Prophet."""
    model = Prophet()
    model.fit(df)
    future = model.make_future_dataframe(periods=periods)
    forecast = model.predict(future)
    return forecast


if __name__ == "__main__":
    from etl.load import save_all_data
    from etl.ingest import load_all_data
    from etl.transform import transform_data
    raw_data = load_all_data()
    cleaned_data = transform_data(raw_data)
    for file_name, df in cleaned_data.items():
        if 'purchase_amount' in df.columns:
            prophet_data = prepare_data_for_prophet(df)
            forecast = forecast_roi(prophet_data)
            print(f"Forecasted ROI for {file_name}.")
            print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())
