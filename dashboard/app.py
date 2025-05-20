import streamlit as st
import pandas as pd
import os
from models.attribution import linear_attribution, time_decay_attribution
from models.roi_forecast import prepare_data_for_prophet, forecast_roi

CLEANED_DATA_PATH = 'data/cleaned/'


def load_cleaned_data(file_name):
    """Load cleaned data from a CSV file."""
    file_path = os.path.join(CLEANED_DATA_PATH, file_name)
    return pd.read_csv(file_path)


def main():
    st.title('Marketing ROI Intelligence Platform')

    # Load data
    data_files = ['facebook_ads.csv', 'google_ads.csv', 'email_campaigns.csv', 'customer_transactions.csv', 'website_visits.csv']
    data = {file: load_cleaned_data(file) for file in data_files}

    # Select attribution model
    attribution_model = st.selectbox('Select Attribution Model', ['Linear', 'Time Decay'])

    # Display ROI by channel
    st.header('Channel-wise ROI')
    for file_name, df in data.items():
        if 'purchase_amount' in df.columns:
            if attribution_model == 'Linear':
                df = linear_attribution(df)
            else:
                df = time_decay_attribution(df)
            st.subheader(file_name)
            st.bar_chart(df[['timestamp', 'linear_attribution' if attribution_model == 'Linear' else 'time_decay_attribution']].set_index('timestamp'))

    # Forecast future ROI
    st.header('ROI Forecast')
    for file_name, df in data.items():
        if 'purchase_amount' in df.columns:
            prophet_data = prepare_data_for_prophet(df)
            forecast = forecast_roi(prophet_data)
            st.subheader(f'Forecast for {file_name}')
            st.line_chart(forecast[['ds', 'yhat']].set_index('ds'))


if __name__ == "__main__":
    main()
