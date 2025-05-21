# dashboard/app.py

import streamlit as st
import pandas as pd
from pathlib import Path
import logging
from typing import List, Dict, Any, Optional, Tuple

# Assuming your models are structured to be imported like this
# It's good practice for model functions to accept DataFrames and return DataFrames or relevant types
from models.attribution import linear_attribution, time_decay_attribution
from models.rfm_segmentation import calculate_rfm
from models.roi_forecast import forecast_roi # Assuming prepare_time_series is internal or called by forecast_roi

# --- Configuration & Constants ---
st.set_page_config(
    page_title="Marketing ROI Intelligence",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constants ---
DATA_DIR = Path("data/cleaned")
DEFAULT_FORECAST_PERIODS = 30

# Standard column names (use these in your ETL and throughout the app)
COL_TIMESTAMP = "timestamp"
COL_CHANNEL = "channel"
COL_CAMPAIGN_ID = "campaign_id"
COL_PURCHASE_DATE = "purchase_date"
COL_PURCHASE_AMOUNT = "purchase_amount"
COL_CUSTOMER_ID = "customer_id" # Assuming RFM needs this
COL_COST = "cost"

# --- Utility Functions ---
def display_error(message: str):
    """Displays an error message in the Streamlit app."""
    st.error(f"An error occurred: {message}")
    logger.error(message)

# --- Data Loading & Caching ---
@st.cache_data(ttl=3600) # Cache for 1 hour
def load_individual_data(file_path: Path, date_cols: List[str]) -> Optional[pd.DataFrame]:
    """Loads a single CSV file with error handling."""
    try:
        if not file_path.exists():
            logger.warning(f"Data file not found: {file_path}")
            return None
        return pd.read_csv(file_path, parse_dates=date_cols)
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return None

def load_csv_with_timestamp(path: Path) -> pd.DataFrame:
    """
    Load a CSV, parse any date column, and unify it into 'timestamp'.
    """
    df = pd.read_csv(path)
    # find date columns
    date_cols = [c for c in df.columns if "date" in c.lower()]
    if not date_cols:
        logger.error(f"No date column found in {path.name}")
        raise KeyError(f"No date column in {path.name}")
    # parse the first date column
    df[date_cols[0]] = pd.to_datetime(df[date_cols[0]], errors="coerce")
    # rename it to our standard timestamp
    df = df.rename(columns={date_cols[0]: COL_TIMESTAMP})
    logger.info(f"Parsed and renamed '{date_cols[0]}' to '{COL_TIMESTAMP}' for {path.name}")
    return df

@st.cache_data(ttl=3600)
def load_data() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and unify data from CSV files, renaming and merging as necessary.
    Returns:
        Tuple containing the merged ad events DataFrame and the raw transactions DataFrame.
    """
    logger.info("Loading and merging data...")

    # Load and unify dates
    fb = load_csv_with_timestamp(DATA_DIR / "facebook_ads.csv")
    fb[COL_CHANNEL] = "facebook"

    ggl = load_csv_with_timestamp(DATA_DIR / "google_ads.csv")
    ggl[COL_CHANNEL] = "google"

    email = load_csv_with_timestamp(DATA_DIR / "email_campaigns.csv")
    email[COL_CHANNEL] = "email"

    txn = load_csv_with_timestamp(DATA_DIR / "customer_transactions.csv")
    txn = txn.rename(columns={
        "amount": "purchase_amount",
        "purchase_date": "timestamp"
    })
    # verify:
    assert 'customer_id' in txn.columns, "customer_id missing from transactions"
    assert 'purchase_amount' in txn.columns, "purchase_amount missing from transactions"

    # Debug: confirm columns & sample rows for transactions
    logger.info("Transactions columns after rename: %s", txn.columns.tolist())
    st.sidebar.expander("🔍 Raw Transactions").dataframe(txn.head())

    # Merge purchase_amount into each ad frame
    def merge_ads(ad_df: pd.DataFrame) -> pd.DataFrame:
        return ad_df.merge(
            txn[[COL_CAMPAIGN_ID, COL_PURCHASE_AMOUNT]],
            on=COL_CAMPAIGN_ID,
            how="left"
        )

    ads = pd.concat([merge_ads(fb), merge_ads(ggl), merge_ads(email)], ignore_index=True)
    ads.dropna(subset=[COL_TIMESTAMP], inplace=True)

    # Debug: confirm columns & sample rows for ads
    logger.info("Ads columns: %s", ads.columns.tolist())
    st.sidebar.expander("🔍 Merged Ads").dataframe(ads.head())

    return ads, txn

# --- Main App Logic ---
ads, txn = load_data()

# Verify presence of customer_id in transactions
if COL_CUSTOMER_ID not in txn.columns:
    logger.error(f"Column '{COL_CUSTOMER_ID}' not found in transactions.")
else:
    logger.info(f"Column '{COL_CUSTOMER_ID}' successfully found in transactions.")

# --- Sidebar Controls ---
st.sidebar.header("⚙️ Settings")

# Attribution Model Selector
attr_model_options = ["Linear", "Time Decay"]
attr_model_selected = st.sidebar.selectbox(
    "🎯 Attribution Model",
    attr_model_options,
    help="Choose the model to attribute conversions to marketing channels."
)

# Date Range Filter
min_date = ads[COL_TIMESTAMP].min().date()
max_date = ads[COL_TIMESTAMP].max().date()

# Check if min_date and max_date are the same, which can cause issues with date_input
if min_date == max_date:
    # Provide a sensible default range, e.g., include the single day
    start_date_default, end_date_default = min_date, max_date
else:
    start_date_default, end_date_default = min_date, max_date
    
start_date, end_date = st.sidebar.date_input(
    "🗓️ Date Range (Ad Interaction)",
    [start_date_default, end_date_default],
    min_value=min_date,
    max_value=max_date,
    help="Filter data based on the ad interaction timestamp."
)

if start_date > end_date:
    st.sidebar.warning("Start date cannot be after end date.")
    start_date = end_date # Or handle as an error

# Apply Date Filter
date_mask = (ads[COL_TIMESTAMP].dt.date >= start_date) & (ads[COL_TIMESTAMP].dt.date <= end_date)
filtered_data = ads.loc[date_mask].copy()

if filtered_data.empty:
    st.warning("No data available for the selected date range based on ad interaction time.")
else:
    # Channel Selector (dynamic based on filtered data)
    available_channels = sorted(filtered_data[COL_CHANNEL].unique())
    if not available_channels:
        st.sidebar.info("No channels found in the filtered data.")
        selected_channels = []
    else:
        selected_channels = st.sidebar.multiselect(
            "📊 Channels",
            available_channels,
            default=available_channels,
            help="Select marketing channels to analyze."
        )
        if selected_channels:
            filtered_data = filtered_data[filtered_data[COL_CHANNEL].isin(selected_channels)]
        else:
            st.sidebar.warning("No channels selected. Please select at least one channel.")
            filtered_data = pd.DataFrame() # Empty dataframe

# --- Main Page ---
st.title("📈 Marketing ROI Intelligence Platform")

if filtered_data.empty:
    st.info("Please select filters in the sidebar to see the analysis.")
    st.stop()

# Attribution Section
st.header("💰 Attribution & ROI by Channel")
# Note: The attribution models need to correctly handle the `timestamp` (ad date)
# and `txn_purchase_date` (transaction date) from the `filtered_data`.
# They should also handle `purchase_amount`.

# Caching for attribution results
@st.cache_data
def get_attribution_results(df: pd.DataFrame, model_name: str) -> pd.DataFrame:
    logger.info(f"Calculating attribution with {model_name} model...")
    # Ensure attribution models are robust to NaNs in purchase_amount or txn_purchase_date
    # The attribution model should return a DataFrame with attributed revenue per channel and timestamp.
    # Key columns expected: COL_CHANNEL, COL_TIMESTAMP (of ad), 'attributed_revenue', 'cost'
    if model_name == "Linear":
        # Assuming linear_attribution takes the merged df and returns df with 'attributed_revenue'
        # It needs to consider 'cost' and 'purchase_amount' linked by 'campaign_id'
        # and distribute 'purchase_amount' based on its logic.
        attr_df = linear_attribution(df.copy()) # Pass a copy to avoid modifying cached df
        # Example output columns: channel, timestamp (of ad), cost, attributed_revenue
    elif model_name == "Time Decay":
        attr_df = time_decay_attribution(df.copy())
    else:
        raise ValueError("Invalid attribution model selected")
    logger.info(f"Attribution calculation complete. Shape: {attr_df.shape}")
    return attr_df

try:
    attribution_df = get_attribution_results(filtered_data, attr_model_selected)

    # Verify presence of attributed_revenue
    if 'attributed_revenue' not in attribution_df.columns:
        logger.error("Column 'attributed_revenue' not found in attribution results.")
    else:
        logger.info("Column 'attributed_revenue' successfully created in attribution results.")

    if attribution_df.empty:
        st.warning("Attribution results are empty. Check data and model logic.")
    else:
        # Calculate ROI: (Attributed Revenue - Cost) / Cost
        # Ensure 'cost' and 'attributed_revenue' are present and numeric
        if 'cost' in attribution_df.columns and 'attributed_revenue' in attribution_df.columns:
            attribution_df['cost'] = pd.to_numeric(attribution_df['cost'], errors='coerce').fillna(0)
            attribution_df['attributed_revenue'] = pd.to_numeric(attribution_df['attributed_revenue'], errors='coerce').fillna(0)
            
            # Avoid division by zero for ROI
            attribution_df["roi"] = attribution_df.apply(
                lambda row: (row["attributed_revenue"] - row["cost"]) / row["cost"] if row["cost"] > 0 else 0,
                axis=1
            )
            
            # Display ROI by channel
            # Group by channel and sum up cost and revenue to show aggregate ROI
            summary_roi = attribution_df.groupby(COL_CHANNEL).agg(
                total_cost=('cost', 'sum'),
                total_attributed_revenue=('attributed_revenue', 'sum')
            ).reset_index()
            
            summary_roi["overall_roi"] = summary_roi.apply(
                lambda row: (row["total_attributed_revenue"] - row["total_cost"]) / row["total_cost"] if row["total_cost"] > 0 else 0,
                axis=1
            )
            
            st.subheader("Channel Performance Summary")
            st.dataframe(summary_roi.style.format({
                "total_cost": "${:,.2f}",
                "total_attributed_revenue": "${:,.2f}",
                "overall_roi": "{:.2%}"
            }), use_container_width=True)

            # Plot attributed revenue over time for selected channels
            st.subheader("Attributed Revenue Over Time")
            # Pivot for plotting multiple lines
            pivot_attr = attribution_df.groupby([COL_TIMESTAMP, COL_CHANNEL])['attributed_revenue'].sum().unstack(fill_value=0)
            if not pivot_attr.empty:
                st.line_chart(pivot_attr[selected_channels], use_container_width=True)
            else:
                st.info("No attributed revenue data to plot for the selected channels and time.")

        else:
            st.warning("Columns 'cost' or 'attributed_revenue' not found in attribution results.")

except Exception as e:
    display_error(f"Failed to calculate attribution: {e}")
    attribution_df = pd.DataFrame() # ensure it's defined for later checks

# --- Customer RFM Segmentation ---
try:
    st.header("👥 Customer RFM Segmentation")
    # Verify presence of customer_id before RFM calculation
    if COL_CUSTOMER_ID not in txn.columns:
        logger.error(f"Column '{COL_CUSTOMER_ID}' not found in transactions before RFM calculation.")
    else:
        logger.info(f"Column '{COL_CUSTOMER_ID}' successfully found in transactions before RFM calculation.")
    rfm = calculate_rfm(txn)   # txn has customer_id & purchase_amount & timestamp
    st.dataframe(rfm)
except Exception as e:
    display_error(f"'{COL_CUSTOMER_ID}' column not found. Cannot perform RFM segmentation.")

# ROI Forecast (using attributed revenue)
st.header("🔮 Combined ROI & Revenue Forecast")

# Caching for forecast results
@st.cache_data
def get_roi_forecast(attr_df: pd.DataFrame, periods: int) -> Optional[Tuple[pd.Series, pd.Series]]:
    logger.info("Preparing data and forecasting ROI/Revenue...")
    if 'attributed_revenue' not in attr_df.columns or attr_df.empty:
        logger.warning("Attributed revenue not available for forecasting.")
        return None, None

    # Aggregate daily attributed revenue for forecasting
    # Using the ad interaction timestamp as the basis for the time series.
    daily_revenue_ts = (
        attr_df.groupby(pd.Grouper(key=COL_TIMESTAMP, freq='D'))['attributed_revenue']
        .sum()
        .fillna(0)
    )

    if daily_revenue_ts.empty or len(daily_revenue_ts) < 2: # Holt-Winters needs at least 2 periods typically
        logger.warning("Not enough data points for time series forecasting after aggregation.")
        return daily_revenue_ts, None # Return history even if forecast fails

    # The forecast_roi function should handle model fitting and prediction
    # It should return a pandas Series of forecasted values
    try:
        forecast_series = forecast_roi(daily_revenue_ts, periods=periods)
        logger.info(f"Forecasting complete. Forecasted {len(forecast_series)} periods.")
        return daily_revenue_ts, forecast_series
    except Exception as e:
        logger.error(f"Error during forecasting: {e}")
        return daily_revenue_ts, None # Return history, but no forecast

if not attribution_df.empty:
    historical_revenue, forecast_values = get_roi_forecast(attribution_df, periods=DEFAULT_FORECAST_PERIODS)

    if historical_revenue is not None and not historical_revenue.empty:
        st.subheader("Attributed Revenue: History & Forecast")
        history_plot_df = historical_revenue.rename("Actual Revenue")
        
        if forecast_values is not None and not forecast_values.empty:
            forecast_plot_df = forecast_values.rename("Forecasted Revenue")
            combined_plot_df = pd.concat([history_plot_df, forecast_plot_df], axis=1)
        else:
            st.info("Forecast could not be generated. Displaying historical data only.")
            combined_plot_df = history_plot_df.to_frame()
            
        st.line_chart(combined_plot_df, use_container_width=True)
    else:
        st.info("No historical attributed revenue to display or forecast.")
else:
    st.info("Attribution results are not available, so forecasting cannot be performed.")


# Raw Data Expander
with st.expander("📂 Show Filtered & Merged Data Sample", expanded=False):
    if not filtered_data.empty:
        st.dataframe(filtered_data.head(100), use_container_width=True)
        st.caption(f"Displaying the first 100 rows of the {len(filtered_data)} filtered rows.")
    else:
        st.caption("No data to display.")

logger.info("Dashboard rendering complete.")