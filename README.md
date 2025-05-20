# Marketing ROI Intelligence Platform (MRIP)

## Overview
The Marketing ROI Intelligence Platform (MRIP) is a simulation of a media analytics pipeline designed for a company like Ovative Group. It aims to provide insights into marketing ROI through data ingestion, transformation, modeling, and visualization. The platform leverages Python, SQL, Airflow, and Streamlit to process and analyze marketing and customer data.

## Architecture
- **Data**: Raw and cleaned data stored in CSV files.
- **ETL**: Modular scripts for data ingestion, transformation, and loading.
- **Models**: Attribution and forecasting models for marketing analysis.
- **Dashboard**: Streamlit app for visualizing marketing insights.
- **Automation**: Airflow DAGs for automating ETL and modeling processes.

## Features
- **ETL Pipeline**: Ingests raw data, cleans and standardizes it, and loads it into a cleaned data directory.
- **Attribution Models**: Implements linear and time-decay attribution models.
- **RFM Segmentation**: Provides customer segmentation based on recency, frequency, and monetary value.
- **ROI Forecasting**: Uses Prophet to forecast future ROI.
- **Streamlit Dashboard**: Visualizes channel-wise ROI, attribution breakdown, and ROI forecasts.
- **Airflow Automation**: Automates the ETL and modeling processes with daily DAG runs.

## Running the Project Locally
### Prerequisites
- Docker and Docker Compose
- Python 3.8+

### Setup
1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd mpad-analytics
   ```

2. **Build and run the Docker containers**:
   ```bash
   docker-compose up --build
   ```

3. **Access the Streamlit dashboard**:
   Open your browser and go to `http://localhost:8501`

4. **Access the Airflow UI**:
   Open your browser and go to `http://localhost:8080`

## Tech Stack
- **Python**: Data processing and modeling
- **Pandas**: Data manipulation
- **Streamlit**: Dashboard visualization
- **Apache Airflow**: Workflow automation
- **Prophet**: Time series forecasting
- **PostgreSQL**: Database for Airflow

## Business Case (Optional)
For a retail client, the MRIP system can provide valuable insights into marketing effectiveness, customer behavior, and future ROI predictions, enabling data-driven decision-making and optimized marketing strategies.
