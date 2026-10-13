# Project Report

## Overview
This project builds a PySpark-based data pipeline to analyze 5 years of daily stock data for RELIANCE.NS, TCS.NS, INFY.NS, HDFCBANK.NS, and ICICIBANK.NS. The pipeline includes data ingestion via yfinance, data cleaning, feature engineering, and anomaly detection based on Z-scores for both price returns and trading volumes.

## Results
- **Date Range**: 2021-09-29 to 2026-09-29
- **Total Records Processed**: 6205
- **Total Anomalies Detected**: 133
- **Anomalies by Stock**:
  - TCS.NS: 30
  - HDFCBANK.NS: 28
  - INFY.NS: 26
  - ICICIBANK.NS: 25
  - RELIANCE.NS: 24
- **Anomaly Types**:
  - Volume Spike: 81
  - Price Movement: 27
  - Price + Volume: 25

## Outputs
- **Raw Data**: `data/raw/*.csv`
- **Processed Data**: `data/processed/stock_data.parquet`
- **Visualizations**: `output/charts/*.png`
- **Exported Reports**: `output/exports/*.csv`
- **Dashboard**: Streamlit app at `dashboard/app.py`
