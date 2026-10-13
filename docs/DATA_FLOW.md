# Data Flow

1. **Source**: yfinance API downloads stock data (Date, Open, High, Low, Close, Volume).
2. **Ingestion**: Data saved as temporary CSVs in `data/raw/`.
3. **PySpark Loader**: `data_loader.py` reads CSVs, unifies schema, appends a `Stock` column.
4. **Preprocessing**: Nulls dropped, schema validated in `preprocessing.py`.
5. **Feature Engineering**: Window functions used to calculate MAs, daily returns, volatility.
6. **Anomaly Detection**: Rolling Z-scores calculated to flag price and volume spikes.
7. **Storage**: Final DataFrame serialized to `data/processed/stock_data.parquet`.
8. **Downstream**: `analytics.py` and `visualization.py` consume this dataframe/parquet to produce exports and charts. The Streamlit app independently queries the Parquet file.
