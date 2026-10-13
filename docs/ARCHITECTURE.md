# Architecture

The system uses a modular, multi-step pipeline built on Apache Spark.

## Components
1. **Data Ingestion**: `yfinance` fetches raw data and stores it as CSVs.
2. **Data Processing Engine**: PySpark reads the CSVs, preprocesses them, and engineers features across distributed partitions.
3. **Storage**: The cleaned and enriched dataset is stored in an optimized columnar format (Parquet).
4. **Analytics & Visualization**: Spark SQL aggregates the data, and pandas/matplotlib generate summary exports and PNG charts.
5. **Dashboard**: A Streamlit application loads the processed Parquet data into memory for interactive querying.
