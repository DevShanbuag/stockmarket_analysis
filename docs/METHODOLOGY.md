# Methodology

## Preprocessing
- Removal of nulls in critical columns (Date, Close, Volume).
- Deduplication of identical dates for the same stock.
- Ensuring all closing prices and volumes are non-negative.

## Feature Engineering
- **Moving Averages**: 20-day and 50-day windows to capture trend directions.
- **Volatility**: 20-day rolling standard deviation of daily returns.
- **Volume Ratio**: Current daily volume divided by the 20-day average volume.

## Anomaly Detection
- **Price Anomaly**: Normalized daily return converted to a Z-score based on a 30-day rolling window. An anomaly is flagged if |Z-score| > 3.0.
- **Volume Anomaly**: Daily volume converted to a Z-score based on a 20-day rolling window. An anomaly is flagged if Z-score > 3.0.
