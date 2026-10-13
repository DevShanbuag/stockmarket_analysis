# Data Dictionary

| Column | Type | Description |
|--------|------|-------------|
| Date | Date | The trading date |
| Stock | String | Ticker symbol |
| Open | Double | Opening price |
| High | Double | Highest price of the day |
| Low | Double | Lowest price of the day |
| Close | Double | Closing price of the day |
| Volume | Long | Trading volume |
| Previous_Close | Double | Closing price of the previous day |
| Daily_Return | Double | Normalized daily price return ((Close - Previous_Close) / Previous_Close) |
| Daily_Return_Percent | Double | Percentage daily return |
| MA_20 | Double | 20-day Moving Average of Close |
| MA_50 | Double | 50-day Moving Average of Close |
| Rolling_Volatility | Double | 20-day rolling standard deviation of daily return |
| Price_Anomaly | Boolean | True if absolute return Z-score > 3.0 |
| Volume_Anomaly | Boolean | True if volume Z-score > 3.0 |
| Final_Anomaly | Boolean | True if either Price or Volume anomaly is present |
| Anomaly_Type | String | 'Normal', 'Price Movement', 'Volume Spike', or 'Price + Volume' |
