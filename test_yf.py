import yfinance as yf
import pandas as pd
print("Testing yfinance...")
data = yf.download('RELIANCE.NS', period='1mo')
print("Type:", type(data))
print("Rows:", len(data))
print("Columns:", data.columns.tolist())
print("Head:")
print(data.head(3))
print("Tail:")
print(data.tail(3))
