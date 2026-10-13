import os
import sys
import yfinance as yf
import pandas as pd

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import STOCKS, DEFAULT_PERIOD, DATA_RAW_DIR

def download_data():
    for stock in STOCKS:
        print(f"Downloading {stock} data...")
        data = yf.download(stock, period=DEFAULT_PERIOD)
        
        # Normalize MultiIndex columns from yfinance
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] for col in data.columns]
            
        if not data.empty:
            # Ensure correct column order
            req_cols = ["Open", "High", "Low", "Close", "Volume"]
            data = data[req_cols]
            
            file_path = os.path.join(DATA_RAW_DIR, f"{stock}.csv")
            data.to_csv(file_path)
            print(f"Saved {stock} to {file_path}")
            print(f"Rows: {len(data)}")
            print(f"Date range: {data.index.min()} to {data.index.max()}")
            print("Columns:", data.columns.tolist())
            print("-" * 30)
        else:
            print(f"Failed to download data for {stock}")

if __name__ == "__main__":
    download_data()
