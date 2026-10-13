import os
import pandas as pd
from config import STOCKS, DATA_RAW_DIR

print("Ticker | Rows | First Date | Last Date | Status")

for ticker in STOCKS:
    file_path = os.path.join(DATA_RAW_DIR, f"{ticker}.csv")
    status = "SUCCESS"
    rows = 0
    first_date = "N/A"
    last_date = "N/A"
    
    if not os.path.exists(file_path):
        status = "FAILED (Not found)"
    elif os.path.getsize(file_path) == 0:
        status = "FAILED (Empty file)"
    else:
        try:
            df = pd.read_csv(file_path)
            rows = len(df)
            if rows == 0:
                status = "FAILED (0 rows)"
            else:
                req_cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
                missing = [c for c in req_cols if c not in df.columns]
                if missing:
                    status = f"FAILED (Missing: {missing})"
                else:
                    first_date = str(df["Date"].min())[:10]
                    last_date = str(df["Date"].max())[:10]
        except Exception as e:
            status = f"FAILED ({str(e)})"
            
    print(f"{ticker} | {rows} | {first_date} | {last_date} | {status}")

