import os
import sys
import matplotlib.pyplot as plt
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import OUTPUT_CHARTS_DIR, STOCKS, MA_SHORT_WINDOW, MA_LONG_WINDOW

def generate_visualizations(spark_df):
    print("Generating visualizations...")
    df = spark_df.toPandas()
    df['Date'] = pd.to_datetime(df['Date'])
    
    for stock in STOCKS:
        stock_df = df[df['Stock'] == stock].sort_values('Date')
        
        plt.figure(figsize=(10, 6))
        plt.plot(stock_df['Date'], stock_df['Close'])
        plt.title(f"{stock} Closing Price")
        plt.xlabel("Date")
        plt.ylabel("Price")
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_closing_price.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.plot(stock_df['Date'], stock_df['Close'], label='Close')
        plt.plot(stock_df['Date'], stock_df[f'MA_{MA_SHORT_WINDOW}'], label=f'MA {MA_SHORT_WINDOW}')
        plt.title(f"{stock} Price and MA{MA_SHORT_WINDOW}")
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_price_ma{MA_SHORT_WINDOW}.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.plot(stock_df['Date'], stock_df['Close'], label='Close')
        plt.plot(stock_df['Date'], stock_df[f'MA_{MA_SHORT_WINDOW}'], label=f'MA {MA_SHORT_WINDOW}')
        plt.plot(stock_df['Date'], stock_df[f'MA_{MA_LONG_WINDOW}'], label=f'MA {MA_LONG_WINDOW}')
        plt.title(f"{stock} Price and Moving Averages")
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_price_mas.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.bar(stock_df['Date'], stock_df['Volume'])
        plt.title(f"{stock} Trading Volume")
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_volume.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.plot(stock_df['Date'], stock_df['Rolling_Volatility'])
        plt.title(f"{stock} Rolling Volatility")
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_volatility.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.plot(stock_df['Date'], stock_df['Daily_Return_Percent'])
        plt.title(f"{stock} Daily Returns (%)")
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_returns.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.plot(stock_df['Date'], stock_df['Close'], label='Close', alpha=0.5)
        anomalies = stock_df[stock_df['Price_Anomaly'] == True]
        plt.scatter(anomalies['Date'], anomalies['Close'], color='red', label='Price Anomaly')
        plt.title(f"{stock} Price Anomalies")
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_price_anomalies.png"))
        plt.close()
        
        plt.figure(figsize=(10, 6))
        plt.bar(stock_df['Date'], stock_df['Volume'], alpha=0.5, label='Volume')
        vol_anomalies = stock_df[stock_df['Volume_Anomaly'] == True]
        plt.scatter(vol_anomalies['Date'], vol_anomalies['Volume'], color='red', label='Volume Anomaly', zorder=5)
        plt.title(f"{stock} Volume Anomalies")
        plt.legend()
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, f"{stock}_volume_anomalies.png"))
        plt.close()

    anomaly_counts = df[df['Final_Anomaly'] == True]['Stock'].value_counts()
    if not anomaly_counts.empty:
        plt.figure(figsize=(10, 6))
        anomaly_counts.plot(kind='bar')
        plt.title("Total Anomalies by Stock")
        plt.ylabel("Count")
        plt.tight_layout()
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, "anomaly_count_by_stock.png"))
        plt.close()
    
    anomaly_types = df[df['Anomaly_Type'] != 'Normal']['Anomaly_Type'].value_counts()
    if not anomaly_types.empty:
        plt.figure(figsize=(10, 6))
        anomaly_types.plot(kind='pie', autopct='%1.1f%%')
        plt.title("Anomaly Type Distribution")
        plt.ylabel("")
        plt.savefig(os.path.join(OUTPUT_CHARTS_DIR, "anomaly_type_distribution.png"))
        plt.close()
    
    print(f"Visualizations saved to {OUTPUT_CHARTS_DIR}")

if __name__ == "__main__":
    pass
