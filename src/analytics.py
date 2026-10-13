import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DATA_PROCESSED_DIR, OUTPUT_EXPORTS_DIR

def save_to_parquet(df):
    file_path = os.path.join(DATA_PROCESSED_DIR, "stock_data.parquet")
    df.write.mode("overwrite").parquet(file_path)
    print(f"Saved processed data to {file_path}")

def generate_exports(df, spark):
    df.createOrReplaceTempView("stock_data")
    
    # 1. Stock Summary
    summary = spark.sql("""
        SELECT Stock, 
               MIN(Date) as Start_Date,
               MAX(Date) as End_Date,
               COUNT(*) as Total_Days,
               AVG(Close) as Avg_Close,
               AVG(Volume) as Avg_Volume
        FROM stock_data
        GROUP BY Stock
    """)
    summary.toPandas().to_csv(os.path.join(OUTPUT_EXPORTS_DIR, "stock_summary.csv"), index=False)
    
    # 2. Anomaly Summary
    anomaly_summary = spark.sql("""
        SELECT Stock, 
               Anomaly_Type, 
               COUNT(*) as Count 
        FROM stock_data 
        WHERE Final_Anomaly = true
        GROUP BY Stock, Anomaly_Type
    """)
    anomaly_summary.toPandas().to_csv(os.path.join(OUTPUT_EXPORTS_DIR, "anomaly_summary.csv"), index=False)
    
    # 3. Anomaly Details
    anomaly_details = spark.sql("""
        SELECT Date, Stock, Close, Volume, Anomaly_Type 
        FROM stock_data 
        WHERE Final_Anomaly = true
        ORDER BY Date DESC
    """)
    anomaly_details.toPandas().to_csv(os.path.join(OUTPUT_EXPORTS_DIR, "anomaly_details.csv"), index=False)
    
    # 4. Volatility Summary
    volatility_summary = spark.sql("""
        SELECT Stock, 
               AVG(Rolling_Volatility) as Avg_Volatility,
               MAX(Rolling_Volatility) as Max_Volatility
        FROM stock_data
        GROUP BY Stock
    """)
    volatility_summary.toPandas().to_csv(os.path.join(OUTPUT_EXPORTS_DIR, "volatility_summary.csv"), index=False)
    print(f"Exports saved to {OUTPUT_EXPORTS_DIR}")
