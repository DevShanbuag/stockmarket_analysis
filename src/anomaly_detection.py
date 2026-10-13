import sys
import os
from pyspark.sql import Window, functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.spark_session import get_spark_session
from src.data_loader import load_data
from src.preprocessing import preprocess_data
from src.feature_engineering import engineer_features
from config import ANOMALY_Z_THRESHOLD, RETURN_WINDOW, VOLUME_WINDOW

def detect_anomalies(df):
    window_return = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-RETURN_WINDOW + 1, 0)
    window_volume = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-VOLUME_WINDOW + 1, 0)
    
    # 1. Price Anomaly
    df = df.withColumn(f"Return_Mean_{RETURN_WINDOW}", F.avg("Daily_Return").over(window_return))
    df = df.withColumn(f"Return_Std_{RETURN_WINDOW}", F.stddev("Daily_Return").over(window_return))
    
    df = df.withColumn("Return_ZScore", 
                       F.when((F.col(f"Return_Std_{RETURN_WINDOW}").isNotNull()) & (F.col(f"Return_Std_{RETURN_WINDOW}") > 0),
                              (F.col("Daily_Return") - F.col(f"Return_Mean_{RETURN_WINDOW}")) / F.col(f"Return_Std_{RETURN_WINDOW}"))
                       .otherwise(0.0))
    
    df = df.withColumn("Price_Anomaly", F.abs(F.col("Return_ZScore")) > ANOMALY_Z_THRESHOLD)
    
    # 2. Volume Anomaly
    df = df.withColumn(f"Volume_Mean_{VOLUME_WINDOW}", F.avg("Volume").over(window_volume))
    df = df.withColumn(f"Volume_Std_{VOLUME_WINDOW}", F.stddev("Volume").over(window_volume))
    
    df = df.withColumn("Volume_ZScore", 
                       F.when((F.col(f"Volume_Std_{VOLUME_WINDOW}").isNotNull()) & (F.col(f"Volume_Std_{VOLUME_WINDOW}") > 0),
                              (F.col("Volume") - F.col(f"Volume_Mean_{VOLUME_WINDOW}")) / F.col(f"Volume_Std_{VOLUME_WINDOW}"))
                       .otherwise(0.0))
    
    df = df.withColumn("Volume_Anomaly", F.col("Volume_ZScore") > ANOMALY_Z_THRESHOLD)
    
    # 3. Final Anomaly & Type
    df = df.withColumn("Final_Anomaly", F.col("Price_Anomaly") | F.col("Volume_Anomaly"))
    
    df = df.withColumn("Anomaly_Type", 
                       F.when(F.col("Price_Anomaly") & F.col("Volume_Anomaly"), "Price + Volume")
                       .when(F.col("Price_Anomaly"), "Price Movement")
                       .when(F.col("Volume_Anomaly"), "Volume Spike")
                       .otherwise("Normal"))
                       
    return df

if __name__ == "__main__":
    spark = get_spark_session()
    df = load_data(spark)
    if df:
        cleaned_df = preprocess_data(df)
        featured_df = engineer_features(cleaned_df)
        anomaly_df = detect_anomalies(featured_df)
        
        print("Anomaly Counts by Type:")
        anomaly_df.groupBy("Anomaly_Type").count().show()
