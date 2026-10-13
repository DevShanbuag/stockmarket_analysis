import sys
import os
from pyspark.sql import Window, functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.spark_session import get_spark_session
from src.data_loader import load_data
from src.preprocessing import preprocess_data
from config import RETURN_WINDOW, VOLATILITY_WINDOW, MA_SHORT_WINDOW, MA_LONG_WINDOW, VOLUME_WINDOW

def engineer_features(df):
    window_spec = Window.partitionBy("Stock").orderBy("Date")
    
    df = df.withColumn("Previous_Close", F.lag("Close", 1).over(window_spec))
    
    df = df.withColumn("Daily_Return", 
                       F.when(F.col("Previous_Close") > 0, (F.col("Close") - F.col("Previous_Close")) / F.col("Previous_Close"))
                       .otherwise(0.0))
    df = df.withColumn("Daily_Return_Percent", F.col("Daily_Return") * 100)
    
    window_ma_short = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-MA_SHORT_WINDOW + 1, 0)
    window_ma_long = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-MA_LONG_WINDOW + 1, 0)
    
    df = df.withColumn(f"MA_{MA_SHORT_WINDOW}", F.avg("Close").over(window_ma_short))
    df = df.withColumn(f"MA_{MA_LONG_WINDOW}", F.avg("Close").over(window_ma_long))
    
    window_volatility = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-VOLATILITY_WINDOW + 1, 0)
    df = df.withColumn("Rolling_Volatility", F.stddev("Daily_Return").over(window_volatility))
    df = df.withColumn("Volatility_Percent", F.stddev("Daily_Return_Percent").over(window_volatility))
    
    window_volume = Window.partitionBy("Stock").orderBy("Date").rowsBetween(-VOLUME_WINDOW + 1, 0)
    df = df.withColumn(f"Average_Volume_{VOLUME_WINDOW}", F.avg("Volume").over(window_volume))
    
    # handle division by zero
    df = df.withColumn("Volume_Ratio", 
                       F.when(F.col(f"Average_Volume_{VOLUME_WINDOW}") > 0, F.col("Volume") / F.col(f"Average_Volume_{VOLUME_WINDOW}"))
                       .otherwise(0.0))
    
    df = df.withColumn("Daily_Price_Range", F.col("High") - F.col("Low"))
    df = df.withColumn("Daily_Price_Range_Percent", 
                       F.when(F.col("Previous_Close") > 0, (F.col("Daily_Price_Range") / F.col("Previous_Close")) * 100)
                       .otherwise(0.0))
    
    df = df.withColumn("Gap_Percent", 
                       F.when(F.col("Previous_Close") > 0, ((F.col("Open") - F.col("Previous_Close")) / F.col("Previous_Close")) * 100)
                       .otherwise(0.0))
    
    return df

if __name__ == "__main__":
    spark = get_spark_session()
    df = load_data(spark)
    if df:
        cleaned_df = preprocess_data(df)
        featured_df = engineer_features(cleaned_df)
        featured_df.select("Date", "Stock", "Close", "Daily_Return_Percent", "Rolling_Volatility").show(5)
