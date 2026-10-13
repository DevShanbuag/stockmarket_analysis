import sys
import os
from pyspark.sql import functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.spark_session import get_spark_session
from src.data_loader import load_data

def preprocess_data(df):
    # Null checks and removal
    df = df.dropna(subset=["Date", "Close", "Volume", "Stock"])
    
    # Deduplicate
    df = df.dropDuplicates(["Stock", "Date"])
    
    # Numeric validation
    df = df.filter((F.col("Close") > 0) & (F.col("Volume") >= 0))
    
    return df

if __name__ == "__main__":
    spark = get_spark_session()
    df = load_data(spark)
    if df:
        print("Before preprocessing:", df.count())
        cleaned_df = preprocess_data(df)
        print("After preprocessing:", cleaned_df.count())
