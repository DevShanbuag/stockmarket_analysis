import os
import sys
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, LongType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import STOCKS, DATA_RAW_DIR
from src.spark_session import get_spark_session

def load_data(spark):
    schema = StructType([
        StructField("Date", DateType(), True),
        StructField("Price", DoubleType(), True), # Ignore unused yf columns if needed, but we will define all
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True)
    ])
    
    combined_df = None
    
    for stock in STOCKS:
        file_path = os.path.join(DATA_RAW_DIR, f"{stock}.csv")
        if not os.path.exists(file_path):
            print(f"Warning: {file_path} not found.")
            continue
            
        # We read as string first then cast to avoid schema mismatch if headers differ slightly
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        # Select required columns only and cast
        df = df.select(
            F.to_date(F.col("Date")).alias("Date"),
            F.col("Open").cast(DoubleType()).alias("Open"),
            F.col("High").cast(DoubleType()).alias("High"),
            F.col("Low").cast(DoubleType()).alias("Low"),
            F.col("Close").cast(DoubleType()).alias("Close"),
            F.col("Volume").cast(LongType()).alias("Volume")
        )
        
        df = df.withColumn("Stock", F.lit(stock))
        
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.unionByName(df)
            
    return combined_df

if __name__ == "__main__":
    spark = get_spark_session()
    df = load_data(spark)
    if df:
        print("Total rows:", df.count())
        df.groupBy("Stock").count().show()
        df.select(F.min("Date").alias("Minimum date"), F.max("Date").alias("Maximum date")).show()
        df.printSchema()
