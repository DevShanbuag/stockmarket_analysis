from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.appName("Verify").master("local[*]").getOrCreate()
df = spark.read.parquet("data/processed/stock_data.parquet")
print("Row count:", df.count())
print("Stock count:", df.select("Stock").distinct().count())
print("Stocks present:")
df.select("Stock").distinct().show()
df.select(F.min("Date").alias("min_date"), F.max("Date").alias("max_date")).show()
print("Schema:")
df.printSchema()
