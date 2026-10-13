import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

print("================ PARQUET ================")
os.environ["JAVA_HOME"] = "/Users/devshanbuag/Desktop/Stock_Market/jdk-17.0.2.jdk/Contents/Home"
spark = SparkSession.builder.appName("Verify").master("local[*]").getOrCreate()
pdf = spark.read.parquet("data/processed/stock_data.parquet")
row_list = pdf.select(F.min("Date"), F.max("Date")).collect()[0]
print(f"Minimum date: {row_list[0]}")
print(f"Maximum date: {row_list[1]}")
print("Schema:")
pdf.printSchema()

print("================ ANOMALIES ================")
summ = pd.read_csv("output/exports/anomaly_summary.csv")
det = pd.read_csv("output/exports/anomaly_details.csv")

total_anomalies = len(det)
print(f"Total anomalies: {total_anomalies}")
total_rows = 6205 # From parquet count
print(f"Anomaly percentage: {(total_anomalies / total_rows) * 100:.2f}%\n")

print("By stock:")
print(det['Stock'].value_counts().to_string())

print("\nBy type:")
print(det['Anomaly_Type'].value_counts().to_string())

