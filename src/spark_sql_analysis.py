import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def run_spark_sql(df, spark):
    df.createOrReplaceTempView("stock_data")
    
    print("--- SPARK SQL ANALYSIS ---")
    
    print("1. Average, Min, Max Close by Stock")
    spark.sql("""
        SELECT Stock, 
               AVG(Close) as Avg_Close, 
               MIN(Close) as Min_Close, 
               MAX(Close) as Max_Close 
        FROM stock_data 
        GROUP BY Stock
    """).show()
    
    print("2. Average Volume by Stock")
    spark.sql("""
        SELECT Stock, 
               AVG(Volume) as Avg_Volume 
        FROM stock_data 
        GROUP BY Stock
    """).show()
    
    print("3. Average Volatility by Stock")
    spark.sql("""
        SELECT Stock, 
               AVG(Rolling_Volatility) as Avg_Volatility 
        FROM stock_data 
        WHERE Rolling_Volatility IS NOT NULL
        GROUP BY Stock
    """).show()
    
    print("4. Anomaly Count by Stock")
    spark.sql("""
        SELECT Stock, 
               SUM(CAST(Final_Anomaly AS INT)) as Total_Anomalies 
        FROM stock_data 
        GROUP BY Stock
    """).show()
    
    print("5. Anomaly Type Count")
    spark.sql("""
        SELECT Anomaly_Type, 
               COUNT(*) as Count 
        FROM stock_data 
        GROUP BY Anomaly_Type
    """).show()

if __name__ == "__main__":
    pass
