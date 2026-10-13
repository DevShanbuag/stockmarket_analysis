import os

def get_spark_session(app_name="StockMarketBDA"):
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("Spark version:", spark.version)
    
    import sys
    print("Python version:", sys.version)
    
    print("Java version:")
    os.system("java -version")
    
    df = spark.createDataFrame([{"test": "success"}])
    df.show()
