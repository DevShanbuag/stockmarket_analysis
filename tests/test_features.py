import pytest
from pyspark.sql import SparkSession
from datetime import date
from src.feature_engineering import engineer_features

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestFeatures").getOrCreate()

def test_engineer_features(spark):
    data = [
        (date(2023, 1, 1), "TCS", 100.0, 105.0, 95.0, 100.0, 1000),
        (date(2023, 1, 2), "TCS", 102.0, 106.0, 96.0, 105.0, 1200),
    ]
    schema = ["Date", "Stock", "Open", "High", "Low", "Close", "Volume"]
    df = spark.createDataFrame(data, schema)
    
    result = engineer_features(df)
    
    assert "Daily_Return" in result.columns
    assert "Rolling_Volatility" in result.columns
    
    rows = result.orderBy("Date").collect()
    assert rows[1]["Daily_Return"] == 0.05
