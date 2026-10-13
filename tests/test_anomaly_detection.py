import pytest
from pyspark.sql import SparkSession
from datetime import date
from src.anomaly_detection import detect_anomalies
from src.feature_engineering import engineer_features

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestAnomaly").getOrCreate()

def test_anomaly_detection(spark):
    data = [(date(2023, 1, i), "TCS", 100.0, 105.0, 95.0, 100.0 + (i % 2), 1000) for i in range(1, 10)]
    data.append((date(2023, 1, 10), "TCS", 100.0, 150.0, 95.0, 150.0, 10000))
    
    schema = ["Date", "Stock", "Open", "High", "Low", "Close", "Volume"]
    df = spark.createDataFrame(data, schema)
    
    df = engineer_features(df)
    result = detect_anomalies(df)
    
    assert "Final_Anomaly" in result.columns
    assert "Anomaly_Type" in result.columns
