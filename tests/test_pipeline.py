import pytest
from pyspark.sql import SparkSession
from src.data_loader import load_data
from src.preprocessing import preprocess_data
from src.feature_engineering import engineer_features
from src.anomaly_detection import detect_anomalies

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TestPipeline").getOrCreate()

def test_pipeline(spark):
    df = load_data(spark)
    assert df is not None
    assert df.count() > 0
    
    cleaned_df = preprocess_data(df)
    assert cleaned_df.count() <= df.count()
    
    featured_df = engineer_features(cleaned_df)
    assert "Rolling_Volatility" in featured_df.columns
    
    anomaly_df = detect_anomalies(featured_df)
    assert "Final_Anomaly" in anomaly_df.columns
