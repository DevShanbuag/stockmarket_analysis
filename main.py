import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(PROJECT_ROOT)

from src.download_data import download_data
from src.spark_session import get_spark_session
from src.data_loader import load_data
from src.preprocessing import preprocess_data
from src.feature_engineering import engineer_features
from src.anomaly_detection import detect_anomalies
from src.analytics import save_to_parquet, generate_exports
from src.spark_sql_analysis import run_spark_sql
from src.visualization import generate_visualizations
from config import DATA_PROCESSED_DIR

def main():
    print("Step 6: Data Download")
    download_data()
    
    print("Step 7: Start PySpark")
    spark = get_spark_session()
    
    print("Step 8: Load Data")
    df = load_data(spark)
    if not df:
        print("No data loaded. Exiting.")
        return
        
    print("Step 9: Preprocessing")
    cleaned_df = preprocess_data(df)
    
    print("Step 10: Feature Engineering")
    featured_df = engineer_features(cleaned_df)
    
    print("Step 11: Anomaly Detection")
    anomaly_df = detect_anomalies(featured_df)
    
    anomaly_df.cache()
    print("Total records after processing:", anomaly_df.count())
    
    print("Step 12: Spark SQL Analysis")
    run_spark_sql(anomaly_df, spark)
    
    print("Step 13: Save to Parquet")
    save_to_parquet(anomaly_df)
    
    pq_df = spark.read.parquet(os.path.join(DATA_PROCESSED_DIR, "stock_data.parquet"))
    print("Parquet readback count:", pq_df.count())
    
    print("Step 14: Generate Exports")
    generate_exports(anomaly_df, spark)
    
    print("Step 15: Visualizations")
    generate_visualizations(anomaly_df)
    
    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
