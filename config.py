import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

STOCKS = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "ICICIBANK.NS"
]

DEFAULT_PERIOD = "5y"

ANOMALY_Z_THRESHOLD = 3.0

RETURN_WINDOW = 30

VOLATILITY_WINDOW = 20

MA_SHORT_WINDOW = 20

MA_LONG_WINDOW = 50

VOLUME_WINDOW = 20

DATA_RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DATA_PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
DATA_METADATA_DIR = os.path.join(PROJECT_ROOT, "data", "metadata")
OUTPUT_CHARTS_DIR = os.path.join(PROJECT_ROOT, "output", "charts")
OUTPUT_EXPORTS_DIR = os.path.join(PROJECT_ROOT, "output", "exports")
OUTPUT_REPORTS_DIR = os.path.join(PROJECT_ROOT, "output", "reports")
