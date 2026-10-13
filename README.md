# Stock Market Big Data Analytics

A PySpark-based data processing pipeline that downloads historical stock data, engineers financial features, detects price and volume anomalies, and presents the findings in a Streamlit dashboard.

## Setup
1. Create virtual environment: `python3 -m venv .venv`
2. Activate: `source .venv/bin/activate`
3. Install dependencies: `pip install -r requirements.txt` (Ensure Java is installed for PySpark)
4. Run pipeline: `python3 main.py` or `./run_pipeline.sh`
5. Start Dashboard: `streamlit run dashboard/app.py`
6. Run tests: `pytest`

## Structure
- `data/`: Raw CSVs and processed Parquet
- `src/`: Core PySpark pipeline modules
- `dashboard/`: Streamlit app
- `output/`: Visualizations and CSV exports
- `docs/`: Project documentation
- `tests/`: Pytest test suite
