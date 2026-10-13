# Testing

## Test Suite
The project uses `pytest` for unit and integration testing.

- `tests/test_features.py`: Mocks a DataFrame and validates that Moving Averages, Volatility, and Returns are computed accurately.
- `tests/test_anomaly_detection.py`: Creates a synthetic price spike and checks if the anomaly detection engine successfully catches it and tags it correctly.
- `tests/test_pipeline.py`: End-to-end test loading real raw CSVs, executing the full preprocessing and feature engineering pipeline to assert that transformations succeed without schema errors or null pointers.

## Execution
Run tests locally via:
```bash
pytest
```
All 3 unit/integration tests passed.
