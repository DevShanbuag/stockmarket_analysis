# Viva QA

**Q: Why use PySpark instead of Pandas?**
A: PySpark is built for distributed data processing. While Pandas operates in-memory on a single machine, PySpark distributes data across worker nodes, making it scalable for multi-terabyte financial datasets.

**Q: How does the anomaly detection logic work?**
A: We use a rolling Z-score. For price, we take a 30-day window of daily returns. We compute the mean and standard deviation. If today's return deviates by more than 3 standard deviations, it's flagged as an anomaly. The same logic applies to trading volume with a 20-day window.

**Q: Why store the output in Parquet instead of CSV?**
A: Parquet is a columnar storage format that is highly optimized for analytical queries. It enforces schema, compresses data efficiently, and allows downstream tools (like our Streamlit dashboard) to query only necessary columns, significantly speeding up I/O.

**Q: What happens if a stock has missing data for a day?**
A: The preprocessing pipeline explicitly drops rows with nulls in critical columns (Close, Volume) and deduplicates entries. The PySpark window functions compute moving averages strictly on available data points, ensuring robustness.
