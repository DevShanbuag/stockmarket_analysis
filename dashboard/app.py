import streamlit as st
import sys
import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PROJECT_ROOT)

from src.spark_session import get_spark_session
from config import STOCKS, DATA_PROCESSED_DIR

# ------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------
st.set_page_config(
    page_title="Stock Market Analytics & Anomaly Detection",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Use plotly dark theme as default if desired, or just leave it for plotly.

# ------------------------------------------------------------
# PYSPARK CACHE & LOAD
# ------------------------------------------------------------
@st.cache_resource
def get_spark():
    return get_spark_session(app_name="Streamlit_BDA_Dashboard")

@st.cache_resource
def load_data(_spark):
    parquet_path = os.path.join(DATA_PROCESSED_DIR, "stock_data.parquet")
    if os.path.exists(parquet_path):
        # Load directly using Spark
        return _spark.read.parquet(parquet_path)
    return None

spark = get_spark()
raw_df = load_data(spark)

if raw_df is None:
    st.error("Processed Parquet data not found. Please run the data pipeline first.")
    st.stop()

# Cache min/max dates so we don't recalculate them every UI update
@st.cache_data
def get_date_bounds():
    bounds = raw_df.agg(F.min("Date"), F.max("Date")).collect()[0]
    return pd.to_datetime(bounds[0]).date(), pd.to_datetime(bounds[1]).date()

min_date, max_date = get_date_bounds()

# ------------------------------------------------------------
# SIDEBAR
# ------------------------------------------------------------
st.sidebar.title("Controls")

st.sidebar.markdown("---")
st.sidebar.subheader("STOCK FILTER")
selected_stock = st.sidebar.selectbox("Select Stock", ["All Stocks"] + STOCKS)

st.sidebar.markdown("---")
st.sidebar.subheader("DATE FILTER")
date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date], min_value=min_date, max_value=max_date)

st.sidebar.markdown("---")
st.sidebar.subheader("ANALYSIS FILTER")
anomaly_filter = st.sidebar.selectbox("Anomaly Type", ["All", "Normal", "Price Movement", "Volume Spike", "Price + Volume"])
show_only_anomalies = st.sidebar.checkbox("Show only anomalies", value=False)

# ------------------------------------------------------------
# HEADER
# ------------------------------------------------------------
st.title("STOCK MARKET ANALYTICS & ANOMALY DETECTION")
st.markdown("### Big Data Analytics using PySpark")
st.caption(f"{len(STOCKS)} Stocks • Historical Market Data • Spark Analytics • Statistical Anomaly Detection")
st.markdown("---")

# ------------------------------------------------------------
# DATA FILTERING (PYSPARK)
# ------------------------------------------------------------
if len(date_range) != 2:
    st.warning("Please select a valid start and end date.")
    st.stop()

start_date, end_date = date_range

# Filter logic in Spark
df_filtered = raw_df.filter((F.col("Date") >= start_date) & (F.col("Date") <= end_date))

if selected_stock != "All Stocks":
    df_filtered = df_filtered.filter(F.col("Stock") == selected_stock)

if show_only_anomalies:
    df_filtered = df_filtered.filter(F.col("Final_Anomaly") == True)
    
if anomaly_filter != "All":
    df_filtered = df_filtered.filter(F.col("Anomaly_Type") == anomaly_filter)

# Check if dataset is empty after filtering
record_count = df_filtered.count()
if record_count == 0:
    st.warning("No data found for the selected filters.")
    st.stop()

# ------------------------------------------------------------
# TOP KPI SECTION (Spark Aggregations)
# ------------------------------------------------------------
kpi_data = df_filtered.agg(
    F.avg("Close").alias("Avg_Close"),
    F.avg("Daily_Return_Percent").alias("Avg_Return"),
    F.avg("Rolling_Volatility").alias("Avg_Vol"),
    F.avg("Volume").alias("Avg_Volume"),
    F.sum(F.col("Final_Anomaly").cast("int")).alias("Total_Anomalies")
).collect()[0]

# For latest close, pick the row with max date
max_date_in_range = df_filtered.agg(F.max("Date")).collect()[0][0]
latest_closes = df_filtered.filter(F.col("Date") == max_date_in_range).agg(F.avg("Close").alias("Latest_Avg_Close")).collect()[0][0]

total_anomalies = kpi_data["Total_Anomalies"] or 0
anomaly_rate = (total_anomalies / record_count) * 100 if record_count > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Close (Avg)", f"₹{latest_closes:.2f}" if latest_closes else "N/A")
col2.metric("Average Close", f"₹{kpi_data['Avg_Close']:.2f}" if kpi_data['Avg_Close'] else "N/A")
col3.metric("Average Return", f"{kpi_data['Avg_Return']:.2f}%" if kpi_data['Avg_Return'] else "N/A")
col4.metric("Volatility", f"{kpi_data['Avg_Vol']:.2f}%" if kpi_data['Avg_Vol'] else "N/A")

col5, col6, col7, col8 = st.columns(4)
col5.metric("Total Trading Records", f"{record_count}")
col6.metric("Total Anomalies", f"{total_anomalies}")
col7.metric("Anomaly Rate", f"{anomaly_rate:.2f}%")
col8.metric("Average Volume", f"{kpi_data['Avg_Volume']:,.0f}" if kpi_data['Avg_Volume'] else "N/A")

st.markdown("---")

# ------------------------------------------------------------
# DATASET OVERVIEW
# ------------------------------------------------------------
st.header("Dataset Overview")
st.markdown(f"**Stocks**: {len(STOCKS) if selected_stock == 'All Stocks' else 1} | **Trading Records**: {record_count} | **Date Range**: {start_date} to {end_date} | **Anomalies**: {total_anomalies}")

overview_df = df_filtered.groupBy("Stock").agg(
    F.count("*").alias("Records"),
    F.avg("Close").alias("Avg Close"),
    F.avg("Volume").alias("Avg Volume"),
    F.avg("Rolling_Volatility").alias("Avg Volatility"),
    F.sum(F.col("Final_Anomaly").cast("int")).alias("Anomalies")
).toPandas()

# Formatting table
overview_df['Avg Close'] = overview_df['Avg Close'].apply(lambda x: f"₹{x:.2f}")
overview_df['Avg Volume'] = overview_df['Avg Volume'].apply(lambda x: f"{x:,.0f}")
overview_df['Avg Volatility'] = overview_df['Avg Volatility'].apply(lambda x: f"{x:.4f}")
st.dataframe(overview_df, use_container_width=True)

st.markdown("---")

# Helper function to convert Spark df to Pandas efficiently for charting
def get_chart_data(*columns):
    return df_filtered.select("Date", "Stock", *columns).orderBy("Date").toPandas()

# ------------------------------------------------------------
# PRICE ANALYSIS SECTION
# ------------------------------------------------------------
st.header("Price Trend & Moving Averages")
st.caption("Historical price analysis using PySpark-derived features")

if selected_stock != "All Stocks":
    pd_price = get_chart_data("Close", "MA_20", "MA_50")
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=pd_price["Date"], y=pd_price["Close"], mode='lines', name="Close", line=dict(color="#1f77b4", width=2)))
    fig.add_trace(go.Scatter(x=pd_price["Date"], y=pd_price["MA_20"], mode='lines', name="MA20", line=dict(color="#ff7f0e", dash='dash')))
    fig.add_trace(go.Scatter(x=pd_price["Date"], y=pd_price["MA_50"], mode='lines', name="MA50", line=dict(color="#2ca02c", dash='dash')))
    fig.update_layout(title="Price Trend & Moving Averages", xaxis_title="Date", yaxis_title="Price (₹)")
    st.plotly_chart(fig, use_container_width=True)
else:
    # All stocks normalized
    st.markdown("When viewing all stocks, prices are normalized (First observation = 100) for comparative analysis.")
    first_closes = df_filtered.groupBy("Stock").agg(F.min("Date").alias("Min_Date"))
    joined = df_filtered.join(first_closes, on="Stock")
    base_prices = joined.filter(F.col("Date") == F.col("Min_Date")).select("Stock", F.col("Close").alias("Base_Close"))
    
    norm_df = df_filtered.join(base_prices, on="Stock").withColumn("Indexed_Price", (F.col("Close") / F.col("Base_Close")) * 100)
    pd_norm = norm_df.select("Date", "Stock", "Indexed_Price").orderBy("Date").toPandas()
    
    fig = px.line(pd_norm, x="Date", y="Indexed_Price", color="Stock", title="Price Trend & Moving Averages (Normalized)")
    fig.update_layout(xaxis_title="Date", yaxis_title="Indexed Price")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# DAILY RETURNS ANALYSIS
# ------------------------------------------------------------
st.header("Daily Return Analysis")

col1, col2 = st.columns([3, 1])

pd_ret = get_chart_data("Daily_Return_Percent")
fig = px.line(pd_ret, x="Date", y="Daily_Return_Percent", color="Stock" if selected_stock == "All Stocks" else None, title="Daily Return % over Time")
fig.add_hline(y=0, line_dash="dash", line_color="red")
fig.update_layout(yaxis_title="Return %")

with col1:
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Statistics")
    stats_df = df_filtered.select(
        F.mean("Daily_Return_Percent").alias("Mean Return"),
        F.expr("percentile_approx(Daily_Return_Percent, 0.5)").alias("Median Return"),
        F.stddev("Daily_Return_Percent").alias("Std Dev"),
        F.min("Daily_Return_Percent").alias("Min Return"),
        F.max("Daily_Return_Percent").alias("Max Return")
    ).toPandas().T
    stats_df.columns = ["Value"]
    stats_df["Value"] = stats_df["Value"].apply(lambda x: f"{x:.4f}%")
    st.dataframe(stats_df, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# VOLATILITY ANALYSIS
# ------------------------------------------------------------
st.header("Rolling Volatility")
st.caption("Rolling standard deviation of normalized daily return (20-day window)")

pd_vol = get_chart_data("Rolling_Volatility")
fig = px.line(pd_vol, x="Date", y="Rolling_Volatility", color="Stock" if selected_stock == "All Stocks" else None, title="Volatility Trend")
fig.update_layout(yaxis_title="Rolling Volatility")
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# VOLUME ANALYSIS
# ------------------------------------------------------------
st.header("Trading Volume Analysis")
pd_vol_data = get_chart_data("Volume", "Average_Volume_20", "Volume_Ratio")

col_vol1, col_vol2 = st.columns(2)
with col_vol1:
    if selected_stock != "All Stocks":
        fig = go.Figure()
        fig.add_trace(go.Bar(x=pd_vol_data['Date'], y=pd_vol_data['Volume'], name="Volume", marker_color="#1f77b4"))
        fig.add_trace(go.Scatter(x=pd_vol_data['Date'], y=pd_vol_data['Average_Volume_20'], mode='lines', name="Avg Volume 20", line=dict(color="#ff7f0e", width=2)))
        fig.update_layout(title="Volume over Time", yaxis_title="Volume")
    else:
        fig = px.line(pd_vol_data, x="Date", y="Volume", color="Stock", title="Volume over Time")
    st.plotly_chart(fig, use_container_width=True)

with col_vol2:
    if selected_stock != "All Stocks":
        fig = px.line(pd_vol_data, x="Date", y="Volume_Ratio", title="Volume Ratio (Actual / 20-Day Avg)")
        fig.update_traces(line_color="#2ca02c")
    else:
        fig = px.line(pd_vol_data, x="Date", y="Volume_Ratio", color="Stock", title="Volume Ratio (Actual / 20-Day Avg)")
    fig.add_hline(y=1, line_dash="dash", line_color="red")
    fig.update_layout(yaxis_title="Ratio")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# ANOMALY ANALYSIS
# ------------------------------------------------------------
st.header("Anomaly Detection Analysis")
st.markdown(f"**Total Anomalies**: {total_anomalies} ({anomaly_rate:.2f}%)")

col_ano1, col_ano2 = st.columns(2)

with col_ano1:
    st.subheader("Anomalies by Type")
    anomaly_by_type = df_filtered.filter(F.col("Final_Anomaly") == True).groupBy("Anomaly_Type").count().toPandas()
    if not anomaly_by_type.empty:
        fig = px.bar(anomaly_by_type, x="Anomaly_Type", y="count", text="count", color="Anomaly_Type", title="Distribution of Anomaly Types")
        fig.update_traces(textposition='auto')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No anomalies to display.")

with col_ano2:
    st.subheader("Anomaly Distribution by Stock")
    anomaly_by_stock = df_filtered.filter(F.col("Final_Anomaly") == True).groupBy("Stock").count().toPandas()
    if not anomaly_by_stock.empty:
        fig = px.bar(anomaly_by_stock, x="Stock", y="count", text="count", color="Stock", title="Anomalies per Stock")
        fig.update_traces(textposition='auto')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No anomalies to display.")

st.markdown("---")

# ------------------------------------------------------------
# ANOMALY TIMELINE
# ------------------------------------------------------------
st.header("Anomaly Timeline")
pd_ano_time = get_chart_data("Close", "Final_Anomaly")

fig = go.Figure()
if selected_stock != "All Stocks":
    fig.add_trace(go.Scatter(x=pd_ano_time['Date'], y=pd_ano_time['Close'], mode='lines', name="Close Price", line=dict(color="#1f77b4")))
    anomalies = pd_ano_time[pd_ano_time['Final_Anomaly'] == True]
    fig.add_trace(go.Scatter(x=anomalies['Date'], y=anomalies['Close'], mode='markers', name='Anomaly', marker=dict(color='red', size=8, line=dict(color='white', width=1))))
else:
    for stock in pd_ano_time['Stock'].unique():
        stock_data = pd_ano_time[pd_ano_time['Stock'] == stock]
        fig.add_trace(go.Scatter(x=stock_data['Date'], y=stock_data['Close'], mode='lines', name=f"{stock} Close", line=dict(width=1.5), opacity=0.5))
        anomalies = stock_data[stock_data['Final_Anomaly'] == True]
        fig.add_trace(go.Scatter(x=anomalies['Date'], y=anomalies['Close'], mode='markers', name=f'{stock} Anomaly', marker=dict(size=6, symbol='x')))

fig.update_layout(title="Price Series with Highlighted Anomalies", xaxis_title="Date", yaxis_title="Price")
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# Z-SCORE ANALYSIS
# ------------------------------------------------------------
col_z1, col_z2 = st.columns(2)

pd_zscore = get_chart_data("Return_ZScore", "Volume_ZScore")

with col_z1:
    st.header("Return Z-Score")
    fig = px.line(pd_zscore, x="Date", y="Return_ZScore", color="Stock" if selected_stock == "All Stocks" else None, title="Return Z-Score Over Time")
    fig.add_hline(y=3, line_dash="dash", line_color="red", annotation_text="+3 Sigma")
    fig.add_hline(y=-3, line_dash="dash", line_color="red", annotation_text="-3 Sigma")
    st.plotly_chart(fig, use_container_width=True)

with col_z2:
    st.header("Volume Z-Score")
    fig = px.line(pd_zscore, x="Date", y="Volume_ZScore", color="Stock" if selected_stock == "All Stocks" else None, title="Volume Z-Score Over Time")
    fig.add_hline(y=3, line_dash="dash", line_color="red", annotation_text="+3 Sigma")
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# STATISTICAL ANALYTICS
# ------------------------------------------------------------
st.header("STATISTICAL ANALYTICS")

col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)

stat_metrics = df_filtered.select(
    F.mean("Daily_Return_Percent").alias("Mean_Return"),
    F.stddev("Daily_Return_Percent").alias("Std_Dev"),
    F.mean("Rolling_Volatility").alias("Mean_Volatility"),
    F.max(F.abs("Return_ZScore")).alias("Max_Abs_ZScore")
).collect()[0]

col_stat1.metric("Mean Daily Return", f"{stat_metrics['Mean_Return']:.4f}%" if stat_metrics['Mean_Return'] else "N/A")
col_stat2.metric("Return Standard Deviation", f"{stat_metrics['Std_Dev']:.4f}%" if stat_metrics['Std_Dev'] else "N/A")
col_stat3.metric("Mean Rolling Volatility", f"{stat_metrics['Mean_Volatility']:.4f}" if stat_metrics['Mean_Volatility'] else "N/A")
col_stat4.metric("Maximum Absolute Z-Score", f"{stat_metrics['Max_Abs_ZScore']:.4f}" if stat_metrics['Max_Abs_ZScore'] else "N/A")

st.subheader("Daily Return Distribution")
pd_daily_ret = get_chart_data("Daily_Return_Percent")
valid_ret = pd_daily_ret['Daily_Return_Percent'].dropna()

fig = px.histogram(valid_ret, nbins=50, title="Daily Return Distribution", color_discrete_sequence=['#1f77b4'])
mean_val = valid_ret.mean()
fig.add_vline(x=mean_val, line_dash="dash", line_color="red", annotation_text="Mean")
fig.update_layout(xaxis_title="Daily Return %", yaxis_title="Frequency", showlegend=False)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# ANOMALY TABLE
# ------------------------------------------------------------
st.header("Detected Anomalies")
anomaly_table_df = df_filtered.filter(F.col("Final_Anomaly") == True).select(
    "Date", "Stock", "Close", "Daily_Return_Percent", "Volume", "Return_ZScore", "Volume_ZScore", "Anomaly_Type"
).orderBy(F.abs(F.col("Return_ZScore")).desc()).toPandas()

if not anomaly_table_df.empty:
    st.dataframe(anomaly_table_df, use_container_width=True)
else:
    st.info("No anomalies detected for the selected filters.")

st.markdown("---")

# ------------------------------------------------------------
# SPARK SQL INSIGHTS
# ------------------------------------------------------------
st.header("Spark SQL Insights")
df_filtered.createOrReplaceTempView("dashboard_stock_data")

col_sql1, col_sql2 = st.columns(2)

with col_sql1:
    st.markdown("**Top Stocks by Anomaly Count**")
    sql_anomalies = spark.sql("""
        SELECT Stock, SUM(CAST(Final_Anomaly AS INT)) as Total_Anomalies 
        FROM dashboard_stock_data 
        GROUP BY Stock
        ORDER BY Total_Anomalies DESC
        LIMIT 5
    """).toPandas()
    st.dataframe(sql_anomalies, use_container_width=True)

with col_sql2:
    st.markdown("**Max Observed Volatility by Stock**")
    sql_vol = spark.sql("""
        SELECT Stock, MAX(Rolling_Volatility) as Max_Volatility
        FROM dashboard_stock_data
        GROUP BY Stock
        ORDER BY Max_Volatility DESC
        LIMIT 5
    """).toPandas()
    st.dataframe(sql_vol, use_container_width=True)

st.markdown("---")

# ------------------------------------------------------------
# BDA CONCEPT: DISTRIBUTED PROCESSING
# ------------------------------------------------------------
st.header("BDA CONCEPT: DISTRIBUTED PROCESSING")

st.markdown("""
MAP
↓
PySpark transformations
Filtering / feature calculations

↓

GROUP / SHUFFLE
↓
groupBy + Window Operations

↓

REDUCE
↓
Aggregation
count / avg / stddev / anomaly counts
""")

st.write("PySpark DataFrame operations demonstrate the distributed processing principles underlying MapReduce-style analytics.")
st.write("The current project uses PySpark rather than Hadoop MapReduce.")

