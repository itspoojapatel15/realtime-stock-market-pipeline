"""Stock market pipeline constants."""

# Window durations
WINDOW_1MIN = "1 minute"
WINDOW_5MIN = "5 minutes"
WINDOW_1HR = "1 hour"

# Watermark delay for late-arriving data
WATERMARK_DELAY = "30 seconds"

# Z-score lookback period (number of data points)
ZSCORE_LOOKBACK = 100

# Anomaly severity thresholds
ANOMALY_WARNING = 2.0
ANOMALY_CRITICAL = 3.0
ANOMALY_EXTREME = 4.0

# Kafka consumer group IDs
CONSUMER_GROUP_STREAMING = "stock-streaming-processor"
CONSUMER_GROUP_ANOMALY = "stock-anomaly-detector"

# BigQuery table names
BQ_TABLE_TRADES_RAW = "trades_raw"
BQ_TABLE_OHLCV_1MIN = "ohlcv_1min"
BQ_TABLE_OHLCV_5MIN = "ohlcv_5min"
BQ_TABLE_OHLCV_1HR = "ohlcv_1hr"
BQ_TABLE_ANOMALIES = "anomalies"
BQ_TABLE_DAILY_SUMMARY = "daily_summary"

# Trade schema fields
TRADE_SCHEMA = {
    "symbol": "STRING",
    "price": "FLOAT64",
    "volume": "INT64",
    "timestamp": "TIMESTAMP",
    "exchange": "STRING",
    "conditions": "STRING",
}
