"""
Spark Structured Streaming processor — consumes Kafka trades,
computes rolling OHLCV windows (1min, 5min, 1hr), and writes
results to BigQuery and the Kafka anomalies topic.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    first as spark_first,
    last as spark_last,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    count,
    avg,
    lit,
    current_timestamp,
    to_timestamp,
    struct,
    to_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    TimestampType,
)


KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "stock.trades"
KAFKA_ANOMALIES_TOPIC = "stock.anomalies"
CHECKPOINT_BASE = "/tmp/spark-checkpoints"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "stock_analytics"


trade_schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", LongType(), False),
        StructField("timestamp", StringType(), False),
        StructField("exchange", StringType(), True),
        StructField("conditions", StringType(), True),
        StructField("ingested_at", StringType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("StockStreamProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
        )
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Read and parse the Kafka trades stream."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw.select(from_json(col("value").cast("string"), trade_schema).alias("trade"))
        .select("trade.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .withWatermark("event_time", "30 seconds")
    )


def compute_ohlcv(trades_df, window_duration: str, slide_duration: str | None = None):
    """Compute OHLCV aggregation over a tumbling or sliding window."""
    slide = slide_duration or window_duration
    win = window(col("event_time"), window_duration, slide)

    return (
        trades_df.groupBy(col("symbol"), win.alias("time_window"))
        .agg(
            spark_first("price").alias("open"),
            spark_max("price").alias("high"),
            spark_min("price").alias("low"),
            spark_last("price").alias("close"),
            spark_sum("volume").alias("volume"),
            count("*").alias("trade_count"),
            avg("price").alias("vwap"),
        )
        .withColumn("window_start", col("time_window.start"))
        .withColumn("window_end", col("time_window.end"))
        .withColumn("window_duration", lit(window_duration))
        .withColumn("processed_at", current_timestamp())
        .drop("time_window")
    )


def write_to_bigquery(df, table_name: str, checkpoint_suffix: str):
    """Write streaming DataFrame to BigQuery."""
    return (
        df.writeStream.format("bigquery")
        .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_suffix}")
        .option("temporaryGcsBucket", f"{BQ_PROJECT}-spark-temp")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


def write_to_kafka(df, topic: str, checkpoint_suffix: str):
    """Write anomaly alerts back to Kafka."""
    return (
        df.select(
            col("symbol").alias("key"),
            to_json(struct("*")).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", topic)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_suffix}")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )


def write_to_console(df, checkpoint_suffix: str):
    """Write to console for debugging."""
    return (
        df.writeStream.format("console")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_suffix}")
        .option("truncate", "false")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    trades = read_kafka_stream(spark)

    # 1-minute OHLCV windows
    ohlcv_1min = compute_ohlcv(trades, "1 minute")
    q1 = write_to_bigquery(ohlcv_1min, "ohlcv_1min", "ohlcv-1min")

    # 5-minute OHLCV windows
    ohlcv_5min = compute_ohlcv(trades, "5 minutes")
    q2 = write_to_bigquery(ohlcv_5min, "ohlcv_5min", "ohlcv-5min")

    # 1-hour OHLCV windows
    ohlcv_1hr = compute_ohlcv(trades, "1 hour")
    q3 = write_to_bigquery(ohlcv_1hr, "ohlcv_1hr", "ohlcv-1hr")

    # Raw trades to BigQuery for batch reconciliation
    raw_trades = trades.select(
        "symbol",
        "price",
        "volume",
        "event_time",
        "exchange",
        current_timestamp().alias("loaded_at"),
    )
    q4 = write_to_bigquery(raw_trades, "trades_raw_stream", "trades-raw")

    print("All streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
