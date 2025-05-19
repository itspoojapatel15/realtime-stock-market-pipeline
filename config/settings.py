from pydantic_settings import BaseSettings
from typing import List


class PolygonSettings(BaseSettings):
    api_key: str = ""
    ws_url: str = "wss://socket.polygon.io/stocks"

    model_config = {"env_prefix": "POLYGON_"}


class AlphaVantageSettings(BaseSettings):
    api_key: str = ""

    model_config = {"env_prefix": "ALPHA_VANTAGE_"}


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = "localhost:9092"
    topic_trades: str = "stock.trades"
    topic_quotes: str = "stock.quotes"
    topic_anomalies: str = "stock.anomalies"
    schema_registry_url: str = "http://localhost:8081"

    model_config = {"env_prefix": "KAFKA_"}


class SparkSettings(BaseSettings):
    master: str = "spark://spark-master:7077"
    checkpoint_dir: str = "/tmp/spark-checkpoints"

    model_config = {"env_prefix": "SPARK_"}


class BigQuerySettings(BaseSettings):
    project_id: str = ""
    dataset: str = "stock_analytics"
    credentials_path: str = "credentials/gcp-key.json"

    model_config = {"env_prefix": "GCP_"}


class AlertSettings(BaseSettings):
    slack_webhook_url: str = ""
    zscore_threshold: float = 3.0

    model_config = {"env_prefix": "ALERT_"}


class Settings(BaseSettings):
    polygon: PolygonSettings = PolygonSettings()
    alpha_vantage: AlphaVantageSettings = AlphaVantageSettings()
    kafka: KafkaSettings = KafkaSettings()
    spark: SparkSettings = SparkSettings()
    bigquery: BigQuerySettings = BigQuerySettings()
    alerts: AlertSettings = AlertSettings()
    stock_symbols: str = "AAPL,GOOGL,MSFT,AMZN,TSLA,NVDA,META,JPM,V,JNJ"

    @property
    def symbols_list(self) -> List[str]:
        return [s.strip() for s in self.stock_symbols.split(",")]

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
