"""
Z-score anomaly detection on streaming stock data.

Maintains rolling statistics per symbol and flags trades where price
or volume deviates beyond the configured Z-score threshold. Detected
anomalies are published to the Kafka anomalies topic and optionally
sent as Slack alerts.
"""

import json
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np
import structlog
from confluent_kafka import Consumer, Producer

from config.settings import settings
from config.constants import (
    ZSCORE_LOOKBACK,
    ANOMALY_WARNING,
    ANOMALY_CRITICAL,
    ANOMALY_EXTREME,
    CONSUMER_GROUP_ANOMALY,
)

logger = structlog.get_logger()


@dataclass
class RollingStats:
    """Maintains rolling window statistics for a single symbol."""

    prices: deque = field(default_factory=lambda: deque(maxlen=ZSCORE_LOOKBACK))
    volumes: deque = field(default_factory=lambda: deque(maxlen=ZSCORE_LOOKBACK))

    @property
    def price_mean(self) -> float:
        return float(np.mean(self.prices)) if len(self.prices) > 1 else 0.0

    @property
    def price_std(self) -> float:
        return float(np.std(self.prices, ddof=1)) if len(self.prices) > 2 else 0.0

    @property
    def volume_mean(self) -> float:
        return float(np.mean(self.volumes)) if len(self.volumes) > 1 else 0.0

    @property
    def volume_std(self) -> float:
        return float(np.std(self.volumes, ddof=1)) if len(self.volumes) > 2 else 0.0

    def add(self, price: float, volume: int):
        self.prices.append(price)
        self.volumes.append(volume)

    def price_zscore(self, price: float) -> float:
        if self.price_std == 0:
            return 0.0
        return (price - self.price_mean) / self.price_std

    def volume_zscore(self, volume: int) -> float:
        if self.volume_std == 0:
            return 0.0
        return (volume - self.volume_mean) / self.volume_std


def classify_severity(zscore: float) -> str:
    """Classify anomaly severity based on absolute Z-score."""
    abs_z = abs(zscore)
    if abs_z >= ANOMALY_EXTREME:
        return "EXTREME"
    elif abs_z >= ANOMALY_CRITICAL:
        return "CRITICAL"
    elif abs_z >= ANOMALY_WARNING:
        return "WARNING"
    return "NORMAL"


def create_anomaly_event(
    trade: dict,
    price_zscore: float,
    volume_zscore: float,
    stats: RollingStats,
) -> dict | None:
    """Create anomaly event if thresholds are exceeded."""
    threshold = settings.alerts.zscore_threshold
    price_anomaly = abs(price_zscore) >= threshold
    volume_anomaly = abs(volume_zscore) >= threshold

    if not price_anomaly and not volume_anomaly:
        return None

    max_zscore = max(abs(price_zscore), abs(volume_zscore))
    severity = classify_severity(max_zscore)

    return {
        "symbol": trade["symbol"],
        "price": trade["price"],
        "volume": trade["volume"],
        "timestamp": trade["timestamp"],
        "price_zscore": round(price_zscore, 4),
        "volume_zscore": round(volume_zscore, 4),
        "price_mean": round(stats.price_mean, 4),
        "price_std": round(stats.price_std, 4),
        "volume_mean": round(stats.volume_mean, 2),
        "volume_std": round(stats.volume_std, 2),
        "severity": severity,
        "anomaly_type": (
            "PRICE_AND_VOLUME"
            if price_anomaly and volume_anomaly
            else "PRICE" if price_anomaly else "VOLUME"
        ),
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


def run_anomaly_detector():
    """Consume trades from Kafka, detect anomalies, publish alerts."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": CONSUMER_GROUP_ANOMALY,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": "anomaly-detector",
        }
    )

    consumer.subscribe([settings.kafka.topic_trades])
    symbol_stats: dict[str, RollingStats] = {}
    anomaly_count = 0

    logger.info(
        "anomaly_detector_started",
        threshold=settings.alerts.zscore_threshold,
        lookback=ZSCORE_LOOKBACK,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("consumer_error", error=msg.error())
                continue

            trade = json.loads(msg.value().decode("utf-8"))
            symbol = trade["symbol"]

            if symbol not in symbol_stats:
                symbol_stats[symbol] = RollingStats()

            stats = symbol_stats[symbol]
            price_z = stats.price_zscore(trade["price"])
            volume_z = stats.volume_zscore(trade["volume"])

            anomaly = create_anomaly_event(trade, price_z, volume_z, stats)
            if anomaly:
                producer.produce(
                    topic=settings.kafka.topic_anomalies,
                    key=symbol.encode("utf-8"),
                    value=json.dumps(anomaly).encode("utf-8"),
                )
                anomaly_count += 1
                logger.warning(
                    "anomaly_detected",
                    symbol=symbol,
                    severity=anomaly["severity"],
                    type=anomaly["anomaly_type"],
                    price_z=anomaly["price_zscore"],
                    volume_z=anomaly["volume_zscore"],
                )

            stats.add(trade["price"], trade["volume"])
            producer.poll(0)

    except KeyboardInterrupt:
        logger.info("anomaly_detector_shutdown", total_anomalies=anomaly_count)
    finally:
        consumer.close()
        producer.flush(timeout=10)


if __name__ == "__main__":
    run_anomaly_detector()
