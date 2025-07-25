"""Tests for Z-score anomaly detection logic."""

import math
import pytest
from collections import deque
from dataclasses import dataclass, field

ZSCORE_LOOKBACK = 100
ANOMALY_WARNING = 2.0
ANOMALY_CRITICAL = 3.0
ANOMALY_EXTREME = 4.0


@dataclass
class RollingStats:
    prices: deque = field(default_factory=lambda: deque(maxlen=ZSCORE_LOOKBACK))
    volumes: deque = field(default_factory=lambda: deque(maxlen=ZSCORE_LOOKBACK))

    @property
    def price_mean(self):
        return sum(self.prices) / len(self.prices) if len(self.prices) > 1 else 0.0

    @property
    def price_std(self):
        if len(self.prices) < 3:
            return 0.0
        mean = self.price_mean
        return math.sqrt(sum((p - mean) ** 2 for p in self.prices) / (len(self.prices) - 1))

    @property
    def volume_mean(self):
        return sum(self.volumes) / len(self.volumes) if len(self.volumes) > 1 else 0.0

    @property
    def volume_std(self):
        if len(self.volumes) < 3:
            return 0.0
        mean = self.volume_mean
        return math.sqrt(sum((v - mean) ** 2 for v in self.volumes) / (len(self.volumes) - 1))

    def add(self, price, volume):
        self.prices.append(price)
        self.volumes.append(volume)

    def price_zscore(self, price):
        if self.price_std == 0:
            return 0.0
        return (price - self.price_mean) / self.price_std

    def volume_zscore(self, volume):
        if self.volume_std == 0:
            return 0.0
        return (volume - self.volume_mean) / self.volume_std


def classify_severity(zscore):
    abs_z = abs(zscore)
    if abs_z >= ANOMALY_EXTREME:
        return "EXTREME"
    elif abs_z >= ANOMALY_CRITICAL:
        return "CRITICAL"
    elif abs_z >= ANOMALY_WARNING:
        return "WARNING"
    return "NORMAL"


def create_anomaly_event(trade, price_zscore, volume_zscore, stats):
    threshold = 3.0
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
        "severity": severity,
        "anomaly_type": (
            "PRICE_AND_VOLUME" if price_anomaly and volume_anomaly
            else "PRICE" if price_anomaly else "VOLUME"
        ),
        "price_zscore": round(price_zscore, 4),
        "volume_zscore": round(volume_zscore, 4),
    }


class TestRollingStats:
    def test_empty_stats(self):
        stats = RollingStats()
        assert stats.price_mean == 0.0
        assert stats.price_std == 0.0
        assert stats.price_zscore(100.0) == 0.0

    def test_add_and_compute(self):
        stats = RollingStats()
        for p in [100, 101, 102, 103, 104]:
            stats.add(p, 1000)
        assert stats.price_mean == pytest.approx(102.0, abs=0.01)
        assert stats.price_std > 0

    def test_zscore_calculation(self):
        stats = RollingStats()
        for i in range(50):
            stats.add(100.0, 1000)
        assert abs(stats.price_zscore(100.0)) < 0.01

    def test_extreme_zscore(self):
        stats = RollingStats()
        for _ in range(50):
            stats.add(100.0 + (_ * 0.01), 1000)
        z = stats.price_zscore(200.0)
        assert abs(z) > 3.0


class TestClassifySeverity:
    def test_normal(self):
        assert classify_severity(1.5) == "NORMAL"

    def test_warning(self):
        assert classify_severity(2.5) == "WARNING"

    def test_critical(self):
        assert classify_severity(3.5) == "CRITICAL"

    def test_extreme(self):
        assert classify_severity(4.5) == "EXTREME"

    def test_negative_zscore(self):
        assert classify_severity(-3.5) == "CRITICAL"


class TestCreateAnomalyEvent:
    def test_no_anomaly_below_threshold(self):
        trade = {"symbol": "AAPL", "price": 150.0, "volume": 1000}
        stats = RollingStats()
        result = create_anomaly_event(trade, 1.0, 1.0, stats)
        assert result is None

    def test_price_anomaly_detected(self):
        trade = {"symbol": "AAPL", "price": 200.0, "volume": 1000}
        stats = RollingStats()
        for _ in range(50):
            stats.add(150.0, 1000)
        result = create_anomaly_event(trade, 4.0, 0.5, stats)
        assert result is not None
        assert result["anomaly_type"] == "PRICE"
        assert result["severity"] == "EXTREME"

    def test_combined_anomaly(self):
        trade = {"symbol": "TSLA", "price": 300.0, "volume": 50000}
        stats = RollingStats()
        for _ in range(50):
            stats.add(200.0, 1000)
        result = create_anomaly_event(trade, 3.5, 4.0, stats)
        assert result is not None
        assert result["anomaly_type"] == "PRICE_AND_VOLUME"
