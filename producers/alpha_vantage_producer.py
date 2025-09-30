"""
Alpha Vantage REST API producer — polls intraday prices into Kafka.

Fetches 1-minute interval data via Alpha Vantage TIME_SERIES_INTRADAY
endpoint, deduplicates against previously seen timestamps, and publishes
new trades to the Kafka stock.trades topic. Designed as a fallback/supplement
to the Polygon.io WebSocket producer.
"""

import json
import time
import signal
from datetime import datetime, timezone

import httpx
import structlog
from confluent_kafka import Producer
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings

logger = structlog.get_logger()

_running = True
BASE_URL = "https://www.alphavantage.co/query"


def _signal_handler(sig, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def create_kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": "alpha-vantage-producer",
            "acks": "all",
            "retries": 3,
            "compression.type": "snappy",
        }
    )


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, max=30))
def fetch_intraday(client: httpx.Client, symbol: str) -> dict:
    """Fetch 1-minute intraday data for a symbol."""
    response = client.get(
        BASE_URL,
        params={
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "1min",
            "apikey": settings.alpha_vantage.api_key,
            "outputsize": "compact",
            "datatype": "json",
        },
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()

    if "Error Message" in data:
        raise ValueError(f"API error for {symbol}: {data['Error Message']}")
    if "Note" in data:
        raise ValueError(f"Rate limited: {data['Note']}")

    return data


def parse_intraday_response(symbol: str, data: dict) -> list[dict]:
    """Parse Alpha Vantage intraday response into trade records."""
    time_series = data.get("Time Series (1min)", {})
    trades = []

    for ts_str, values in time_series.items():
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)

        trades.append(
            {
                "symbol": symbol,
                "price": float(values["4. close"]),
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "volume": int(values["5. volume"]),
                "timestamp": dt.isoformat(),
                "exchange": "alpha_vantage",
                "conditions": "[]",
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        )

    return sorted(trades, key=lambda t: t["timestamp"])


def run_producer():
    """Poll Alpha Vantage API and publish to Kafka."""
    producer = create_kafka_producer()
    symbols = settings.symbols_list
    topic = settings.kafka.topic_trades
    seen_timestamps: dict[str, set[str]] = {s: set() for s in symbols}
    total_published = 0

    logger.info("starting_alpha_vantage_producer", symbols=symbols)

    with httpx.Client() as client:
        while _running:
            for symbol in symbols:
                if not _running:
                    break

                try:
                    data = fetch_intraday(client, symbol)
                    trades = parse_intraday_response(symbol, data)

                    new_trades = [
                        t
                        for t in trades
                        if t["timestamp"] not in seen_timestamps[symbol]
                    ]

                    for trade in new_trades:
                        producer.produce(
                            topic=topic,
                            key=symbol.encode("utf-8"),
                            value=json.dumps(trade).encode("utf-8"),
                        )
                        seen_timestamps[symbol].add(trade["timestamp"])
                        total_published += 1

                    producer.poll(0)
                    logger.info(
                        "symbol_fetched",
                        symbol=symbol,
                        new_trades=len(new_trades),
                        total=total_published,
                    )

                except Exception as e:
                    logger.error("fetch_error", symbol=symbol, error=str(e))

                # Alpha Vantage free tier: 5 calls/min
                time.sleep(12)

            logger.info("polling_cycle_complete", total_published=total_published)
            time.sleep(60)

    producer.flush(timeout=10)
    logger.info("producer_shutdown", total_published=total_published)


if __name__ == "__main__":
    run_producer()
