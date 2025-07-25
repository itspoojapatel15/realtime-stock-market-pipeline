"""
Polygon.io WebSocket producer — streams real-time trades into Kafka.

Connects to Polygon.io's WebSocket feed, subscribes to trade events
for configured symbols, and publishes JSON messages to the Kafka
stock.trades topic, partitioned by symbol hash for ordering guarantees.
"""

import json
import time
import signal
import sys
from datetime import datetime, timezone

import structlog
import websocket
from confluent_kafka import Producer

from config.settings import settings

logger = structlog.get_logger()

_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("shutdown_signal_received", signal=sig)
    _running = False


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def _delivery_report(err, msg):
    if err:
        logger.error("kafka_delivery_failed", error=str(err), topic=msg.topic())
    else:
        logger.debug(
            "kafka_delivered",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
        )


def create_kafka_producer() -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "client.id": "polygon-trade-producer",
            "acks": "all",
            "retries": 3,
            "linger.ms": 5,
            "batch.num.messages": 1000,
            "compression.type": "snappy",
        }
    )


def parse_trade_message(data: dict) -> dict | None:
    """Parse Polygon.io trade event into standardized format."""
    if data.get("ev") != "T":
        return None

    return {
        "symbol": data["sym"],
        "price": float(data["p"]),
        "volume": int(data["s"]),
        "timestamp": datetime.fromtimestamp(
            data["t"] / 1000, tz=timezone.utc
        ).isoformat(),
        "exchange": str(data.get("x", "")),
        "conditions": json.dumps(data.get("c", [])),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def run_producer():
    """Main producer loop — connect to WebSocket, publish to Kafka."""
    producer = create_kafka_producer()
    symbols = settings.symbols_list
    topic = settings.kafka.topic_trades

    logger.info("starting_polygon_producer", symbols=symbols, topic=topic)

    trade_count = 0
    last_log_time = time.time()

    def on_message(ws, message):
        nonlocal trade_count, last_log_time

        for event in json.loads(message):
            trade = parse_trade_message(event)
            if not trade:
                continue

            producer.produce(
                topic=topic,
                key=trade["symbol"].encode("utf-8"),
                value=json.dumps(trade).encode("utf-8"),
                callback=_delivery_report,
            )
            trade_count += 1

        producer.poll(0)

        now = time.time()
        if now - last_log_time >= 10:
            logger.info("producer_stats", trades_published=trade_count)
            last_log_time = now

    def on_open(ws):
        logger.info("websocket_connected")
        auth_msg = {"action": "auth", "params": settings.polygon.api_key}
        ws.send(json.dumps(auth_msg))

        sub_msg = {
            "action": "subscribe",
            "params": ",".join(f"T.{s}" for s in symbols),
        }
        ws.send(json.dumps(sub_msg))
        logger.info("subscribed_to_trades", symbols=symbols)

    def on_error(ws, error):
        logger.error("websocket_error", error=str(error))

    def on_close(ws, close_status_code, close_msg):
        logger.info(
            "websocket_closed", status=close_status_code, message=close_msg
        )

    ws = websocket.WebSocketApp(
        settings.polygon.ws_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    while _running:
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10)
            if _running:
                logger.warning("websocket_disconnected_reconnecting")
                time.sleep(5)
        except Exception as e:
            logger.error("producer_error", error=str(e))
            time.sleep(5)

    producer.flush(timeout=10)
    logger.info("producer_shutdown_complete", total_trades=trade_count)


if __name__ == "__main__":
    run_producer()
