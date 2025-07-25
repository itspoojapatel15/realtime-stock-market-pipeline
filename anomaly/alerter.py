"""
Slack alerter — consumes anomaly events from Kafka and sends
formatted alerts to Slack via webhook.
"""

import json
import time

import httpx
import structlog
from confluent_kafka import Consumer

from config.settings import settings

logger = structlog.get_logger()

SEVERITY_COLORS = {
    "EXTREME": "#FF0000",
    "CRITICAL": "#FF6600",
    "WARNING": "#FFCC00",
}

SEVERITY_EMOJI = {
    "EXTREME": ":rotating_light:",
    "CRITICAL": ":warning:",
    "WARNING": ":large_yellow_circle:",
}


def format_slack_message(anomaly: dict) -> dict:
    """Format anomaly event as a Slack Block Kit message."""
    severity = anomaly["severity"]
    emoji = SEVERITY_EMOJI.get(severity, ":question:")
    color = SEVERITY_COLORS.get(severity, "#999999")

    direction = "UP" if anomaly["price_zscore"] > 0 else "DOWN"

    return {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"{emoji} {severity} Anomaly: {anomaly['symbol']}",
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f"*Price:* ${anomaly['price']:.2f} ({direction})",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Volume:* {anomaly['volume']:,}",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Price Z-Score:* {anomaly['price_zscore']:.2f}",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Volume Z-Score:* {anomaly['volume_zscore']:.2f}",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Type:* {anomaly['anomaly_type']}",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f"*Detected:* {anomaly['detected_at'][:19]}",
                            },
                        ],
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": (
                                    f"Price mean: ${anomaly['price_mean']:.2f} "
                                    f"(σ={anomaly['price_std']:.2f}) | "
                                    f"Volume mean: {anomaly['volume_mean']:,.0f} "
                                    f"(σ={anomaly['volume_std']:,.0f})"
                                ),
                            }
                        ],
                    },
                ],
            }
        ]
    }


def send_slack_alert(anomaly: dict):
    """Send anomaly alert to Slack webhook."""
    webhook_url = settings.alerts.slack_webhook_url
    if not webhook_url:
        logger.debug("slack_webhook_not_configured")
        return

    message = format_slack_message(anomaly)
    try:
        response = httpx.post(webhook_url, json=message, timeout=10)
        response.raise_for_status()
        logger.info("slack_alert_sent", symbol=anomaly["symbol"])
    except Exception as e:
        logger.error("slack_alert_failed", error=str(e))


def run_alerter():
    """Consume anomaly events and send Slack alerts."""
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": "stock-alerter",
            "auto.offset.reset": "latest",
        }
    )
    consumer.subscribe([settings.kafka.topic_anomalies])
    alert_count = 0

    logger.info("alerter_started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("consumer_error", error=msg.error())
                continue

            anomaly = json.loads(msg.value().decode("utf-8"))

            # Only alert on CRITICAL and EXTREME
            if anomaly["severity"] in ("CRITICAL", "EXTREME"):
                send_slack_alert(anomaly)
                alert_count += 1

    except KeyboardInterrupt:
        logger.info("alerter_shutdown", alerts_sent=alert_count)
    finally:
        consumer.close()


if __name__ == "__main__":
    run_alerter()
