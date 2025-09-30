"""
Daily batch reconciliation — compares streaming aggregates against
batch-computed values from raw trades to detect data quality drift.

Runs as an Airflow task to validate that the speed layer (Spark Streaming)
and batch layer (BigQuery SQL) produce consistent results.
"""

from datetime import date, timedelta

import structlog
from google.cloud import bigquery

from config.settings import settings

logger = structlog.get_logger()


def get_bq_client() -> bigquery.Client:
    return bigquery.Client(
        project=settings.bigquery.project_id,
        credentials=None,  # Uses ADC
    )


def reconcile_daily(target_date: date | None = None) -> dict:
    """
    Compare streaming vs batch aggregates for a given date.

    Returns a reconciliation report with discrepancies.
    """
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    client = get_bq_client()
    ds = settings.bigquery.dataset
    date_str = target_date.isoformat()

    logger.info("starting_reconciliation", date=date_str)

    # Batch: compute OHLCV directly from raw trades
    batch_query = f"""
    SELECT
        symbol,
        DATE(event_time) as trade_date,
        COUNT(*) as trade_count,
        ROUND(AVG(price), 4) as avg_price,
        MIN(price) as min_price,
        MAX(price) as max_price,
        SUM(volume) as total_volume,
        MIN(event_time) as first_trade,
        MAX(event_time) as last_trade
    FROM `{ds}.trades_raw_stream`
    WHERE DATE(event_time) = '{date_str}'
    GROUP BY symbol, DATE(event_time)
    ORDER BY symbol
    """

    # Stream: get pre-computed 1-minute aggregates summed up
    stream_query = f"""
    SELECT
        symbol,
        DATE(window_start) as trade_date,
        SUM(trade_count) as trade_count,
        ROUND(AVG(close), 4) as avg_price,
        MIN(low) as min_price,
        MAX(high) as max_price,
        SUM(volume) as total_volume,
        MIN(window_start) as first_window,
        MAX(window_end) as last_window
    FROM `{ds}.ohlcv_1min`
    WHERE DATE(window_start) = '{date_str}'
    GROUP BY symbol, DATE(window_start)
    ORDER BY symbol
    """

    batch_results = {
        row.symbol: dict(row) for row in client.query(batch_query).result()
    }
    stream_results = {
        row.symbol: dict(row) for row in client.query(stream_query).result()
    }

    all_symbols = set(batch_results.keys()) | set(stream_results.keys())
    discrepancies = []

    for symbol in sorted(all_symbols):
        batch = batch_results.get(symbol)
        stream = stream_results.get(symbol)

        if not batch:
            discrepancies.append(
                {"symbol": symbol, "issue": "MISSING_FROM_BATCH", "severity": "HIGH"}
            )
            continue
        if not stream:
            discrepancies.append(
                {"symbol": symbol, "issue": "MISSING_FROM_STREAM", "severity": "HIGH"}
            )
            continue

        # Check trade count difference (allow 1% tolerance)
        count_diff_pct = abs(batch["trade_count"] - stream["trade_count"]) / max(
            batch["trade_count"], 1
        )
        if count_diff_pct > 0.01:
            discrepancies.append(
                {
                    "symbol": symbol,
                    "issue": "TRADE_COUNT_MISMATCH",
                    "batch_count": batch["trade_count"],
                    "stream_count": stream["trade_count"],
                    "diff_pct": round(count_diff_pct * 100, 2),
                    "severity": "HIGH" if count_diff_pct > 0.05 else "MEDIUM",
                }
            )

        # Check volume difference
        vol_diff_pct = abs(batch["total_volume"] - stream["total_volume"]) / max(
            batch["total_volume"], 1
        )
        if vol_diff_pct > 0.01:
            discrepancies.append(
                {
                    "symbol": symbol,
                    "issue": "VOLUME_MISMATCH",
                    "batch_volume": batch["total_volume"],
                    "stream_volume": stream["total_volume"],
                    "diff_pct": round(vol_diff_pct * 100, 2),
                    "severity": "MEDIUM",
                }
            )

        # Check price range consistency
        if batch["min_price"] != stream["min_price"]:
            discrepancies.append(
                {
                    "symbol": symbol,
                    "issue": "MIN_PRICE_MISMATCH",
                    "batch": batch["min_price"],
                    "stream": stream["min_price"],
                    "severity": "LOW",
                }
            )

    report = {
        "date": date_str,
        "symbols_checked": len(all_symbols),
        "discrepancies": discrepancies,
        "status": "PASS" if len(discrepancies) == 0 else "FAIL",
        "high_severity_count": sum(
            1 for d in discrepancies if d.get("severity") == "HIGH"
        ),
    }

    logger.info(
        "reconciliation_complete",
        date=date_str,
        status=report["status"],
        discrepancies=len(discrepancies),
    )

    # Store reconciliation results
    recon_table = f"{ds}.reconciliation_log"
    rows = [
        {
            "reconciliation_date": date_str,
            "status": report["status"],
            "symbols_checked": report["symbols_checked"],
            "discrepancy_count": len(discrepancies),
            "high_severity_count": report["high_severity_count"],
            "details": str(discrepancies),
        }
    ]
    client.insert_rows_json(recon_table, rows)

    return report


if __name__ == "__main__":
    report = reconcile_daily()
    print(f"Reconciliation: {report['status']} | Discrepancies: {len(report['discrepancies'])}")
