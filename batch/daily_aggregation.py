"""
Daily batch aggregation — computes authoritative daily OHLCV summaries
from raw trade data in BigQuery. Part of the Lambda architecture's
batch layer, providing the "source of truth" that reconciles against
streaming results.
"""

from datetime import date, timedelta

import structlog
from google.cloud import bigquery

from config.settings import settings

logger = structlog.get_logger()


def compute_daily_summary(target_date: date | None = None):
    """Compute and store daily OHLCV summary from raw trades."""
    if target_date is None:
        target_date = date.today() - timedelta(days=1)

    client = bigquery.Client(project=settings.bigquery.project_id)
    ds = settings.bigquery.dataset
    date_str = target_date.isoformat()

    logger.info("computing_daily_summary", date=date_str)

    query = f"""
    CREATE OR REPLACE TABLE `{ds}.daily_summary_batch`
    PARTITION BY trade_date
    CLUSTER BY symbol
    AS
    WITH ranked_trades AS (
        SELECT
            symbol,
            price,
            volume,
            event_time,
            DATE(event_time) as trade_date,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, DATE(event_time)
                ORDER BY event_time ASC
            ) as rn_asc,
            ROW_NUMBER() OVER (
                PARTITION BY symbol, DATE(event_time)
                ORDER BY event_time DESC
            ) as rn_desc
        FROM `{ds}.trades_raw_stream`
        WHERE DATE(event_time) = '{date_str}'
    ),
    daily_agg AS (
        SELECT
            symbol,
            trade_date,
            MAX(CASE WHEN rn_asc = 1 THEN price END) as open_price,
            MAX(price) as high_price,
            MIN(price) as low_price,
            MAX(CASE WHEN rn_desc = 1 THEN price END) as close_price,
            SUM(volume) as total_volume,
            COUNT(*) as trade_count,
            ROUND(AVG(price), 4) as avg_price,
            ROUND(STDDEV(price), 4) as price_stddev,
            MIN(event_time) as market_open_time,
            MAX(event_time) as market_close_time,
            ROUND(
                (MAX(CASE WHEN rn_desc = 1 THEN price END)
                 - MAX(CASE WHEN rn_asc = 1 THEN price END))
                / NULLIF(MAX(CASE WHEN rn_asc = 1 THEN price END), 0) * 100,
                4
            ) as daily_return_pct
        FROM ranked_trades
        GROUP BY symbol, trade_date
    )
    SELECT
        *,
        CURRENT_TIMESTAMP() as computed_at
    FROM daily_agg
    """

    job = client.query(query)
    job.result()

    logger.info(
        "daily_summary_complete",
        date=date_str,
        rows_affected=job.num_dml_affected_rows,
    )

    return {"date": date_str, "rows": job.num_dml_affected_rows}


if __name__ == "__main__":
    result = compute_daily_summary()
    print(f"Daily summary: {result}")
