{{ config(materialized='view') }}

select
    symbol,
    window_start,
    window_end,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap,
    DATE(window_start) as trade_date,
    EXTRACT(HOUR FROM window_start) as trade_hour,
    round((close - open) / nullif(open, 0) * 100, 4) as bar_return_pct,
    high - low as price_range,
    processed_at
from {{ source('stock_raw', 'ohlcv_1min') }}
