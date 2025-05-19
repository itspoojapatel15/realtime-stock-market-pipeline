{{ config(materialized='view') }}

select
    symbol,
    price,
    volume,
    timestamp as event_timestamp,
    price_zscore,
    volume_zscore,
    price_mean,
    price_std,
    volume_mean,
    volume_std,
    severity,
    anomaly_type,
    detected_at,
    DATE(timestamp) as anomaly_date
from {{ source('stock_raw', 'anomalies') }}
