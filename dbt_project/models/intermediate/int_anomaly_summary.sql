{{ config(materialized='ephemeral') }}

with anomalies as (
    select * from {{ ref('stg_anomalies') }}
)
select
    symbol,
    anomaly_date,
    count(*) as anomaly_count,
    countif(severity = 'EXTREME') as extreme_count,
    countif(severity = 'CRITICAL') as critical_count,
    countif(severity = 'WARNING') as warning_count,
    countif(anomaly_type = 'PRICE') as price_anomalies,
    countif(anomaly_type = 'VOLUME') as volume_anomalies,
    countif(anomaly_type = 'PRICE_AND_VOLUME') as combined_anomalies,
    round(avg(abs(price_zscore)), 2) as avg_price_zscore,
    round(max(abs(price_zscore)), 2) as max_price_zscore,
    round(avg(abs(volume_zscore)), 2) as avg_volume_zscore
from anomalies
group by symbol, anomaly_date
