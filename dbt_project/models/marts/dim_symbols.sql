{{ config(materialized='table') }}

with trade_stats as (
    select
        symbol,
        min(trade_date) as first_seen,
        max(trade_date) as last_seen,
        count(distinct trade_date) as active_days,
        avg(volume) as avg_daily_volume,
        avg(price) as avg_price
    from {{ ref('stg_trades') }}
    group by symbol
),
anomaly_stats as (
    select
        symbol,
        count(*) as total_anomalies,
        countif(severity = 'EXTREME') as extreme_count
    from {{ ref('stg_anomalies') }}
    group by symbol
)
select
    t.symbol,
    t.first_seen,
    t.last_seen,
    t.active_days,
    round(t.avg_daily_volume, 0) as avg_daily_volume,
    round(t.avg_price, 2) as avg_price,
    coalesce(a.total_anomalies, 0) as total_anomalies,
    coalesce(a.extreme_count, 0) as extreme_anomaly_count,
    case
        when t.avg_daily_volume > 10000000 then 'mega_cap'
        when t.avg_daily_volume > 1000000 then 'large_cap'
        when t.avg_daily_volume > 100000 then 'mid_cap'
        else 'small_cap'
    end as volume_tier
from trade_stats t
left join anomaly_stats a on t.symbol = a.symbol
