{{ config(
    materialized='incremental',
    partition_by={"field": "trade_date", "data_type": "date", "granularity": "month"},
    cluster_by=["symbol"],
    unique_key=["symbol", "trade_date"]
) }}

with daily_stats as (
    select * from {{ ref('int_symbol_daily_stats') }}
),
anomaly_stats as (
    select * from {{ ref('int_anomaly_summary') }}
)
select
    d.symbol,
    d.trade_date,
    d.day_open,
    d.day_high,
    d.day_low,
    d.day_close,
    d.day_volume,
    d.day_trade_count,
    d.day_vwap,
    d.daily_return_pct,
    d.intraday_volatility,
    d.range_pct,
    d.bar_count,
    coalesce(a.anomaly_count, 0) as anomaly_count,
    coalesce(a.extreme_count, 0) as extreme_anomalies,
    coalesce(a.critical_count, 0) as critical_anomalies,
    coalesce(a.max_price_zscore, 0) as max_zscore,
    case
        when d.daily_return_pct > 2 then 'strong_up'
        when d.daily_return_pct > 0 then 'up'
        when d.daily_return_pct > -2 then 'down'
        else 'strong_down'
    end as trend_direction,
    case
        when d.intraday_volatility > d.day_vwap * 0.02 then 'high'
        when d.intraday_volatility > d.day_vwap * 0.01 then 'medium'
        else 'low'
    end as volatility_regime
from daily_stats d
left join anomaly_stats a
    on d.symbol = a.symbol and d.trade_date = a.anomaly_date
{% if is_incremental() %}
where d.trade_date > (select max(trade_date) from {{ this }})
{% endif %}
