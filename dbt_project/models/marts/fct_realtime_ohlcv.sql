{{ config(
    materialized='incremental',
    partition_by={"field": "trade_date", "data_type": "date", "granularity": "day"},
    cluster_by=["symbol"],
    unique_key=["symbol", "window_start"]
) }}

with bars as (
    select * from {{ ref('stg_ohlcv_1min') }}
),
with_moving_avg as (
    select
        *,
        avg(close) over (
            partition by symbol
            order by window_start
            rows between 19 preceding and current row
        ) as sma_20,
        avg(close) over (
            partition by symbol
            order by window_start
            rows between 49 preceding and current row
        ) as sma_50,
        avg(volume) over (
            partition by symbol
            order by window_start
            rows between 9 preceding and current row
        ) as volume_sma_10,
        lag(close) over (partition by symbol order by window_start) as prev_close,
        row_number() over (
            partition by symbol, trade_date
            order by window_start
        ) as bar_sequence
    from bars
)
select
    symbol,
    trade_date,
    trade_hour,
    window_start,
    window_end,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    vwap,
    bar_return_pct,
    price_range,
    round(sma_20, 4) as sma_20,
    round(sma_50, 4) as sma_50,
    round(volume_sma_10, 0) as volume_sma_10,
    case
        when close > sma_20 and sma_20 > sma_50 then 'bullish'
        when close < sma_20 and sma_20 < sma_50 then 'bearish'
        else 'neutral'
    end as trend_signal,
    case
        when volume > volume_sma_10 * 2 then 'high'
        when volume > volume_sma_10 * 1.5 then 'elevated'
        else 'normal'
    end as volume_signal,
    bar_sequence
from with_moving_avg
{% if is_incremental() %}
where trade_date >= (select max(trade_date) from {{ this }})
{% endif %}
