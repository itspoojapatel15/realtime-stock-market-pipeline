{{ config(materialized='ephemeral') }}

with minute_bars as (
    select * from {{ ref('stg_ohlcv_1min') }}
),
daily as (
    select
        symbol,
        trade_date,
        min(open) as day_open,
        max(high) as day_high,
        min(low) as day_low,
        max(case when trade_hour = (select max(trade_hour) from minute_bars mb2 where mb2.symbol = minute_bars.symbol and mb2.trade_date = minute_bars.trade_date) then close end) as day_close,
        sum(volume) as day_volume,
        sum(trade_count) as day_trade_count,
        avg(vwap) as day_vwap,
        stddev(close) as intraday_volatility,
        max(high) - min(low) as day_range,
        count(*) as bar_count,
        avg(bar_return_pct) as avg_bar_return
    from minute_bars
    group by symbol, trade_date
)
select
    *,
    round((day_close - day_open) / nullif(day_open, 0) * 100, 4) as daily_return_pct,
    round(day_range / nullif(day_open, 0) * 100, 4) as range_pct
from daily
