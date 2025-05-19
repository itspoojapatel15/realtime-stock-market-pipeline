{{ config(materialized='view') }}

with raw as (
    select * from {{ source('stock_raw', 'trades_raw_stream') }}
),
deduped as (
    select
        *,
        row_number() over (
            partition by symbol, event_time
            order by loaded_at desc
        ) as rn
    from raw
)
select
    symbol,
    price,
    volume,
    event_time as trade_timestamp,
    exchange,
    DATE(event_time) as trade_date,
    EXTRACT(HOUR FROM event_time) as trade_hour,
    case
        when EXTRACT(HOUR FROM event_time) between 9 and 10 then 'market_open'
        when EXTRACT(HOUR FROM event_time) between 11 and 14 then 'midday'
        when EXTRACT(HOUR FROM event_time) between 15 and 16 then 'market_close'
        else 'extended_hours'
    end as trading_session,
    loaded_at
from deduped
where rn = 1
