{{ config(materialized='table', database="postgres", alias="ticker") }}

SELECT
  JSON_VALUE(message, '$.A') AS ask_quantity,
  JSON_VALUE(message, '$.B') AS bid_quantity,
  JSON_VALUE(message, '$.C') AS last_trade_time,
  JSON_VALUE(message, '$.E') AS event_time,
  JSON_VALUE(message, '$.F') AS first_trade_id,
  JSON_VALUE(message, '$.L') AS last_trade_id,
  JSON_VALUE(message, '$.O') AS open_time,
  JSON_VALUE(message, '$.P') AS price_change_percent,
  JSON_VALUE(message, '$.Q') AS last_trade_qty,
  JSON_VALUE(message, '$.a') AS ask_price,
  JSON_VALUE(message, '$.b') AS bid_price,
  JSON_VALUE(message, '$.c') AS close_price,
  JSON_VALUE(message, '$.e') AS event_type,
  JSON_VALUE(message, '$.h') AS high_price,
  JSON_VALUE(message, '$.l') AS low_price,
  JSON_VALUE(message, '$.n') AS total_trades,
  JSON_VALUE(message, '$.o') AS open_price,
  JSON_VALUE(message, '$.p') AS price_change,
  JSON_VALUE(message, '$.q') AS quote_volume,
  JSON_VALUE(message, '$.s') AS symbol,
  JSON_VALUE(message, '$.v') AS base_volume,
  JSON_VALUE(message, '$.w') AS weighted_avg_price,
  JSON_VALUE(message, '$.x') AS prev_close_price
FROM
  `public.kafka_messages`;
