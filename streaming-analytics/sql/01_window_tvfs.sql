-- 01_window_tvfs.sql
-- Mirrors the "Understanding Window TVFs" section.
-- Each query feeds a single order into a window TVF so you can see the full
-- table each TVF returns. Predict how many rows each returns before running it.

-- TUMBLE: 1-minute tumbling window. A single event lands in exactly ONE window,
-- so this returns 1 row.
WITH one_order AS (
  SELECT * FROM examples.marketplace.orders LIMIT 1
)
SELECT customer_id, product_id, event_time, window_start, window_end, window_time
FROM
  TUMBLE(DATA => TABLE one_order,
         TIMECOL => DESCRIPTOR(event_time),
         SIZE => INTERVAL '1' MINUTE);

-- HOP: 1-minute size, 15-second slide. size/slide = 60/15 = 4 overlapping
-- windows, so a single event lands in 4 windows → 4 rows.
WITH one_order AS (
  SELECT * FROM examples.marketplace.orders LIMIT 1
)
SELECT customer_id, product_id, event_time, window_start, window_end, window_time
FROM
  HOP(DATA => TABLE one_order,
      TIMECOL => DESCRIPTOR(event_time),
      SIZE => INTERVAL '1' MINUTE,
      SLIDE => INTERVAL '15' SECONDS);

-- CUMULATE: 1-minute size, 15-second step. Cumulative windows at 15s, 30s, 45s
-- and 60s → also 4 rows, but with window boundaries that always start at the
-- same fixed offset (unlike HOP).
WITH one_order AS (
  SELECT * FROM examples.marketplace.orders LIMIT 1
)
SELECT customer_id, product_id, event_time, window_start, window_end, window_time
FROM
  CUMULATE(DATA => TABLE one_order,
           TIMECOL => DESCRIPTOR(event_time),
           SIZE => INTERVAL '1' MINUTE,
           STEP => INTERVAL '15' SECONDS);
