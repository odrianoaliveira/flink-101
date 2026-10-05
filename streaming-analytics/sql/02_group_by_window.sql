-- 02_group_by_window.sql
-- Mirrors the "Using these TVFs for windowing" section.
-- Two queries that differ ONLY in the GROUP BY clause. The first is recognized
-- as a window aggregation (insert-only output, state cleared per window); the
-- second runs as a normal aggregation (updating stream, unbounded state).

-- GROUP BY window_start, window_end → window aggregation
WITH one_thousand_orders AS (
  SELECT * FROM examples.marketplace.orders LIMIT 1000
)
SELECT window_start, COUNT(*) AS order_count
FROM TUMBLE(DATA => TABLE one_thousand_orders,
            TIMECOL => DESCRIPTOR(event_time),
            SIZE => INTERVAL '5' SECOND)
GROUP BY window_start, window_end;

-- GROUP BY window_start only → normal aggregation (updating stream)
WITH one_thousand_orders AS (
  SELECT * FROM examples.marketplace.orders LIMIT 1000
)
SELECT window_start, COUNT(*) AS order_count
FROM TUMBLE(DATA => TABLE one_thousand_orders,
            TIMECOL => DESCRIPTOR(event_time),
            SIZE => INTERVAL '5' SECOND)
GROUP BY window_start;
