-- 03_window_time.sql
-- Mirrors the "Using the window_time column" section.
-- window_time is the time attribute of the window operator's output (it carries
-- watermarking), so you can run another temporal operation on it — here an OVER
-- window that diffs each 1-second count against the previous one.

WITH orders_per_second AS (
  SELECT window_time, COUNT(*) AS order_count
  FROM TUMBLE(DATA => TABLE examples.marketplace.orders,
              TIMECOL => DESCRIPTOR(event_time),
              SIZE => INTERVAL '1' SECOND)
  GROUP BY window_start, window_end, window_time
)
SELECT
  window_time,
  order_count,
  LAG(order_count, 1) OVER w AS previous_count,
  order_count - LAG(order_count, 1) OVER w AS diff
FROM orders_per_second
WINDOW w AS (
  ORDER BY window_time
);
