-- 03_interval_join.sql — interval join between orders and clicks.
--
-- Finds orders placed by a customer within 5 minutes of that customer clicking
-- on something. The course's original:
--
--   SELECT order_id, orders.$rowtime AS order_time, clicks.$rowtime AS click_time
--   FROM orders JOIN clicks ON customer_id = user_id
--   WHERE orders.$rowtime BETWEEN clicks.$rowtime AND clicks.$rowtime + INTERVAL '5' MINUTES;
--
-- Requirements (vanilla Flink):
--   * BOTH inputs must be append-only with watermarks (orders and clicks are)
--   * the time predicate uses the event-time attribute of both sides
--
-- Note: an interval join is many-to-many within the window — one order can match
-- many clicks (and vice versa), so the result can grow quickly. Tune the
-- producer rates / window size to keep it readable.

SELECT
  o.order_id,
  o.event_time AS order_time,
  c.event_time AS click_time
FROM orders AS o
JOIN clicks AS c
  ON o.customer_id = c.user_id
 AND o.event_time BETWEEN c.event_time AND c.event_time + INTERVAL '5' MINUTE;
