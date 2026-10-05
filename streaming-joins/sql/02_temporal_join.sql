-- 02_temporal_join.sql — the RIGHT way: a temporal join (versioned table join).
--
-- `FOR SYSTEM_TIME AS OF o.event_time` relates each order to the version of the
-- customer record that was current AT THE ORDER'S EVENT TIME. This is almost
-- always what you want for stream enrichment.
--
-- Compared to a regular join, the temporal join result is INSERT-ONLY:
--   * you can apply any further processing (windowing, MATCH_RECOGNIZE, ...)
--   * the output has watermarks
--   * the runtime does NOT have to store past orders
--
-- Requirements (vanilla Flink):
--   * the right (versioned) table must have a PRIMARY KEY — `customers` does
--   * the right (versioned) table must also have a rowtime attribute
--     (`customers.update_time` maps the Kafka record timestamp)
--   * the join condition must use that primary key
--   * the left table must have a watermark on the time column in AS OF

SELECT
  o.order_id,
  o.event_time,
  o.price,
  c.name,
  c.city,
  c.postcode
FROM orders AS o
JOIN customers FOR SYSTEM_TIME AS OF o.event_time AS c
  ON o.customer_id = c.customer_id;
