-- 05_deduplication.sql
-- Mirrors the "Deduplication" section.
-- Deduplication is a special case of an OVER aggregation: ROW_NUMBER() ordered
-- by a time attribute, filtered to row_num = 1.

-- General form:
--   SELECT [columns] FROM (
--     SELECT [columns], ROW_NUMBER() OVER (PARTITION BY key
--       ORDER BY time_attribute ASC|DESC) AS row_num
--     FROM table)
--   WHERE row_num = 1;

-- ASC keeps the FIRST (earliest) order per customer:
SELECT *
FROM (
  SELECT *, event_time AS order_time,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time ASC) AS row_num
  FROM examples.marketplace.orders
  WHERE customer_id < 3005)
WHERE row_num = 1;

-- DESC keeps the MOST RECENT order per customer:
SELECT *
FROM (
  SELECT *, event_time AS order_time,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_time DESC) AS row_num
  FROM examples.marketplace.orders
  WHERE customer_id < 3005)
WHERE row_num = 1;
