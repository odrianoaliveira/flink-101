-- 01_regular_join.sql — the WRONG way to enrich orders with customer data.
--
-- A regular (inner) join between the insert-only `orders` stream and the
-- updating `customers` table produces an UPDATING result stream: whenever a
-- customer record changes, previously-emitted rows are retracted (-U) and
-- re-emitted (+U) with the new customer data.
--
-- This is why the course says "DO NOT DO THIS" for stream enrichment:
--   * the result is an updating stream, so you cannot apply further processing
--     that requires an insert-only stream (windowing, MATCH_RECOGNIZE, ...)
--   * the runtime must keep ALL past orders in state so it can re-join them
--     against future customer updates (unbounded state growth)
--
-- Watch the retractions: the producer updates a random customer every ~5 s.
-- In the SQL client (default `table` result mode) you will see -U / +U rows.

SELECT
  o.order_id,
  o.customer_id,
  o.price,
  c.name,
  c.city,
  c.postcode
FROM orders AS o
JOIN customers AS c
  ON o.customer_id = c.customer_id;
