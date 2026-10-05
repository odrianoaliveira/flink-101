-- 04_over_windows.sql
-- Mirrors the "OVER windows" section.
-- Report price changes per product using LAG. Filters to product_id = '1000'
-- to keep the output readable (change the filter to see other products).

SELECT
  event_time,
  product_id,
  price,
  LAG(price, 1) OVER w AS previous_price,
  price - LAG(price, 1) OVER w AS diff
FROM examples.marketplace.orders
WHERE product_id = '1000'
WINDOW w AS (
  PARTITION BY product_id
  ORDER BY event_time
);

-- Use COALESCE to treat a missing previous price as 0 instead of NULL:
SELECT
  event_time,
  product_id,
  price,
  LAG(price, 1) OVER w AS previous_price,
  price - COALESCE(LAG(price, 1) OVER w, 0) AS diff
FROM examples.marketplace.orders
WHERE product_id = '1000'
WINDOW w AS (
  PARTITION BY product_id
  ORDER BY event_time
);
