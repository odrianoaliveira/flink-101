-- 01_default_watermarks.sql
-- Mirrors the "Using the default watermarks" section of the exercise.

-- This is the course statement, verbatim (the `examples` catalog and
-- `marketplace` database are created by 00_init.sql):
--   describe extended `examples`.`marketplace`.`clicks`;
-- (note the $rowtime column with METADATA VIRTUAL / SOURCE_WATERMARK)
DESCRIBE EXTENDED examples.marketplace.clicks;

-- Sorting on the time attribute is allowed — watermarks tell the runtime
-- how much buffering is needed before emitting sorted results:
SELECT user_id, url, event_time
FROM examples.marketplace.clicks
ORDER BY event_time;

-- Sorting on a non-time attribute fails:
--   Error: Sort on a non-time-attribute field is not supported.
SELECT user_id, url, event_time
FROM examples.marketplace.clicks
ORDER BY click_id;

-- Observe the watermark behaviour:
--   * the watermark is NULL at the very beginning
--   * it stays constant for a while, then jumps forward
--     (the periodic watermark generator emits every 200 ms by default)
SELECT user_id, url, event_time, CURRENT_WATERMARK(event_time) AS wm
FROM examples.marketplace.clicks
LIMIT 500;
