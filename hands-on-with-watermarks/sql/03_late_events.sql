-- 03_late_events.sql
-- Mirrors the "Detecting late events" section: create a table with up to
-- 5 seconds of out-of-order-ness but only 1 second of allowed lateness.

-- Sink: we write shifted timestamps INTO the Kafka record timestamp.
CREATE TABLE examples.marketplace.ooo_clicks_sink (
  user_id    INT,
  url        STRING,
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'ooo_clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'at-least-once'
);

-- Shift each event 0..5 seconds forward: timestamps become up to 5 s
-- out of order. (Confluent: rand_integer(6).)
INSERT INTO examples.marketplace.ooo_clicks_sink
SELECT
  user_id,
  url,
  TIMESTAMPADD(SECOND, RAND_INTEGER(6), event_time) AS event_time
FROM examples.marketplace.clicks;

-- Source: watermark only tolerates 1 second of out-of-order-ness.
CREATE TABLE examples.marketplace.ooo_clicks (
  user_id    INT,
  url        STRING,
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ooo_clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'sql-course-ooo-clicks',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Count late events among the first 1000, using a CTE for readability.
-- NOTE: if there are no late events, this produces NO result at all —
-- a quirk worth observing.
WITH a_thousand_clicks AS (
  SELECT * FROM examples.marketplace.ooo_clicks LIMIT 1000
)
SELECT COUNT(*)
FROM a_thousand_clicks
WHERE event_time <= CURRENT_WATERMARK(event_time);

-- Running count of total vs late events, using a CASE statement
-- (a good trick: different aggregation conditions per output field).
SELECT
  COUNT(*) AS total_events,
  SUM(
    CASE WHEN event_time <= CURRENT_WATERMARK(event_time) THEN 1
         ELSE 0
    END) AS late_events
FROM examples.marketplace.ooo_clicks;

-- Run these repeatedly: results are non-deterministic, because the
-- periodic watermark generator (every 200 ms) lets some "would-be-late"
-- events slip through. When catching up on historic data, thousands of
-- events are processed between watermarks, so far fewer events are late
-- than the 5 s vs 1 s setup would suggest.
