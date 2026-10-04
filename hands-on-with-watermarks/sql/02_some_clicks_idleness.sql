-- 02_some_clicks_idleness.sql
-- Mirrors the "Idleness" section: copy 500 clicks into a new table backed by a
-- 2-partition topic where one partition is always empty.

-- Confluent: `distributed by hash(partition_key) into 2 buckets`.
-- Locally: the `some_clicks` topic was created with 2 partitions, and
-- key.fields + a fixed key route every record to the same partition,
-- leaving the other partition empty.
CREATE TABLE examples.marketplace.some_clicks_sink (
  partition_key INT,
  user_id       INT,
  url           STRING,
  event_time    TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'some_clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json',
  'key.format' = 'json',
  'key.fields' = 'partition_key',
  'sink.delivery-guarantee' = 'at-least-once'
);

-- Copy 500 clicks, all with partition_key = 1 (empty-partition setup)
INSERT INTO examples.marketplace.some_clicks_sink
SELECT
  1 AS partition_key,
  user_id,
  url,
  event_time
FROM examples.marketplace.clicks
LIMIT 500;

-- Read the copied data back. Confluent: `$rowtime`; here: event_time.
CREATE TABLE examples.marketplace.some_clicks (
  partition_key INT,
  user_id       INT,
  url           STRING,
  event_time    TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'some_clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'sql-course-some-clicks',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Q: Why does this take a while to produce results?
-- A: The empty partition holds the watermark back until the idle
--    timeout marks it idle.
SELECT
  user_id,
  url,
  event_time,
  CURRENT_WATERMARK(event_time) AS wm
FROM examples.marketplace.some_clicks
ORDER BY event_time;

-- Experiment with the idle timeout (Confluent uses the alias
-- 'sql.tables.scan.idle-timeout'; the vanilla Flink option is below).
-- Confluent's "progressive idleness" does not exist in vanilla Flink —
-- the timeout is fixed.
SET 'table.exec.source.idle-timeout' = '10 s';

SELECT
  user_id,
  url,
  event_time,
  CURRENT_WATERMARK(event_time) AS wm
FROM examples.marketplace.some_clicks
ORDER BY event_time;

-- Setting it to 0 disables idleness — queries that depend on watermarks
-- will then hang forever while a partition is empty:
--   SET 'table.exec.source.idle-timeout' = '0 ms';
-- (reset afterwards: SET 'table.exec.source.idle-timeout' = '60 s';)
