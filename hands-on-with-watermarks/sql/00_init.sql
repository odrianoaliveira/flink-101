-- 00_init.sql — run automatically by the SQL client on startup (-i)
-- Local equivalent of the Confluent Cloud `examples.marketplace.clicks` table.
--
-- We recreate Confluent's catalog/database namespace so that the course's
-- fully-qualified identifier `examples.marketplace.clicks` resolves verbatim.
-- The `generic_in_memory` catalog is built into Flink (no extra jar needed)
-- and supports DDL; its tables live for the lifetime of the SQL client session.

CREATE CATALOG examples WITH ('type' = 'generic_in_memory');
CREATE DATABASE IF NOT EXISTS examples.marketplace;

CREATE TABLE examples.marketplace.clicks (
  click_id  STRING,
  user_id   INT,
  url       STRING,
  -- Confluent exposes this as the built-in `$rowtime` column.
  -- Here we map the Kafka record timestamp ourselves.
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  -- Confluent's default is WATERMARK AS `SOURCE_WATERMARK()`.
  -- Vanilla Flink's Kafka source does not emit source watermarks,
  -- so we define a bounded-out-of-orderness strategy of 5 seconds.
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'clicks',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'sql-course',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Make `examples.marketplace` the session default so unqualified table names
-- (e.g. `some_clicks`, `ooo_clicks`) behave like they do in the course shell.
USE CATALOG examples;
USE marketplace;
