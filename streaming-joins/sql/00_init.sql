-- 00_init.sql — run automatically by the SQL client on startup (-i)
-- Local equivalent of the Confluent `examples.marketplace` tables used in the
-- "Streaming joins" exercise: customers (versioned), orders (insert-only),
-- clicks (insert-only).
--
-- We recreate Confluent's catalog/database namespace so the course's
-- fully-qualified identifiers resolve verbatim. The `generic_in_memory` catalog
-- is built into Flink and supports DDL.

CREATE CATALOG examples WITH ('type' = 'generic_in_memory');
CREATE DATABASE IF NOT EXISTS examples.marketplace;

-- customers: a versioned (updating) table. Each record for a given customer_id
-- replaces the previous one — this is what makes temporal joins possible.
-- Backed by a compacted Kafka topic; the upsert-kafka connector reads the Kafka
-- message key (customer_id) as the primary key.
CREATE TABLE examples.marketplace.customers (
  customer_id INT NOT NULL,
  name        STRING NOT NULL,
  address     STRING NOT NULL,
  postcode    STRING NOT NULL,
  city        STRING NOT NULL,
  email       STRING NOT NULL,
  -- When this customer version became current (Kafka record timestamp).
  -- Required by event-time temporal joins: the versioned table needs both a
  -- primary key and a rowtime attribute.
  update_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR update_time AS update_time,
  PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'customers',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'sql-course',
  'key.format' = 'raw',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY'
);

-- orders: immutable / insert-only (no primary key). `event_time` maps the Kafka
-- record timestamp — the local equivalent of Confluent's built-in `$rowtime`.
CREATE TABLE examples.marketplace.orders (
  order_id    STRING,
  customer_id INT,
  product_id  STRING,
  price       DOUBLE,
  event_time  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'sql-course',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- clicks: immutable / insert-only. `user_id` is the foreign-key counterpart of
-- orders.customer_id (used by the interval join).
CREATE TABLE examples.marketplace.clicks (
  click_id   STRING,
  user_id    INT,
  product_id STRING,
  event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
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
-- behave like they do in the course shell.
USE CATALOG examples;
USE marketplace;
