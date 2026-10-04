# Flink SQL Watermarks Exercise — Local Edition

A local (Docker-based) re-creation of the Confluent Developer course exercise
["Hands-on with watermarks"](https://developer.confluent.io/courses/flink-sql/watermarks-exercise/),
using **Apache Flink 1.20.5** (latest 1.x LTS patch) and **Apache Kafka 3.9** instead of Confluent Cloud.

> **Visual primer:** open [`docs/watermark-offset-comparison.html`](docs/watermark-offset-comparison.html)
> in a browser. It is an animated, side-by-side explanation of why `earliest-offset` hides late
> events (backlog replay → the watermark lags) while `latest-offset` reveals them.

## What's in the box

| Component | Purpose |
|---|---|
| `docker-compose.yml` | Kafka (KRaft), Flink jobmanager + taskmanager, SQL client, click producer |
| `Dockerfile` | Flink 1.20.5 image + Kafka SQL connector jar |
| `producer/click_producer.py` | ~50 click events/sec to `clicks`, with Kafka record timestamps = event time |
| `sql/00_init.sql` | Auto-loaded: creates the `examples` catalog + `marketplace` database and the `clicks` table |
| `sql/01_default_watermarks.sql` | Sort on event time, `current_watermark()`, sort-on-non-time-attribute error |
| `sql/02_some_clicks_idleness.sql` | 2-partition topic with one empty partition → idleness experiments |
| `sql/03_late_events.sql` | Out-of-order events, tight watermark, counting late events (CTE + CASE) |

## Quick start

```bash
docker compose up -d --build          # kafka, flink, producer (NOT sql-client)
docker compose run --rm sql-client    # opens the Flink SQL CLI
```

- Flink Web UI: http://localhost:8081
- Kafka is also reachable from your host at `localhost:9092`.
- The `clicks` table is already created for you (via `-i /sql/00_init.sql`), and
  the course's fully-qualified name `examples.marketplace.clicks` resolves
  as-is. An `examples` catalog + `marketplace` database (backed by Flink's
  built-in `generic_in_memory` catalog) is created at startup, and
  `examples.marketplace` is set as the session default.
- In the SQL client, switch result modes with `SET 'execution.runtime-mode' = 'streaming';`
  (streaming is the default) and view a running job in the Web UI at :8081.
- To run many statements at once: `sql-client.sh embedded -i /sql/00_init.sql -f /sql/01_default_watermarks.sql`
  — or just paste statements one by one (recommended for learning).

## Confluent Cloud → vanilla Flink translation

The exercise relies on several Confluent-specific features. Here is how each maps:

| Confluent Cloud | This local setup |
|---|---|
| `$rowtime` hidden column mapped to the Kafka record timestamp | Your own `event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` column |
| Default `WATERMARK AS SOURCE_WATERMARK()` on `$rowtime` | `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` (vanilla Flink's Kafka source does not emit source watermarks, so we define a bounded-out-of-orderness strategy) |
| `distributed by hash(partition_key) into 2 buckets` | Topic created with 2 partitions + `key.fields` = `partition_key` and a fixed key, so one partition stays empty |
| Progressive idleness (short timeout ramping to 5 min) | Fixed `table.exec.source.idle-timeout` (default 60 s; experiment with `SET 'table.exec.source.idle-timeout' = '10 s';`) |
| `SET 'sql.tables.scan.idle-timeout'` | `SET 'table.exec.source.idle-timeout'` |
| `confluent flink statement list / delete` | Stop jobs from the Flink Web UI at http://localhost:8081, or `CANCEL JOB '<job-id>'` in the SQL client |
| `examples.marketplace.clicks` (course data) | Same identifier works locally: the `examples`/`marketplace` catalog + database wrap a `clicks` table fed by `producer/click_producer.py` (~50 events/s, record timestamps = event time) |

Everything else — `current_watermark()`, `order by` on a time attribute, CTEs,
`CASE`, `RAND_INTEGER`, metadata columns for reading **and writing** the Kafka
record timestamp — works the same in vanilla Flink SQL.

## Suggested path through the exercise

1. **Default watermarks** (`sql/01_default_watermarks.sql`): `DESCRIBE EXTENDED
   examples.marketplace.clicks`, sort by `event_time`, try sorting by
   `click_id` (error), watch
   `current_watermark()` stay NULL at first, then jump every ~200 ms.
2. **Idleness** (`sql/02_some_clicks_idleness.sql`): copy 500 clicks into the
   2-partition `some_clicks` topic with a fixed key (one partition stays empty).
   Sort it — notice the delay before results appear. Then change
   `table.exec.source.idle-timeout` and re-run. Try `0 ms` to see queries hang.
3. **Late events** (`sql/03_late_events.sql`): build `ooo_clicks` with up to 5 s
   of out-of-order timestamps but only 1 s of allowed lateness. Run the CTE and
   CASE-based counting queries several times; observe the non-determinism and
   why fewer events are late than expected (periodic watermarks + fast catch-up
   on historic data).

Note: the exercise's `current_watermark()` section on *updating tables* also
applies locally — `current_watermark()` is non-deterministic, so it can't be
used in queries that process update (changelog) messages.

## Cleanup

```bash
docker compose down -v      # stops everything, removes topics
```

## Troubleshooting

- **No rows from any query**: check the producer is running
  (`docker compose logs producer`) and that you waited a few seconds.
- **SQL client cannot connect**: make sure `jobmanager` and `taskmanager` are
  healthy (`docker compose ps`) and that you started the client with
  `docker compose run --rm sql-client`.
- **Connector errors** (`Could not find any factory ...`): the Docker build
  downloads `flink-sql-connector-kafka-3.4.0-1.20.jar`; rebuild with
  `docker compose build`.
- **Reuse this environment for the other course exercises** (window
  aggregations, JOINs, MATCH_RECOGNIZE...): the `clicks` table is a drop-in
  replacement for `examples.marketplace.clicks`.
