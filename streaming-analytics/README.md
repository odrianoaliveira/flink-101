# Flink SQL Streaming Analytics Exercise — Local Edition

A local (Docker-based) re-creation of the Confluent Developer course exercise
["Streaming analytics"](https://developer.confluent.io/courses/flink-sql/streaming-analytics-exercise/),
using **Apache Flink 2.2.1** and **Apache Kafka 3.9** instead of Confluent Cloud.

This exercise covers window table-valued functions (Window TVFs), why you must
GROUP BY both `window_start` and `window_end`, the `window_time` column, OVER
windows (with `LAG` and `COALESCE`), and deduplication.

## What's in the box

| Component | Purpose |
|---|---|
| `docker-compose.yml` | Kafka (KRaft), Flink jobmanager + taskmanager, SQL client, order producer |
| `Dockerfile` | Flink 2.2.1 image + Kafka SQL connector jar |
| `producer/order_producer.py` | ~50 order events/sec to `orders`, with Kafka record timestamps = event time |
| `sql/00_init.sql` | Auto-loaded: creates the `examples` catalog + `marketplace` database and the `orders` table |
| `sql/01_window_tvfs.sql` | TUMBLE / HOP / CUMULATE on a single order — predict the row counts |
| `sql/02_group_by_window.sql` | GROUP BY `window_start, window_end` vs `window_start` alone |
| `sql/03_window_time.sql` | Tumble → OVER window on `window_time`, with `LAG` |
| `sql/04_over_windows.sql` | `LAG` + `COALESCE` to detect price changes per product |
| `sql/05_deduplication.sql` | `ROW_NUMBER()` deduplication (ASC vs DESC) |

## Quick start

```bash
docker compose up -d --build          # kafka, flink, producer (NOT sql-client)
docker compose run --rm sql-client    # opens the Flink SQL CLI
```

- Flink Web UI: http://localhost:8081
- Kafka is also reachable from your host at `localhost:9092`.
- The `orders` table is already created for you (via `-i /sql/00_init.sql`), and
  the course's fully-qualified name `examples.marketplace.orders` resolves
  as-is. An `examples` catalog + `marketplace` database (backed by Flink's
  built-in `generic_in_memory` catalog) is created at startup, and
  `examples.marketplace` is set as the session default.
- To run many statements at once: `sql-client.sh embedded -Drest.address=jobmanager -Drest.port=8081 -i /sql/00_init.sql -f /sql/01_window_tvfs.sql`
  — or just paste statements one by one (recommended for learning).
- Flink 2.x note: the embedded SQL client connects to the JobManager via the
  `-Drest.address`/`-Drest.port` session options (not `flink-conf.yaml`).

## Confluent Cloud → vanilla Flink translation

The exercise relies on several Confluent-specific features. Here is how each maps:

| Confluent Cloud | This local setup |
|---|---|
| `$rowtime` hidden column mapped to the Kafka record timestamp | Your own `event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` column |
| Default `WATERMARK AS SOURCE_WATERMARK()` on `$rowtime` | `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` (vanilla Flink's Kafka source does not emit source watermarks) |
| `DESCRIPTOR($rowtime)` | `DESCRIPTOR(event_time)` |
| `SELECT *, $rowtime` | `SELECT *` (`event_time` is already part of `*` in vanilla Flink) |
| `examples.marketplace.orders` (course data) | Same identifier works locally: the `examples`/`marketplace` catalog + database wrap an `orders` table fed by `producer/order_producer.py` (~50 events/s) |

Everything else — Window TVFs with named arguments (`TUMBLE(DATA => TABLE ...)`),
`window_start` / `window_end` / `window_time`, OVER windows, `LAG`, `COALESCE`,
and `ROW_NUMBER()` deduplication — works the same in vanilla Flink SQL. Window
TVFs use the bare `FROM TUMBLE(...)` syntax (no `TABLE(...)` wrapper), which is
valid since Flink 2.0.

## Suggested path through the exercise

1. **Window TVFs** (`sql/01_window_tvfs.sql`): run each query and check your
   prediction — TUMBLE returns 1 row, HOP returns 4 (60s/15s), CUMULATE returns 4.
   Notice how HOP and CUMULATE window boundaries differ.
2. **GROUP BY** (`sql/02_group_by_window.sql`): compare `GROUP BY window_start,
   window_end` (window aggregation, insert-only) with `GROUP BY window_start`
   (normal aggregation, updating stream).
3. **window_time** (`sql/03_window_time.sql`): count orders per second, then use
   an OVER window to diff each count against the previous one.
4. **OVER windows** (`sql/04_over_windows.sql`): watch price changes for
   `product_id = '1000'`; try `COALESCE` to replace NULL with 0.
5. **Deduplication** (`sql/05_deduplication.sql`): compare `ASC` (first order per
   customer) with `DESC` (most recent order per customer).

## Cleanup

```bash
docker compose down -v      # stops everything, removes topics
```

## Troubleshooting

- **No rows from any query**: check the producer is running
  (`docker compose logs producer`) and that you waited a few seconds for
  watermarks to advance.
- **SQL client cannot connect**: make sure `jobmanager` and `taskmanager` are
  healthy (`docker compose ps`) and that you started the client with
  `docker compose run --rm sql-client`.
- **Connector errors** (`Could not find any factory ...`): the Docker build
  downloads `flink-sql-connector-kafka-5.0.0-2.2.jar`; rebuild with
  `docker compose build`.
- **`product_id = '1000'` returns few rows**: the producer generates product IDs
  from 1000 to 1100 (uniformly), so `'1000'` appears roughly every 2 seconds.
  Change the filter or the producer ranges to suit.
