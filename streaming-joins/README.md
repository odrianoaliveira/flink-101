# Flink SQL Streaming Joins Exercise — Local Edition

A local (Docker-based) re-creation of the Confluent Developer course module
["Streaming JOINs in Flink SQL"](https://developer.confluent.io/courses/flink-sql/streaming-joins/),
using **Apache Flink 2.2.1** and **Apache Kafka 3.9** instead of Confluent Cloud.

This exercise covers why a regular (inner) join is the wrong tool for stream
enrichment, and the two joins you should reach for instead: **temporal joins**
(`FOR SYSTEM_TIME AS OF`) and **interval joins**.

## What's in the box

| Component | Purpose |
|---|---|
| `docker-compose.yml` | Kafka (KRaft), Flink jobmanager + taskmanager, SQL client, join producer |
| `Dockerfile` | Flink 2.2.1 image + Kafka SQL connector jar |
| `producer/join_producer.py` | Emits `customers` (with periodic updates), `orders`, and `clicks` |
| `sql/00_init.sql` | Auto-loaded: creates `customers`, `orders`, `clicks` tables |
| `sql/01_regular_join.sql` | Regular inner join (the anti-pattern → updating stream) |
| `sql/02_temporal_join.sql` | `FOR SYSTEM_TIME AS OF` temporal join (insert-only enrichment) |
| `sql/03_interval_join.sql` | Interval join between `orders` and `clicks` |

## Quick start

```bash
docker compose up -d --build          # kafka, flink, producer (NOT sql-client)
docker compose run --rm sql-client    # opens the Flink SQL CLI
```

- Flink Web UI: http://localhost:8081
- Kafka is also reachable from your host at `localhost:9092`.
- The tables are already created for you (via `-i /sql/00_init.sql`), and the
  course's fully-qualified names `examples.marketplace.customers` /
  `examples.marketplace.orders` / `examples.marketplace.clicks` resolve as-is.
- The producer emits ~10 orders/s and ~10 clicks/s, and updates a random
  customer every ~5 s (so you can watch the temporal join react to changes).
- To run many statements at once: `sql-client.sh embedded -Drest.address=jobmanager -Drest.port=8081 -i /sql/00_init.sql -f /sql/02_temporal_join.sql`
  — or just paste statements one by one (recommended for learning).
- Flink 2.x note: the embedded SQL client connects to the JobManager via the
  `-Drest.address`/`-Drest.port` session options (not `flink-conf.yaml`).

## Confluent Cloud → vanilla Flink translation

The module relies on several Confluent-specific features. Here is how each maps:

| Confluent Cloud | This local setup |
|---|---|
| `customers` as a managed versioned table (`PRIMARY KEY ... NOT ENFORCED`) | `upsert-kafka` connector on a **compacted** `customers` topic; the Kafka message key (`customer_id`) is the primary key |
| Versioned table's implicit `$rowtime` (drives event-time temporal joins) | `update_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` + `WATERMARK FOR update_time AS update_time` on `customers` |
| `$rowtime` hidden column mapped to the Kafka record timestamp | Your own `event_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` column |
| Default `WATERMARK AS SOURCE_WATERMARK()` on `$rowtime` | `WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND` |
| `FOR SYSTEM_TIME AS OF orders.order_time` | `FOR SYSTEM_TIME AS OF o.event_time` |
| `orders.$rowtime` / `clicks.$rowtime` | `o.event_time` / `c.event_time` |

Everything else — the `FOR SYSTEM_TIME AS OF` syntax, interval joins with
`BETWEEN ... AND ... + INTERVAL`, and the insert-only vs updating-stream
distinction — works the same in vanilla Flink SQL.

## Suggested path through the exercise

1. **Regular join** (`sql/01_regular_join.sql`): run it and watch the result.
   Because `customers` is an updating table, the output is an *updating* stream —
   you'll see `-U`/`+U` retractions whenever the producer updates a customer
   (~every 5 s). This is why the course says *do not do this* for enrichment.
2. **Temporal join** (`sql/02_temporal_join.sql`): the same enrichment, done
   right. Each order is joined to the customer record that was current *at the
   order's event time*, and the output is insert-only (no retractions).
3. **Interval join** (`sql/03_interval_join.sql`): orders within 5 minutes of a
   click. Both inputs are append-only with watermarks, so the join is bounded by
   the interval. Note that it is many-to-many — one order can match several
   clicks.

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
  downloads `flink-sql-connector-kafka-5.0.0-2.2.jar`; rebuild with
  `docker compose build`.
- **`customers` table empty in the temporal join**: the `customers` topic must be
  compacted (the `kafka-init` service creates it with `cleanup.policy=compact`).
  If you recreated it manually, recreate with the compact flag.
- **Temporal join returns no rows until watermarks advance**: the `orders`
  table defines a 5-second watermark; wait a few seconds after the producer
  starts.
