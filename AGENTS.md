# AGENTS.md

Guidance for AI coding agents working in this repository.

## What this repo is

`flink-101` is a local, Docker-based port of the Confluent Developer course
["Apache Flink® SQL"](https://developer.confluent.io/courses/flink-sql/overview/).
The original course runs on **Confluent Cloud** (managed Flink + Kafka); this
repo ports its hands-on exercises to a **local environment** using vanilla
Apache Flink + Kafka. There is no shared build or shared code between exercises —
treat each directory as fully independent.

## Port status

The course has several hands-on exercises and topic modules; three are ported so far:

- `hands-on-with-watermarks/` — "Time and watermarks" exercise (Flink 1.20.5).
- `streaming-analytics/` — "Streaming analytics" exercise (Flink 2.2.1).
- `streaming-joins/` — "Streaming joins" module (Flink 2.2.1): temporal + interval joins.

Not yet ported: "Getting started with Confluent Cloud", "MATCH_RECOGNIZE",
"Stream enrichment".

When adding a new exercise, mirror the existing structure — `Dockerfile`,
`docker-compose.yml`, `producer/`, `sql/00_init.sql` + exercise scripts, and a
`README.md` with a **Confluent Cloud → vanilla Flink** translation table.

## Layout

Each exercise contains:
- `Dockerfile` — base Flink image + Kafka SQL connector jar
- `docker-compose.yml` — Kafka (KRaft), jobmanager, taskmanager, sql-client, producer
- `producer/` — Python producer (Kafka record timestamp = event time)
- `sql/` — `00_init.sql` (catalog + table DDL) + exercise scripts
- `README.md` — exercise-specific details + Confluent → vanilla translation table

## Rules and gotchas

### Flink versions differ per exercise
Do not assume one Flink version applies repo-wide. The watermarks exercise pins
Flink 1.20.5; `streaming-analytics` pins Flink 2.2.1. When bumping a version,
change the `FROM flink:<version>` line in that exercise's `Dockerfile`.

### Kafka connector version latches to the Flink minor
The connector jar in each `Dockerfile` must match the Flink minor version:
- Flink 1.20 → `flink-sql-connector-kafka-3.4.0-1.20.jar`
- Flink 2.2 → `flink-sql-connector-kafka-5.0.0-2.2.jar`

Update the connector URL together with the `FROM` image. The Maven URL pattern is
`.../org/apache/flink/flink-sql-connector-kafka/<connector-version>/flink-sql-connector-kafka-<connector-version>.jar`.

### Window TVF syntax differs between 1.x and 2.x
- Flink 2.0+: bare `FROM TUMBLE(DATA => ...)`, `HOP(...)`, `CUMULATE(...)`.
- Flink 1.x: requires the `FROM TABLE(TUMBLE(...))` wrapper.

Do not mix these. The `streaming-analytics` SQL files use bare (2.x) syntax.

### SQL client connectivity differs between 1.x and 2.x
- Flink 1.x: the SQL client connects to the JobManager via `flink-conf.yaml`
  (`rest.address` / `rest.port`).
- Flink 2.x: the embedded SQL client reads the cluster address from `-D` session
  options, e.g. `sql-client.sh embedded -Drest.address=jobmanager -Drest.port=8081`.

### Shared host ports — one exercise at a time
All exercises bind Flink Web UI `:8081` and Kafka `:9092`, so they cannot run
simultaneously. Tear down with `docker compose down -v` before switching.

### Versioned tables + temporal joins (upsert-kafka)
For event-time temporal joins (`FOR SYSTEM_TIME AS OF`), the versioned (right)
table must have BOTH a `PRIMARY KEY` and a rowtime attribute. Model a versioned
table with the `upsert-kafka` connector (bundled in the Kafka SQL connector jar)
on a **compacted** topic keyed by the PK:

- `'connector' = 'upsert-kafka'`, `'key.format' = 'raw'` (key = raw INT bytes,
  big-endian), `'value.format' = 'json'`, `'value.fields-include' = 'EXCEPT_KEY'`.
- The producer must key records by the PK and exclude the PK column from the value.
- `upsert-kafka` does **not** support `scan.startup.mode` (it reads from the
  beginning by default); setting it fails DDL validation.
- Add a rowtime attribute to the versioned table (e.g. `update_time
  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'` + `WATERMARK FOR update_time AS
  update_time`) — required for event-time temporal joins.
- Interval joins require both inputs to be append-only with watermarks, and use
  `BETWEEN ... AND ... + INTERVAL '...'` on the event-time attribute.

### Kafka is KRaft, pinned to 3.9
Kafka image is `bitnamilegacy/kafka:3.9.0`, single-node KRaft (no ZooKeeper).
Topics are created by a `kafka-init` service in each `docker-compose.yml`.

### Catalog / database naming is intentional
Each `sql/00_init.sql` creates an `examples` catalog + `marketplace` database on
Flink's built-in `generic_in_memory` catalog so that Confluent course identifiers
(`examples.marketplace.clicks`, `examples.marketplace.orders`) resolve unchanged.
Preserve this when editing DDL.

## Common commands

```bash
cd <exercise>
docker compose up -d --build        # build + start kafka, flink, producer
docker compose run --rm sql-client  # interactive Flink SQL CLI
docker compose logs -f producer     # watch producer output
docker compose down -v              # stop everything + delete topics/state
```

Flink Web UI: http://localhost:8081 · Kafka: `localhost:9092`.

## How to verify changes

1. Rebuild and start: `docker compose up -d --build`.
2. Confirm containers are healthy: `docker compose ps`.
3. Confirm the Flink version: `docker compose exec jobmanager flink --version`
   (or check the Web UI).
4. Run SQL against the client, e.g.
   `docker compose run --rm sql-client ... -i /sql/00_init.sql -f /sql/<script>.sql`
   (add `-Drest.address=jobmanager -Drest.port=8081` for Flink 2.x).
5. Verify queries return the expected rows, then tear down with
   `docker compose down -v`.

## Style

- SQL scripts are pedagogical and reference Confluent course naming; keep them
  readable and commented rather than minified.
- Keep each exercise self-contained; do not introduce cross-exercise imports.
