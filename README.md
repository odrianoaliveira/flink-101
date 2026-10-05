# Flink 101

A local, Docker-based port of the Confluent Developer course
["Apache Flink® SQL"](https://developer.confluent.io/courses/flink-sql/overview/).

The original course runs every exercise on **Confluent Cloud** (managed Flink +
managed Kafka, plus a few Confluent-specific SQL conveniences). This project
ports those exercises to a **local environment** using vanilla
[Apache Flink](https://flink.apache.org/) and
[Apache Kafka](https://kafka.apache.org/) in Docker, so everything runs offline
on your machine with no cloud account or billing.

Each ported exercise lives in its own directory with its own `docker-compose.yml`,
`Dockerfile`, producer, SQL scripts, and README. Nothing is shared between them.

## Course exercise → local port

The course has five hands-on exercises. Two are ported here so far:

| Course exercise | Local directory | Flink | Kafka | Status |
|---|---|---|---|---|
| Time and watermarks | [`hands-on-with-watermarks`](./hands-on-with-watermarks) | 1.20.5 | 3.9 | ✅ Ported |
| Streaming analytics | [`streaming-analytics`](./streaming-analytics) | 2.2.1 | 3.9 | ✅ Ported |
| Getting started with Confluent Cloud | — | — | — | Not yet ported |
| MATCH_RECOGNIZE | — | — | — | Not yet ported |
| Stream enrichment | — | — | — | Not yet ported |

> **Note:** both ported exercises bind the same host ports (Flink Web UI `:8081`,
> Kafka `:9092`), so they cannot run at the same time. Run
> `docker compose down -v` in one before starting the other.

## Why a local port?

Confluent Cloud provides a managed Flink runtime, managed Kafka, and some
Confluent-specific SQL conveniences (e.g. `$rowtime`, source watermarks,
`DESCRIPTOR($rowtime)`). Running the same exercises locally on vanilla
Apache Flink + Kafka means:

- No cloud account or billing — everything runs in Docker.
- You see the mechanics Confluent abstracts away (watermark definitions, Kafka
  record timestamps, connector configuration, the SQL client).
- Each exercise's README includes a **Confluent Cloud → vanilla Flink**
  translation table explaining exactly what changes.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with the Compose plugin
- A few GB of free RAM (Kafka + Flink jobmanager/taskmanager + producer)

## Quick start

Each exercise is self-contained:

```bash
cd <exercise>                      # e.g. hands-on-with-watermarks
docker compose up -d --build       # kafka, flink, producer (NOT sql-client)
docker compose run --rm sql-client # interactive Flink SQL CLI
```

- Flink Web UI: http://localhost:8081
- Kafka broker: `localhost:9092`

See each exercise's README for its SQL scripts, the Confluent → vanilla Flink
translation table, and a suggested learning path.

## Repository layout

```
.
├── hands-on-with-watermarks/   # "Time and watermarks" exercise (Flink 1.20.5)
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── producer/               # click_producer.py
│   ├── sql/                    # 00_init.sql + exercise scripts
│   └── README.md
└── streaming-analytics/        # "Streaming analytics" exercise (Flink 2.2.1)
    ├── docker-compose.yml
    ├── Dockerfile
    ├── producer/               # order_producer.py
    ├── sql/                    # 00_init.sql + exercise scripts
    └── README.md
```

## Notes

- Each `sql/00_init.sql` creates an `examples` catalog + `marketplace` database
  on Flink's built-in `generic_in_memory` catalog, so the course's
  fully-qualified names (`examples.marketplace.clicks`,
  `examples.marketplace.orders`) resolve unchanged.
- The watermarks exercise pins Flink **1.20.5** (latest 1.x LTS), while
  `streaming-analytics` uses Flink **2.2.1** to demonstrate 2.x window-TVF syntax.
