"""Click event producer — local stand-in for the Confluent `examples.marketplace.clicks` stream.

Produces ~50 JSON click events/second to the `clicks` topic, with the Kafka
record timestamp set to the current wall-clock time (event time).
"""
import json
import os
import random
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
RATE = float(os.environ.get("RATE_PER_SECOND", "50"))

URLS = [
    "/signup",
    "/pricing",
    "/docs",
    "/blog/agentic-ai",
    "/blog/flink-watermarks",
    "/checkout",
    "/account",
    "/download",
    "/support",
    "/careers",
]

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def deliver(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


interval = 1.0 / RATE
next_time = time.time()
n = 0
while True:
    event = {
        "click_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "url": random.choice(URLS),
    }
    # Kafka record timestamp = event time (this becomes event_time via
    # METADATA FROM 'timestamp', the local equivalent of Confluent's $rowtime)
    producer.produce(
        "clicks",
        key=str(event["user_id"]),
        value=json.dumps(event),
        timestamp=int(time.time() * 1000),
        callback=deliver,
    )
    n += 1
    producer.poll(0)
    if n % 500 == 0:
        producer.flush()
        print(f"produced {n} clicks")
    next_time += interval
    sleep_for = next_time - time.time()
    if sleep_for > 0:
        time.sleep(sleep_for)
