"""Order event producer — local stand-in for the Confluent `examples.marketplace.orders` stream.

Produces ~50 JSON order events/second to the `orders` topic, with the Kafka
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

# Value ranges chosen so the exercise's filters return readable results:
#   WHERE customer_id < 3005  -> customers 3000..3004 (deduplication demo)
#   WHERE product_id = '1000' -> a steady trickle of rows (OVER window demo)
CUSTOMER_MIN = 3000
CUSTOMER_MAX = 3500
PRODUCT_MIN = 1000
PRODUCT_MAX = 1100

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def deliver(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


interval = 1.0 / RATE
next_time = time.time()
n = 0
while True:
    event = {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(CUSTOMER_MIN, CUSTOMER_MAX),
        "product_id": str(random.randint(PRODUCT_MIN, PRODUCT_MAX)),
        "price": round(random.uniform(10.0, 100.0), 2),
    }
    # Kafka record timestamp = event time (this becomes event_time via
    # METADATA FROM 'timestamp', the local equivalent of Confluent's $rowtime)
    producer.produce(
        "orders",
        key=str(event["order_id"]),
        value=json.dumps(event),
        timestamp=int(time.time() * 1000),
        callback=deliver,
    )
    n += 1
    producer.poll(0)
    if n % 500 == 0:
        producer.flush()
        print(f"produced {n} orders")
    next_time += interval
    sleep_for = next_time - time.time()
    if sleep_for > 0:
        time.sleep(sleep_for)
