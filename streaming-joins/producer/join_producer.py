"""Local stand-in for the Confluent `examples.marketplace` streams used in the
"Streaming joins" exercise:

  - `customers` — versioned / updating table (each record for a key replaces the
                  previous one; the Kafka topic is compacted)
  - `orders`    — insert-only, immutable
  - `clicks`    — insert-only, immutable

Produces:
  - one customer record per customer at startup, then periodic updates (to
    demonstrate versioning for the temporal join)
  - orders and clicks at a steady rate, sharing the same customer/user id pool
    and near-identical timestamps, so the interval join finds matches.
"""
import json
import os
import random
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
ORDER_RATE = float(os.environ.get("ORDER_RATE_PER_SECOND", "10"))
CLICK_RATE = float(os.environ.get("CLICK_RATE_PER_SECOND", "10"))
UPDATE_INTERVAL = float(os.environ.get("CUSTOMER_UPDATE_SECONDS", "5"))

CUSTOMER_MIN = 3000
CUSTOMER_MAX = 3049  # 50 customers / users

CITIES = ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"]
POSTCODES = ["10115", "80331", "20095", "50667", "60311"]

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def deliver(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def produce_customer(customer_id, name, address, postcode, city, email):
    # Value excludes customer_id (matches value.fields-include='EXCEPT_KEY').
    rec = {
        "name": name,
        "address": address,
        "postcode": postcode,
        "city": city,
        "email": email,
    }
    # Key by customer_id as a raw 4-byte big-endian INT (matches key.format='raw').
    # upsert-kafka treats the Kafka message key as the primary key, so a later
    # record with the same key overwrites earlier ones (versioning).
    producer.produce(
        "customers",
        key=customer_id.to_bytes(4, "big"),
        value=json.dumps(rec),
        timestamp=int(time.time() * 1000),
        callback=deliver,
    )


# Seed all customers once.
for cid in range(CUSTOMER_MIN, CUSTOMER_MAX + 1):
    produce_customer(
        cid,
        f"Customer {cid}",
        f"{cid} Market St",
        POSTCODES[cid % len(POSTCODES)],
        CITIES[cid % len(CITIES)],
        f"customer{cid}@example.com",
    )
producer.flush()
print(f"seeded {CUSTOMER_MAX - CUSTOMER_MIN + 1} customers")

order_interval = 1.0 / ORDER_RATE
click_interval = 1.0 / CLICK_RATE
next_order = next_click = next_update = time.time()
n_orders = n_clicks = 0

while True:
    now = time.time()

    # Occasional customer update -> demonstrates versioning for the temporal join.
    if now >= next_update:
        cid = random.randint(CUSTOMER_MIN, CUSTOMER_MAX)
        produce_customer(
            cid,
            f"Customer {cid}",
            f"{cid} Market St",
            POSTCODES[random.randrange(len(POSTCODES))],
            CITIES[random.randrange(len(CITIES))],
            f"customer{cid}@example.com",
        )
        next_update = now + UPDATE_INTERVAL

    # Orders (insert-only, immutable).
    if now >= next_order:
        cid = random.randint(CUSTOMER_MIN, CUSTOMER_MAX)
        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": cid,
            "product_id": str(random.randint(1000, 1100)),
            "price": round(random.uniform(10.0, 100.0), 2),
        }
        producer.produce(
            "orders",
            key=str(order["order_id"]),
            value=json.dumps(order),
            timestamp=int(now * 1000),
            callback=deliver,
        )
        n_orders += 1
        next_order += order_interval

    # Clicks (insert-only), same id pool as customers/orders so interval joins
    # (orders.customer_id = clicks.user_id) find matches.
    if now >= next_click:
        uid = random.randint(CUSTOMER_MIN, CUSTOMER_MAX)
        click = {
            "click_id": str(uuid.uuid4()),
            "user_id": uid,
            "product_id": str(random.randint(1000, 1100)),
        }
        producer.produce(
            "clicks",
            key=str(click["click_id"]),
            value=json.dumps(click),
            timestamp=int(now * 1000),
            callback=deliver,
        )
        n_clicks += 1
        next_click += click_interval

    producer.poll(0)
    if (n_orders + n_clicks) % 1000 == 0:
        producer.flush()
        print(f"produced {n_orders} orders, {n_clicks} clicks")

    sleep_for = min(next_order, next_click, next_update) - time.time()
    if sleep_for > 0:
        time.sleep(sleep_for)
