import json
import os
import time
import uuid
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic_orders = os.getenv("TOPIC_ORDER_ITEMS")

#kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

df = pd.read_csv("dataset/olist_order_items_dataset.csv")



for _, row in df.iterrows():
    event_time = row["shipping_limit_date"]

    if pd.isna(event_time):
        continue

# exclude:
# 1. freight_value
# 2. seller_id
    event = {
        "event_id": str(uuid.uuid4()),
        "frequency": row["order_item_id"],
        "product_id": row["product_id"],
        "event_type": "order_item_added",
        "order_id": row["order_id"], #partition key
        "price": row["price"],
        "event_time": event_time
    }

    producer.send(
        topic = topic_orders,
        key = row["order_id"],
        value = event
    )

    time.sleep(2)

producer.flush()
producer.close()

print("Order items streaming replay finished.")

