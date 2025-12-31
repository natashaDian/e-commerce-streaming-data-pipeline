import json
import os
import time
import uuid
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic_orders = os.getenv("TOPIC_ORDERS")

#kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

df = pd.read_csv("dataset/olist_orders_dataset.csv")

#mapping event type
status_to_event = {
    "created" : "order_created",
    "approved" : "order_approved",
    "processing" : "order_processing",
    "shipped" : "order_shipped",
    "delivered" : "order_delivered",
    "canceled" : "order_failed",
    "unavailable" : "order_failed",
    "invoiced" : "order_invoiced",
}

#mapping event times
status_to_timestamp = {
    "created" : "order_purchase_timestamp",
    "approved" : "order_approved_at",
    "processing" : "order_approved_at",
    "shipped" : "order_delivered_carrier_date",
    "delivered" : "order_delivered_customer_date",
    "canceled" : "order_purchase_timestamp",
    "unavailable" : "order_purchase_timestamp",
    "invoiced" : "order_approved_at",
}

for _, row in df.iterrows():
    status = row["order_status"]
    timestamp_col = status_to_timestamp.get(status)
    event_time = row[timestamp_col]

    if pd.isna(event_time):
        continue

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": status_to_event.get(status, "order_unknown"),
        "order_id": row["order_id"], #partition key
        "customer_id": row["customer_id"],
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

print("Orders streaming replay finished.")

