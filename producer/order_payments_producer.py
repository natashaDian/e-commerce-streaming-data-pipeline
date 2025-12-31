import json
import os
import time
import uuid
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
topic_orders = os.getenv("TOPIC_ORDER_PAYMENTS")

#kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

order = pd.read_csv("dataset/olist_orders_dataset.csv")
payments = pd.read_csv("dataset/olist_order_payments_dataset.csv")

order_payments = pd.merge(
    order, payments, on="order_id", how="left"
)

print(order_payments.shape)

for _, row in order_payments.iterrows():
    event_time = row["order_purchase_timestamp"]

    if pd.isna(event_time):
        continue

    event_type = "payment_success"
    if row["payment_type"] == "not_defined":
        event_type = "payment_failed"

    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": row["order_id"], #partition key
        "payment_sequential": row["payment_sequential"],
        "payment_type": row["payment_type"],
        "payment_installments": row["payment_installments"],
        "payment_value": row["payment_value"],
        "event_time": event_time,
        "event_type": event_type
    }

    producer.send(
        topic = topic_orders,
        key = row["order_id"],
        value = event
    )

    time.sleep(2)

producer.flush()
producer.close()

print("Order payments streaming replay finished.")

