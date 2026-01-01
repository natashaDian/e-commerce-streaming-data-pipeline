import os
from pyspark.sql import SparkSession

from streams.orders import build_orders_stream
from streams.payments import build_payments_stream
from streams.items import build_items_stream

from schemas.orders_schema import orders_schema
from schemas.payments_schema import payments_schema
from schemas.items_schema import items_schema


def main():
    # ===== Spark Session =====
    spark = (
        SparkSession.builder
        .appName("streaming-pipeline")
        .getOrCreate()
    )

    # ===== ENV =====
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic_orders = os.getenv("TOPIC_ORDERS")
    topic_payments = os.getenv("TOPIC_ORDER_PAYMENTS")
    topic_items = os.getenv("TOPIC_ORDER_ITEMS")

    # ===== Build Streams =====
    orders_df = build_orders_stream(
        spark, kafka_bootstrap, topic_orders, orders_schema
    )

    payments_df = build_payments_stream(
        spark, kafka_bootstrap, topic_payments, payments_schema
    )

    items_df = build_items_stream(
        spark, kafka_bootstrap, topic_items, items_schema
    )

    # ===== (TEMP) Output for sanity check =====
    orders_query = (
        orders_df.writeStream
        .format("console")
        .option("truncate", False)
        .start()
    )

    payments_query = (
        payments_df.writeStream
        .format("console")
        .option("truncate", False)
        .start()
    )

    items_query = (
        items_df.writeStream
        .format("console")
        .option("truncate", False)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
