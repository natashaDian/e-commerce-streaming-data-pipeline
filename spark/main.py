import os
from pyspark.sql import SparkSession

from streams.orders_streaming import orders_stream
from streams.items_streaming import items_stream
from streams.payments_streaming import payments_stream

from schemas.orders_schema import orders_schema
from schemas.items_schemas import items_schema
from schemas.payments_schemas import payments_schema

def main():
    spark = (
        SparkSession.builder
        .appName("Ecommerce data streaming")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    #load semua hal dalam env

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    orders_topic = os.getenv("TOPIC_ORDERS")
    payments_topic = os.getenv("TOPIC_ORDER_PAYMENTS")
    items_topic = os.getenv("TOPIC_ORDER_ITEMS")
    checkpoint_base = os.getenv("SPARK_CHECKPOINT_BASE")

    #load streams
    orders_df = orders_stream(spark, kafka_bootstrap, orders_topic, orders_schema)
    items_df = items_stream(spark, kafka_bootstrap, items_topic, items_schema)
    payments_df = payments_stream(spark, kafka_bootstrap, payments_topic, payments_schema)

    #checkpoint independent untuk setiap stream
    orders_c = (orders_df.writeStream
                .format("parquet")
                .option("path", "/opt/spark-output/orders")
                .option("checkpointLocation", f"{checkpoint_base}/orders")
                .outputMode("append")
                .start()
                )
    items_c = (items_df.writeStream
               .format("parquet")
               .option("path", "/opt/spark-output/items")
               .option("checkpointLocation", f"{checkpoint_base}/items")
               .outputMode("append")
               .start()
               )
    payments_c = (payments_df.writeStream
                  .format("parquet")
                  .option("path", "/opt/spark-output/payments")
                  .option("checkpointLocation", f"{checkpoint_base}/payments")
                  .outputMode("append")
                  .start()
                  )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()