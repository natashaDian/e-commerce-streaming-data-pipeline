from pyspark.sql.types import StructType, StructField,StringType, TimestampType

orders_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_time", TimestampType(), True)
    ])