from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

payments_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", DoubleType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("event_type", StringType(), True)
])
