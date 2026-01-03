from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

items_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("frequency", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("event_time", TimestampType(), True)
])