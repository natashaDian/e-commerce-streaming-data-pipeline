from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType

def items_stream(spark, kafka_bootstrap, topic, schema):
    
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )
