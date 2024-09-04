from pyspark.sql import SparkSession

spark: SparkSession = SparkSession \
    .builder \
    .appName("Demo pyspark") \
    .getOrCreate()


if __name__ == "__main__":
    # Extract - read from source
    df_source = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "testnum") \
        .option("startingOffsets", "earliest") \
        .load()
    )

    # Transform
    df = (
        df_source
    )

    # Load - write into sink
    df_sink = (
        df
        .writeStream \
        .format("console") \
        .option("checkpointLocation", "checkpoint/demo_2") \
        .start()
    )

    df_sink.awaitTermination()
