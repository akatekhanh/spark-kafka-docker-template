from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    return (SparkSession
        .builder
        .appName("Thesis's Application")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )