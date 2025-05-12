from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Get Spark session with Tabular configuration
def get_spark_session(tabular_conf: SparkConf)-> SparkSession:
    return (SparkSession
        .builder
        .config(conf=tabular_conf)
        .getOrCreate()
    )