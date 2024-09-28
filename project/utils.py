from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def get_spark_session() -> SparkSession:
    return (SparkSession
        .builder
        .appName("Thesis's Application")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
    )
    

def build_iceberg_conf(spark: SparkSession, **kwargs):
    catalog_name = kwargs.get("catalog_name", "default")
    return (
        SparkConf()
        .setAppName("Thesis's Application")
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .set(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog')
        .set(f'spark.sql.catalog.{catalog_name}.type', 'hadoop')
        .set(f'spark.sql.catalog.{catalog_name}.warehouse', 's3a://my-bucket/path/')
        .set(f'spark.sql.catalog.{catalog_name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    )