from pyspark.sql import SparkSession
from pyspark.conf import SparkConf


def get_spark_session(kwargs={}) -> SparkSession:
    return (SparkSession
        .builder
        .config(conf=build_iceberg_conf(**kwargs))
        .getOrCreate()
    )
    

def build_iceberg_conf(**kwargs):
    catalog_name = kwargs.get("catalog_name", "default")
    return (
        SparkConf()
        .setAppName("Thesis's Application")
        .set('spark.jars', f"""
            ./jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar',
            
        """)
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .set(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog')
        .set(f'spark.sql.catalog.{catalog_name}.type', 'hadoop')
        .set(f'spark.sql.catalog.{catalog_name}.warehouse', '/data')
        # .set(f'spark.sql.catalog.{catalog_name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    )
