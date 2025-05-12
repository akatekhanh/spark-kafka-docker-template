
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from .infra import get_spark_session


# Initialize Spark with Iceberg Configuration, including Jar files and Catalog name
# NOTE: This config is used for Hadoop local file. If you want to use Iceberg in Object Storage, let's read the official document
def build_iceberg_conf(kwargs={}):
    catalog_name = kwargs.get("catalog_name", "optimus")
    return (
        SparkConf()
        .setAppName("Thesis's Application")
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .set(f'spark.sql.catalog.{catalog_name}', 'org.apache.iceberg.spark.SparkCatalog')
        .set(f'spark.sql.catalog.{catalog_name}.type', 'hadoop')
        .set(f'spark.sql.catalog.{catalog_name}.warehouse', '/data')
        # .set(f'spark.sql.catalog.{catalog_name}.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    )
spark = get_spark_session(build_iceberg_conf())


# This is the DDL for Demo raw table
raw_tbl = """
CREATE OR REPLACE TABLE `{catalog_name}`.`{schema_name}`.`{table_name}`
    ( 
        value string,
        tf_etl_timestamp timestamp,
        tf_partition_date string
    )
USING iceberg
PARTITIONED BY (`tf_partition_date`);
"""

spark.sql(raw_tbl.format(
        catalog_name="optimus",
        schema_name="bronze",
        table_name="transaction_log"
    )).show()