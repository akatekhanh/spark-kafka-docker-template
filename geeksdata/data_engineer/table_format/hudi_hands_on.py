
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from infra import get_spark_session


# Initialize Spark with Iceberg Configuration, including Jar files and Catalog name
# NOTE: This config is used for Hadoop local file. If you want to use Iceberg in Object Storage, let's read the official document
def build_conf(kwargs={}):
    catalog_name = kwargs.get("catalog_name", "optimus")
    return (
        SparkConf()
        .setAppName("Tabular hands on application")
    )
spark = get_spark_session(build_conf())


# This is the DDL for Demo raw table
raw_tbl = """
create table if not exists hudi_table0 (
  id int, 
  name string, 
  price double
) using hudi
options (
  type = 'cow',
  primaryKey = 'id'
);
"""

spark.sql(raw_tbl.format(
        catalog_name="optimus",
        schema_name="bronze",
        table_name="transaction_log"
    )).show()