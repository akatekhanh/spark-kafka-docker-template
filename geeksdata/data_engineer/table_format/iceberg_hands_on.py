
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from datetime import datetime
from pyspark.sql import Row


from infra import get_spark_session

# Constant
CATALOG_NAME = 'optimus'
SCHEMA_NAME = 'bronze'
TABLE_NAME = 'transaction_log'

# Initialize Spark with Iceberg Configuration, including Jar files and Catalog name
# NOTE: This config is used for Hadoop local file. If you want to use Iceberg in Object Storage, let's read the official document
def build_iceberg_conf(kwargs={}):
    catalog_name = kwargs.get("catalog_name", CATALOG_NAME)
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


'''
    Part 1: Create Iceberg table in Local environment
'''
# This is the DDL for Demo table
iceberg_tbl = """
CREATE TABLE IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{table_name}`
    ( 
        value string,
        tf_etl_timestamp timestamp,
        tf_partition_date string
    )
USING iceberg
PARTITIONED BY (`tf_partition_date`);
"""

# spark.sql(iceberg_tbl.format(
#         catalog_name=CATALOG_NAME,
#         schema_name=SCHEMA_NAME,
#         table_name=TABLE_NAME
#     )).show()


'''
    Part 2: Insert, Upsert, Delete in Iceberg Table
'''
# Insert
data = [
    Row(value="A", tf_etl_timestamp=datetime.now(), tf_partition_date="2025-05-01"),
    Row(value="B", tf_etl_timestamp=datetime.now(), tf_partition_date="2025-05-01")
]
df = spark.createDataFrame(data)
df.writeTo("optimus.bronze.transaction_log").append()

# Update
spark.sql(f'''
    UPDATE optimus.bronze.transaction_log
    SET value = 'A_updated'
    WHERE value = 'A';
''')