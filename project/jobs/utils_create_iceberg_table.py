from project.utils import get_spark_session

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


if __name__ == '__main__':
    spark = get_spark_session({
        'catalog_name': 'optimus'
    })

    # Create raw table on Iceberg
    spark.sql(raw_tbl.format(
        catalog_name="optimus",
        schema_name="raw",
        table_name="transaction_log"
    )).show()
