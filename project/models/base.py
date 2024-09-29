from attrs import define, field, validators
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType


@define
class Base:
    @staticmethod
    def raw_spark_schema():
        return StructType([
            StructField("value", StringType()),
            StructField("tf_etl_timestamp", TimestampType()),
            StructField("tf_partition_date", StringType())
        ])

    @staticmethod
    def get_table(**kwargs):
        return f"`{kwargs.get('catalog_name')}`.`{kwargs.get('schema_name')}`.`{kwargs.get('table_name')}`"

    @staticmethod
    def table():
        raise NotImplementedError