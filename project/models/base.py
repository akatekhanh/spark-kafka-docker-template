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