from typing import Optional
from attrs import define, field, validators
from pyspark.sql import DataFrame, SparkSession

from project.conf import KafkaSourceConf, IcebergSourceConf


@define
class Source:
    _type = field(
        validator=validators.in_(['kafka', 'iceberg'])
    )
    _ctx = field()
    _conf = field(
        type=Optional[KafkaSourceConf | IcebergSourceConf]
    )
    
    def read_stream(self, spark: SparkSession) -> DataFrame:
        raise NotImplementedError

    def read(self, spark: SparkSession) -> DataFrame:
        raise NotImplementedError

class KafkaSource(Source):
    def read_stream(self, spark: SparkSession) -> DataFrame:
        return (
            spark
            .readStream
            .format(self._type)
            .options(**self._conf.as_dict())
            .load()
        )

    def read(self, spark: SparkSession) -> DataFrame:
        return (
            spark
            .read
            .format(self._type)
            .options(**self._conf.as_dict())
            .load()
        )

class IcebergSource(Source):
    def read_stream(self, spark: SparkSession):
        return (
            spark
            .readStream
            .format(self._type)
            .options(**self._conf.as_dict())
            .load(self._conf.table)
        )

    def read(self, spark: SparkSession) -> DataFrame:
        pass