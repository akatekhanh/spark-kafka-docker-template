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
        return (
            spark
            .readStream
            .format(self._type)
            .options(**self._conf.as_dict())
            .load()
        )