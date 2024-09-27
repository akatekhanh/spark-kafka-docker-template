from typing import Optional
from attrs import define, field, validators
from pyspark.sql import DataFrame

from project.conf import IcebergSinkConf, KafkaSinkConf

@define
class Sink:
    _df: DataFrame = field()
    _type = field(
        validator=validators.in_(['kafka', 'iceberg'])
    )
    _ctx = field()
    _conf = field(
        type=Optional[KafkaSinkConf | IcebergSinkConf]
    )
    
    def write(self):
        (
            self._df
            .selectExpr("CAST(transaction_id AS STRING)", "to_json(struct(*)) AS value")
            .write
            .format(self._type)
            .options(**self._conf.as_dict())
            .save()
        )