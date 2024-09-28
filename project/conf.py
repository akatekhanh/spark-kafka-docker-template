from attrs import define, field
from pyspark.conf import SparkConf


@define
class BaseConf:
    def as_dict(self):
        raise NotImplementedError

@define
class KafkaSinkConf(BaseConf):
    bootstrap_servers: str = field()
    topic: str = field()
    
    def as_dict(self):
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "topic": self.topic
        }

@define
class IcebergSinkConf(BaseConf):
    catalog_name: str = field()
    schema_name: str = field()
    table_name: str = field()

    @property
    def table(self):
        return f"`{self.catalog_name}`.`{self.schema_name}`.`{self.table_name}`"

# Define Source configuration
@define
class KafkaSourceConf(BaseConf):
    bootstrap_servers: str = field()
    subscribe: str = field()
    starting_offsets: str = field(default=None)
    ending_offsets: str = field(default=None)

    def as_dict(self) -> dict:
        conf = dict()
        conf.update({
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.subscribe
        })
        if self.starting_offsets:
            conf.update({"startingOffsets": self.starting_offsets})
        if self.ending_offsets:
            conf.update({"endingOffsets": self.ending_offsets})
        return conf

@define
class IcebergSourceConf(BaseConf):
    catalog_name: str = field()
    schema_name: str = field()
    table_name: str = field()
    warehouse_location: str = field()
    catalog_type: str = field()
    