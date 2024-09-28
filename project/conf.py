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
    pass


# Define Source configuration
@define
class KafkaSourceConf(BaseConf):
    bootstrap_servers: str = field()
    subscribe: str = field()

    def as_dict(self):
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.subscribe
        }

@define
class IcebergSourceConf(BaseConf):
    catalog_name: str = field()
    catalog_type: str = field()
    schema_name: str = field()
    table_name: str = field()
    warehouse_location: str = field()
    