from attrs import define, field


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