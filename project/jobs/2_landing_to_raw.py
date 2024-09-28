
from project.conf import KafkaSinkConf
from project.utils import get_spark_session

if __name__ == '__main__':
    spark = get_spark_session()

    kafka_conf = KafkaSinkConf(
        bootstrap_servers="kafka:9092",
        topic="test"
    )