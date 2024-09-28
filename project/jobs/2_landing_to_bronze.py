from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from project.conf import KafkaSinkConf, KafkaSourceConf
from project.models.transaction_log import TransactionLog
from project.source import Source
from project.utils import get_spark_session

if __name__ == '__main__':
    spark = get_spark_session()

    source: DataFrame = Source(
        type='kafka',
        ctx=None,
        conf=KafkaSourceConf(
            bootstrap_servers="kafka:9092",
            subscribe="test"
        )
    ).read_stream(spark)

    # Transform
    df = (
        source
        .selectExp('cast(value as string)')
        .select(f.from_json(f.col('value'), TransactionLog.spark_schema()).alias('data'))
        .select("data.*")
    )