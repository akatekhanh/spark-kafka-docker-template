from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from project.conf import KafkaSinkConf, KafkaSourceConf, IcebergSinkConf
from project.models.transaction_log import TransactionLog
from project.sink import Sink, IcebergSink
from project.source import Source
from project.utils import get_spark_session

if __name__ == '__main__':
    spark = get_spark_session({
        "catalog_name": "optimus"
    })

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
        .selectExpr(
            'cast(value as string)',
            'current_timestamp() as tf_etl_timestamp',
            "date_format(current_date(), 'yyyy-MM-dd') as tf_partition_date"
        )
    )

    # Write to sink
    sink = IcebergSink(
        df=df,
        type='iceberg',
        ctx=None,
        conf=IcebergSinkConf(
            catalog_name="optimus",
            schema_name="raw",
            table_name="transaction_log"
        )
    )
    sink.write_stream()