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
            subscribe="test",
            starting_offsets="earliest"
        )
    ).read_stream(spark)

    # Transform
    df = (
        source
        .selectExpr(
            'cast(value as string) as value',
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
            table_name="transaction_log",
            trigger="5 seconds",
            checkpoint_location="./checkpoint/landing_to_raw"
        )
    )
    df = sink.write_stream()
    df.awaitTermination()