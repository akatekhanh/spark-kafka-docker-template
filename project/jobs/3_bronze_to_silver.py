from pyspark.sql import DataFrame
import pyspark.sql.functions as f

from project.conf import IcebergSourceConf, KafkaSinkConf, KafkaSourceConf, IcebergSinkConf
from project.models.transaction_log import TransactionLog
from project.sink import Sink, IcebergSink
from project.source import Source, IcebergSource
from project.utils import get_spark_session

if __name__ == '__main__':
    spark = get_spark_session({
        "catalog_name": "optimus"
    })

    source: DataFrame = IcebergSource(
        type='iceberg',
        ctx=None,
        conf=IcebergSourceConf(
            catalog_name="optimus",
            schema_name="bronze",
            table_name="transaction_log"
        )
    ).read(spark)

    # Transform
    df = (
        source
        .selectExpr('cast(value as string)')
        .select(f.from_json(f.col('value'), TransactionLog.spark_schema()).alias('data'))
        .select("data.*")
    )

    # Write to sink
    sink = IcebergSink(
        df=df,
        type='iceberg',
        ctx=None,
        conf=IcebergSinkConf(
            catalog_name="optimus",
            schema_name="silver",
            table_name="transaction_log",
            trigger="5 seconds",
            checkpoint_location="./checkpoint/silver/transaction_log"
        )
    )
    df = sink.write_stream()
    df.awaitTermination()