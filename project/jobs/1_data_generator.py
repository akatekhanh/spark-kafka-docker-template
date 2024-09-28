from attr import define
from project.conf import KafkaSinkConf
from project.models.transaction_log import TransactionLog
from project.sink import Sink, KafkaSink
from project.utils import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import dbldatagen as dg


if __name__ == "__main__":
    spark = get_spark_session()

    # num_rows = 10 * 1000000  # number of rows to generate
    num_rows = 10
    num_partitions = 8  # number of Spark dataframe partitions

    transaction_log = (
        dg.DataGenerator(spark, name="transaction_log_data", rows=num_rows, partitions=num_partitions)
        .withColumn("transaction_id", "int", minValue=1000, uniqueValues=10000000, random=True)
        .withColumn("customer_id", "int", minValue=1, maxValue=500, random=True)
        .withColumn("product_id", "string", prefix="p", minValue=100, maxValue=100000, distribution="normal", random=True)
        .withColumn("quantity", "int", minValue=1, maxValue=100, random=True)
        .withColumn("price", "float", minValue=1.0, maxValue=500.0, random=True)
        .withColumn("total_amount", "float", expr="quantity * price", baseColumn=["quantity", "price"])
        .withColumn("transaction_date", "timestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59", interval="1 day", random=True)
        .withColumn("payment_method", "string", values=["Credit Card", "Debit Card", "PayPal", "Bank Transfer"], random=True)
        .withColumn("shipping_address", "string", values=TransactionLog().shipping_address, random=True)
        .withColumn("billing_address", "string", expr="concat('billing: ', shipping_address)", random=True, baseColumn=['shipping_address'])
        .withColumn("status", "string", values=["Completed", "Pending", "Cancelled"], random=True)
    )

    df_flight_data = transaction_log.build(
        withStreaming=True, options={'rowsPerSecond': 1}
    )
    kafka_conf = KafkaSinkConf(
            bootstrap_servers="kafka:9092",
            topic="test",
            checkpoint_location="./checkpoint"
        )
    sink = KafkaSink(
        df=df_flight_data,
        type="kafka",
        ctx=None,
        conf=kafka_conf
    )
    df = sink.write_stream()
    spark.streams.awaitAnyTermination()

