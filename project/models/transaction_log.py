from attrs import define, field, validators

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from faker import Faker

from .base import Base

@define
class TransactionLog(Base):
    faker = Faker()
    # Define the schema for the first table

    @staticmethod
    def spark_schema() -> StructType:
        return StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", FloatType(), True),
            StructField("total_amount", FloatType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("billing_address", StringType(), True),
            StructField("status", StringType(), True)
        ])

    @property
    def shipping_address(self):
        return [self.faker.address() for _ in range(100)]

    @staticmethod
    def table():
        return "transaction_log"
