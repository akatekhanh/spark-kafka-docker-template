
gen-batch:
	spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1  project/jobs/1_data_generator.py
create_table:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1 project/jobs/utils_create_iceberg_table.py
landing-to-bronze:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1  project/jobs/2_landing_to_bronze.py
