# This makefile is used for automating init process of staring docker environment

lesson_1:
	python src/1_kafka_producer_comsumer/consumer.py
	python src/1_kafka_producer_comsumer/producer.py


# Infrastructure
up:
	./run.sh up

up-spark:
	./run.sh up-spark

down:
	./run.sh down

# Geeksdata

## Data Engineer - Tabular
hudi:
	spark-submit \
	--packages org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.0 \
	--conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' \
	--conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog' \
	--conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'\
	geeksdata/data_engineer/table_format/hudi_hands_on.py 

iceberg:
	spark-submit --jars /home/jovyan/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar  geeksdata/data_engineer/table_format/iceberg_hands_on.py 

deltalake:
	spark-submit --jars /home/jovyan/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.6.1.jar  geeksdata/data_engineer/table_format/iceberg_hands_on.py 