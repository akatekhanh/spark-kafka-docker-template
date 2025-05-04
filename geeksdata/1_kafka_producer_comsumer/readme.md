# This is the lesson 1 - Understanding how to produce and consume from/with Kafka

## To test producer in Kafka
> Produce meaning that we ingest or push data into Kafka cluster specified by Kafka Topic 

**Command to run:**
```bash
python3 src/1_kafka_producer_comsumer/producer.py
```

> Comsume meaning that we get or pul data from Kafka cluster specified by Kafka topic
- There is many options to get data from Kafka, following the documentation [Kafka](https://developer.confluent.io/tutorials/kafka-console-consumer-read-specific-offsets-partitions/confluent.html)

**Commadn to run consumer**
```bash
python3 src/1_kafka_producer_comsumer/consumer.py
```