# Spark and Kafka in Docker feature X
# Spark and Kafka in Docker feature y

## Spark
**Spark version: **
Spark 3.3.0 for Hadoop 3.3 with OpenJDK 8 and Scala 2.12

### Spark Standalone mode
![Spark master and Worker](assets/img/2023-03-12-00-01-13.png)
#### Spark master
To run Spark master
```bash
docker-compose up -d spark-master
```

#### Spark worker
To run Spark worker (1 or 2)
```bash
docker-compose up -d spark-worker-1
```

#### Run full Spark in your local
```bash 
docker-compose up -d
```

## Kafka

### Kafka Cluster
![Kafka and Zookeeper](assets/img/2023-03-12-00-08-43.png)
To start **Kafka**
```bash
docker-compose -f docker-compose-kafka.yml -d
```

### Kakfa Standalone mode with 1 Zookeeper, 1 Kafka and Kafka UI
*How to run it*
```bash
docker-compose -f docker-compose-kafka-standalone.yml up -d
```

*How to see the Kafka via UI*
Go to [localhost:8080](localhost:8080) to view the Kafka UI
![Kafka UI](assets/img/2023-03-12-10-05-18.png)

*View the Offset*
![](assets/img/2023-03-12-10-12-04.png)

*View the Comsumer*
