# Spark and Kafka in Docker template for testing and learning
## Spark
**Spark version:**
Spark latest version for Hadoop with OpenJDK 8 and Scala 2.12

### Spark Standalone mode
![Spark master and Worker architecture](assets/img/2023-03-12-00-01-13.png)
#### Spark Jupyter notebook
```bash
docker-compose -f infrastructure/standalone/docker-compose-spark.yml up -d
```

## Kafka
### Kakfa Standalone mode with 1 Zookeeper, 1 Kafka and Kafka UI
*How to run it*
```bash
docker-compose -f infrastructure/standalone/docker-compose-kafka.yml up -d
```

*How to see the Kafka via UI*
Go to [localhost:8080](localhost:8080) to view the Kafka UI
![Kafka UI](assets/img/2023-03-12-10-05-18.png)

*View the Offset*
![](assets/img/2023-03-12-10-12-04.png)

*View the Comsumer*
