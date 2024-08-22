#!/bin/bash
if [[ $1 == "up" ]] ;
then
    docker-compose -f infrastructure/standalone/docker-compose-kafka.yml up -d
    docker-compose -f infrastructure/standalone/docker-compose-spark.yml up -d
else 
    docker-compose -f infrastructure/standalone/docker-compose-kafka.yml down
    docker-compose -f infrastructure/standalone/docker-compose-spark.yml down
fi