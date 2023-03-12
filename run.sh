#!bin/bash

if [[ $1 == "up" ]] ;
then
    docker-compose -f docker-compose-spark.yml up -d
    docker-compose -f docker-compose-kafka.yml up -d
else 
    docker-compose -f docker-compose-spark.yml down
    docker-compose -f docker-compose-kafka.yml down
fi