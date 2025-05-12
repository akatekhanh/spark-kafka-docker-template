#!/bin/bash

# Precheck docker network, volume
precheck_docker_asset() {
    docker network create standalone_net
}
precheck_docker_asset

if [[ $1 == "up" ]] ;
then
    # Up all containers
    docker-compose -f infrastructure/standalone/docker-compose-kafka.yml up -d
    docker-compose -f infrastructure/standalone/docker-compose-spark.yml up -d
    docker-compose -f infrastructure/standalone/docker-compose-minio.yml up -d

elif [[ $1 == "up-spark" ]];
then
    docker-compose -f infrastructure/standalone/docker-compose-spark.yml up -d

elif [[ $1 == "up-kafka" ]];
then
    docker-compose -f infrastructure/standalone/docker-compose-kafka.yml up -d

elif [[ $1 == "up-minio" ]];
then
    docker-compose -f infrastructure/standalone/docker-compose-minio.yml up -d

else [[ $1 == "down" ]];
    # Down all containers
    docker-compose -f infrastructure/standalone/docker-compose-kafka.yml down
    docker-compose -f infrastructure/standalone/docker-compose-spark.yml down
    docker-compose -f infrastructure/standalone/docker-compose-minio.yml down
fi