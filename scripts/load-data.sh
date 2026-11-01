#!/bin/bash

docker cp datasets/UNSW-NB15.csv namenode:/tmp/UNSW-NB15.csv
docker compose exec namenode hdfs dfs -mkdir -p /data/unsw
docker compose exec namenode hdfs dfs -put -f /tmp/UNSW-NB15.csv /data/unsw/
docker compose exec namenode rm /tmp/UNSW-NB15.csv
docker compose exec namenode hdfs dfs -ls -h /data/unsw