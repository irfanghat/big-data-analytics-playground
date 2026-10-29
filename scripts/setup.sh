#!/bin/bash


###########################################
# Download the official image
###########################################
docker pull apache/hive:4.0.0

export HIVE_VERSION=4.0.0


###########################################
# Run Hive Server
###########################################
# docker run -d -p 10000:10000 -p 10002:10002 --env SERVICE_NAME=hiveserver2 --name hive4 apache/hive:${HIVE_VERSION}
