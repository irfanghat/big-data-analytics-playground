#!/bin/bash

docker compose cp etl/load.sql spark-master:/tmp/load.sql
docker compose exec spark-master /opt/spark/bin/spark-sql -f /tmp/load.sql