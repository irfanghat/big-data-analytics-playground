#!/bin/bash

docker compose exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n hive \
  -e "SELECT attack_cat, COUNT(*) FROM unsw.flows GROUP BY attack_cat;"


docker compose exec spark-master /opt/spark/bin/spark-sql -e "SELECT attack_cat, COUNT(*) FROM unsw.flows GROUP BY attack_cat ORDER BY 2 DESC;"