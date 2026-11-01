#!/bin/bah

docker compose exec hiveserver2 beeline \
  -u jdbc:hive2://localhost:10000 \
  -n hive \
  -e "
SET mapreduce.framework.name;
SET mapred.job.tracker;
SET fs.defaultFS;
SET hive.execution.engine;
"