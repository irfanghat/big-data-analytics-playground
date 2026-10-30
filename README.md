## Setup

Create an `.env` & a `haddop.env` file from the provided templates (`.env.template`, `hadoop.env.template`)

`.env` contents:

```.env
HIVE_VERSION=3.1.3
SPARK_VERSION=3.5.3
HADOOP_VERSION=3.3.6
```

`hadoop.env` contents:

```.env
CORE-SITE.XML_fs.defaultFS=hdfs://namenode:8020
HDFS-SITE.XML_dfs.namenode.rpc-address=namenode:8020
HDFS-SITE.XML_dfs.replication=1
HDFS-SITE.XML_dfs.permissions.enabled=false
```

Pull the official images (At the root directory (_big-data-analytics_)):

```sh
docker compose pull
```

This pulls the offical images.

```sh
# Start all services (Spark, Hadoop, & Hive)
docker compose up
```

