from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("UNSW-HDFS-Connection-Test") \
    .getOrCreate()

path = "hdfs://namenode:8020/data/unsw/UNSW-NB15.csv"

df = spark.read.option("header", "false").csv(path)

print("\n========== HDFS CONNECTION TEST ==========")
print("HDFS connection: SUCCESS")
print("Number of columns:", len(df.columns))
print("First record:")
df.show(1, truncate=False)

spark.stop()
