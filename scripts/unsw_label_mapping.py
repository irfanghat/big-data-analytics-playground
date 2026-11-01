from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder.appName("UNSW-Label-Mapping").getOrCreate()

path = "hdfs://namenode:8020/data/unsw/UNSW-NB15.csv"

columns = [
"srcip","sport","dstip","dsport","proto","state","dur","sbytes","dbytes",
"sttl","dttl","sloss","dloss","service","Sload","Dload","Spkts","Dpkts",
"swin","dwin","stcpb","dtcpb","smeansz","dmeansz","trans_depth",
"res_bdy_len","Sjit","Djit","Stime","Ltime","Sintpkt","Dintpkt","tcprtt",
"synack","ackdat","is_sm_ips_ports","ct_state_ttl","ct_flw_http_mthd",
"is_ftp_login","ct_ftp_cmd","ct_srv_src","ct_srv_dst","ct_dst_ltm",
"ct_src_ltm","ct_src_dport_ltm","ct_dst_sport_ltm","ct_dst_src_ltm",
"attack_cat","label"
]

df = spark.read.option("header", "false").csv(path).toDF(*columns)

from pyspark.sql.functions import col, trim, lower, when

df = df.withColumn("attack_cat", trim(col("attack_cat")))
df = df.withColumn(
    "attack_cat",
    when(col("attack_cat").isNull() | (col("attack_cat") == ""), "Normal")
    .when(lower(col("attack_cat")).isin("backdoor", "backdoors"), "Backdoors")
    .otherwise(col("attack_cat"))
)

sample = df.sampleBy(
    "attack_cat",
    {
        "Normal": 0.00675,
        "Generic": 0.0696,
        "Exploits": 0.337,
        "Fuzzers": 0.618,
        "DoS": 0.917,
        "Reconnaissance": 1.0,
        "Analysis": 1.0,
        "Backdoors": 1.0,
        "Shellcode": 1.0,
        "Worms": 1.0
    },
    seed=42
)

indexer = StringIndexer(
    inputCol="attack_cat",
    outputCol="target",
    handleInvalid="keep"
)

model = indexer.fit(sample)

print("\nMULTICLASS LABEL MAPPING:")
for i, category in enumerate(model.labels):
    print(f"{i} = {category}")

spark.stop()
