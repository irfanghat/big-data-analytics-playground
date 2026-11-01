from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, lower, count, avg, stddev,
    min as spark_min, max as spark_max,
    variance, floor
)
import math
import os

spark = SparkSession.builder \
    .appName("UNSW-NB15-Statistical-Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs://namenode:8020/data/unsw/UNSW-NB15.csv"
OUTPUT = "hdfs://namenode:8020/results/unsw"

columns = [
    "srcip", "sport", "dstip", "dsport", "proto", "state",
    "dur", "sbytes", "dbytes", "sttl", "dttl", "sloss",
    "dloss", "service", "Sload", "Dload", "Spkts", "Dpkts",
    "swin", "dwin", "stcpb", "dtcpb", "smeansz", "dmeansz",
    "trans_depth", "res_bdy_len", "Sjit", "Djit", "Stime",
    "Ltime", "Sintpkt", "Dintpkt", "tcprtt", "synack",
    "ackdat", "is_sm_ips_ports", "ct_state_ttl",
    "ct_flw_http_mthd", "is_ftp_login", "ct_ftp_cmd",
    "ct_srv_src", "ct_srv_dst", "ct_dst_ltm", "ct_src_ltm",
    "ct_src_dport_ltm", "ct_dst_sport_ltm", "ct_dst_src_ltm",
    "attack_cat", "label"
]

try:
    print("Loading UNSW-NB15 dataset...")
    df = spark.read.option("header", "false").csv(INPUT)
    df = df.toDF(*columns)

    # Clean category values and convert binary labels.
    df = df.withColumn("attack_cat", trim(col("attack_cat")))
    df = df.withColumn(
        "attack_cat",
        when(col("attack_cat").isNull() |
             (col("attack_cat") == ""), "Normal")
        .when(lower(col("attack_cat")) == "backdoor", "Backdoors")
        .when(lower(col("attack_cat")) == "backdoors", "Backdoors")
        .when(lower(col("attack_cat")) == "fuzzers", "Fuzzers")
        .when(lower(col("attack_cat")) == "reconnaissance",
              "Reconnaissance")
        .when(lower(col("attack_cat")) == "shellcode", "Shellcode")
        .otherwise(col("attack_cat"))
    )

    df = df.withColumn("label", col("label").cast("double"))

    numeric_cols = [
        "dur", "sbytes", "dbytes", "sttl", "dttl",
        "sloss", "dloss", "Sload", "Dload", "Spkts",
        "Dpkts", "smeansz", "dmeansz", "Sintpkt",
        "Dintpkt", "tcprtt", "synack", "ackdat", "label"
    ]

    for c in numeric_cols:
        df = df.withColumn(c, col(c).cast("double"))

    total = df.count()
    print("\nTOTAL RECORDS:", f"{total:,}")

    # 1. Binary class distribution
    binary = df.groupBy("label").count().orderBy("label")
    print("\n1. BINARY CLASS DISTRIBUTION")
    binary.show()
    binary.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(OUTPUT + "/binary_distribution")

    # 2. Attack category distribution
    categories = df.groupBy("attack_cat").count().orderBy(
        col("count").desc()
    )
    print("\n2. ATTACK CATEGORY DISTRIBUTION")
    categories.show(20, truncate=False)
    categories.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(OUTPUT + "/attack_categories")

    # 3. Descriptive statistics
    selected = [
        "dur", "sbytes", "dbytes", "sttl",
        "dttl", "Spkts", "Dpkts", "tcprtt"
    ]
    print("\n3. DESCRIPTIVE STATISTICS")
    stats = df.select(*selected).describe()
    stats.show(truncate=False)
    stats.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(OUTPUT + "/descriptive_statistics")

    # 4. Pearson correlations
    print("\n4. PEARSON CORRELATION MATRIX (PAIRWISE)")
    corr_pairs = [
        ("dur", "sbytes"), ("dur", "dbytes"),
        ("dur", "Spkts"), ("dur", "Dpkts"),
        ("sbytes", "dbytes"), ("sttl", "dttl"),
        ("tcprtt", "synack"), ("tcprtt", "ackdat"),
        ("dur", "label"), ("sbytes", "label"),
        ("sttl", "label"), ("Spkts", "label")
    ]

    corr_rows = []
    for a, b in corr_pairs:
        value = df.stat.corr(a, b, method="pearson")
        corr_rows.append((a, b, float(value) if value is not None else None))
        print(f"{a} vs {b}: {value}")

    corr_df = spark.createDataFrame(
        corr_rows, ["feature_1", "feature_2", "pearson_correlation"]
    )
    corr_df.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(OUTPUT + "/correlations")

    # 5. Welch two-sample test: duration, normal vs attack
    print("\n5. WELCH TWO-SAMPLE TEST: CONNECTION DURATION")

    grouped = df.groupBy("label").agg(
        count("*").alias("n"),
        avg("dur").alias("mean_duration"),
        variance("dur").alias("variance_duration"),
        stddev("dur").alias("stddev_duration")
    ).collect()

    values = {int(r["label"]): r for r in grouped}
    normal = values[0]
    attack = values[1]

    n0, n1 = normal["n"], attack["n"]
    m0, m1 = normal["mean_duration"], attack["mean_duration"]
    v0, v1 = normal["variance_duration"], attack["variance_duration"]

    se2 = v0 / n0 + v1 / n1
    t_stat = (m0 - m1) / math.sqrt(se2)

    df_welch = (se2 ** 2) / (
        ((v0 / n0) ** 2 / (n0 - 1)) +
        ((v1 / n1) ** 2 / (n1 - 1))
    )

    # Large-sample two-sided normal approximation to the p-value.
    p_approx = math.erfc(abs(t_stat) / math.sqrt(2))

    print("Normal traffic count:", n0)
    print("Attack traffic count:", n1)
    print("Normal mean duration:", m0)
    print("Attack mean duration:", m1)
    print("Welch t-statistic:", t_stat)
    print("Welch degrees of freedom:", df_welch)
    print("Approximate two-sided p-value:", p_approx)

    # 6. Duration histogram (20 equal-width buckets)
    print("\n6. CONNECTION DURATION HISTOGRAM")

    bounds = df.agg(
        spark_min("dur").alias("minimum"),
        spark_max("dur").alias("maximum")
    ).first()

    minimum = bounds["minimum"]
    maximum = bounds["maximum"]
    width = (maximum - minimum) / 20.0

    if width > 0:
        histogram = df.withColumn(
            "duration_bucket",
            floor((col("dur") - minimum) / width).cast("integer")
        ).groupBy("duration_bucket").count().orderBy("duration_bucket")

        histogram.show(25)
        histogram.coalesce(1).write.mode("overwrite").option(
            "header", "true"
        ).csv(OUTPUT + "/duration_histogram")

    print("\nANALYSIS COMPLETED SUCCESSFULLY.")
    print("Results saved under:", OUTPUT)

finally:
    spark.stop()
