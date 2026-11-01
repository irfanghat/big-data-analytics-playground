from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, lower
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator

spark = SparkSession.builder \
    .appName("UNSW-NB15-Classification") \
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
    print("Loading dataset...")
    df = spark.read.option("header", "false").csv(INPUT).toDF(*columns)

    df = df.withColumn("attack_cat", trim(col("attack_cat")))
    df = df.withColumn(
        "attack_cat",
        when(col("attack_cat").isNull() | (col("attack_cat") == ""), "Normal")
        .when(lower(col("attack_cat")) == "backdoor", "Backdoors")
        .when(lower(col("attack_cat")) == "backdoors", "Backdoors")
        .otherwise(col("attack_cat"))
    )

    df = df.withColumn("label", col("label").cast("double"))

    numeric = [
        "sport", "dsport", "dur", "sbytes", "dbytes", "sttl",
        "dttl", "sloss", "dloss", "Sload", "Dload", "Spkts",
        "Dpkts", "swin", "dwin", "smeansz", "dmeansz",
        "trans_depth", "res_bdy_len", "Sjit", "Djit",
        "Sintpkt", "Dintpkt", "tcprtt", "synack", "ackdat",
        "is_sm_ips_ports", "ct_state_ttl", "ct_flw_http_mthd",
        "is_ftp_login", "ct_ftp_cmd", "ct_srv_src", "ct_srv_dst",
        "ct_dst_ltm", "ct_src_ltm", "ct_src_dport_ltm",
        "ct_dst_sport_ltm", "ct_dst_src_ltm"
    ]

    categorical = ["proto", "state", "service"]

    for c in numeric:
        df = df.withColumn(c, col(c).cast("double"))

    df = df.fillna(0, subset=numeric)
    df = df.fillna("unknown", subset=categorical)

    # Exclude IP addresses, timestamps and target fields.
    feature_cols = numeric + categorical

    def build_pipeline(target, multiclass=False):
        indexers = [
            StringIndexer(
                inputCol=c,
                outputCol=c + "_idx",
                handleInvalid="keep"
            )
            for c in categorical
        ]

        encoder = OneHotEncoder(
            inputCols=[c + "_idx" for c in categorical],
            outputCols=[c + "_vec" for c in categorical],
            handleInvalid="keep"
        )

        assembler = VectorAssembler(
            inputCols=numeric + [c + "_vec" for c in categorical],
            outputCol="raw_features",
            handleInvalid="keep"
        )

        scaler = StandardScaler(
            inputCol="raw_features",
            outputCol="features",
            withStd=True,
            withMean=False
        )

        stages = indexers + [encoder, assembler, scaler]

        if multiclass:
            label_indexer = StringIndexer(
                inputCol=target,
                outputCol="target",
                handleInvalid="keep"
            )
            stages.append(label_indexer)
            lr = LogisticRegression(
                featuresCol="features",
                labelCol="target",
                maxIter=20,
                regParam=0.01,
                family="multinomial"
            )
        else:
            lr = LogisticRegression(
                featuresCol="features",
                labelCol=target,
                maxIter=20,
                regParam=0.01
            )

        stages.append(lr)
        return Pipeline(stages=stages)

    def evaluate_model(model, test, target, name, multiclass=False):
        predictions = model.transform(test).cache()

        evaluator = MulticlassClassificationEvaluator(
            labelCol=target,
            predictionCol="prediction"
        )

        accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
        precision = evaluator.setMetricName("weightedPrecision").evaluate(predictions)
        recall = evaluator.setMetricName("weightedRecall").evaluate(predictions)
        f1 = evaluator.setMetricName("f1").evaluate(predictions)

        print("\n==========", name, "RESULTS ==========")
        print("Test records:", test.count())
        print("Accuracy:", accuracy)
        print("Weighted precision:", precision)
        print("Weighted recall:", recall)
        print("Weighted F1:", f1)

        print("\nConfusion matrix (actual, predicted, count):")
        matrix = predictions.groupBy(target, "prediction").count().orderBy(target, "prediction")
        matrix.show(100, truncate=False)

        matrix.coalesce(1).write.mode("overwrite").option(
            "header", "true"
        ).csv(OUTPUT + "/" + name.lower() + "_confusion_matrix")

        predictions.select(target, "prediction", "probability").limit(1000) \
            .write.mode("overwrite").parquet(OUTPUT + "/" + name.lower() + "_predictions")

        print("Accuracy:", accuracy, file=open("/tmp/" + name + "_metrics.txt", "w"))
        print("Precision:", precision, file=open("/tmp/" + name + "_metrics.txt", "a"))
        print("Recall:", recall, file=open("/tmp/" + name + "_metrics.txt", "a"))
        print("F1:", f1, file=open("/tmp/" + name + "_metrics.txt", "a"))

        predictions.unpersist()

    # BINARY CLASSIFICATION: approximately balanced sample
    print("\nPreparing binary classification sample...")
    binary_sample = df.stat.sampleBy(
        "label", {0.0: 0.05, 1.0: 0.31}, seed=42
    ).cache()

    print("Binary sample records:", binary_sample.count())
    binary_train, binary_test = binary_sample.randomSplit([0.8, 0.2], seed=42)

    binary_pipeline = build_pipeline("label")
    binary_model = binary_pipeline.fit(binary_train)

    evaluate_model(
        binary_model, binary_test, "label",
        "Binary", multiclass=False
    )

    binary_sample.unpersist()

    # MULTICLASS CLASSIFICATION: stratified sample, maximum 15,000/category
    print("\nPreparing multiclass classification sample...")

    counts = {
        r["attack_cat"]: r["count"]
        for r in df.groupBy("attack_cat").count().collect()
    }

    fractions = {
        category: min(1.0, 15000.0 / amount)
        for category, amount in counts.items()
    }

    multi_sample = df.stat.sampleBy(
        "attack_cat", fractions, seed=42
    ).cache()

    print("Multiclass sample records:", multi_sample.count())
    print("Category sample sizes:")
    multi_sample.groupBy("attack_cat").count().orderBy("attack_cat").show(20, False)

    multi_train, multi_test = multi_sample.randomSplit([0.8, 0.2], seed=42)

    multi_pipeline = build_pipeline("attack_cat", multiclass=True)
    multi_model = multi_pipeline.fit(multi_train)

    print("\nMULTICLASS LABEL MAPPING:")
    label_model = multi_model.stages[-2]
    for index, category in enumerate(label_model.labels):
        print(index, "=", category)

    # Transform labels using the fitted StringIndexer in the pipeline.
    multi_predictions = multi_model.transform(multi_test).cache()

    evaluator = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction"
    )

    print("\n========== MULTICLASS RESULTS ==========")
    print("Test records:", multi_test.count())
    for metric in ["accuracy", "weightedPrecision", "weightedRecall", "f1"]:
        evaluator.setMetricName(metric)
        print(metric, ":", evaluator.evaluate(multi_predictions))

    print("\nMulticlass confusion matrix:")
    multi_predictions.groupBy("target", "prediction").count() \
        .orderBy("target", "prediction").show(120, False)

    multi_predictions.groupBy("target", "prediction").count() \
        .coalesce(1).write.mode("overwrite").option(
            "header", "true"
        ).csv(OUTPUT + "/multiclass_confusion_matrix")

    multi_sample.unpersist()
    multi_predictions.unpersist()

    print("\nBINARY AND MULTICLASS CLASSIFICATION COMPLETED.")

finally:
    spark.stop()
