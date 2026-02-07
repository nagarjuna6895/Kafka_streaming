from pyspark.sql import SparkSession

##command to execute as postgres driver not working from here
##spark-submit --packages org.postgresql:postgresql:42.7.3,org.apache.hadoop:hadoop-aws:3.3.4 postgres_snapshot.py

# ============================
# Spark Session (BATCH)
# ============================
spark = (
    SparkSession.builder
    .appName("Postgres_Table_Snapshots")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.postgresql:postgresql:42.7.3",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ])
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================
# MinIO / S3 configuration
# ============================
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", "********")
hadoop_conf.set("fs.s3a.secret.key", "********")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# ============================
# Postgres JDBC details
# ============================
JDBC_URL = "jdbc:postgresql://localhost:5432/creditstar"

JDBC_PROPS = {
    "user": "credit_dev",
    "password": "********",
    "driver": "org.postgresql.Driver"
}

# ============================
# Snapshot helper function
# ============================
def snapshot_table(table_name, output_path):
    (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table_name)
        .option("user", JDBC_PROPS["user"])
        .option("password", JDBC_PROPS["password"])
        .option("driver", JDBC_PROPS["driver"])
        .load()
        .write
        .mode("overwrite")   # safe to rerun
        .parquet(output_path)
    )

    print(f"Snapshot created for {table_name}")

# ============================
# Create snapshots (ALL 3)
# ============================

# Users snapshot
snapshot_table(
    "public.user",
    "s3a://creditstartdevrawbucket/users_snapshot/"
)

# Loans snapshot
snapshot_table(
    "public.loan",
    "s3a://creditstartdevrawbucket/loans_snapshot/"
)

# Payments snapshot
snapshot_table(
    "public.payment",
    "s3a://creditstartdevrawbucket/payments_snapshot/"
)

print("All snapshots completed successfully")

# ============================
# Optional verification
# ============================
spark.read.parquet(
    "s3a://creditstartdevrawbucket/loans_snapshot/"
).show(5, truncate=False)
