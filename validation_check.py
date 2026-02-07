from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull

# ============================
# Spark Session
# ============================
spark = (
    SparkSession.builder
    .appName("Validate_User_Features_From_S3")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================
# MinIO / S3 configuration
# ============================
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", "*******")
hadoop_conf.set("fs.s3a.secret.key", "*******")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# ============================
# S3 Feature Path
# ============================
S3_FEATURE_PATH = "s3a://creditstartdevrawbucket/user_features/"

# ============================
# 1. Read data
# ============================
df = spark.read.parquet(S3_FEATURE_PATH)

# ============================
# 2. SHOW SAMPLE DATA (FIRST)
# ============================
print("\n=== SAMPLE DATA (FIRST 20 ROWS) ===")
df.orderBy(col("event_time").desc()).show(20, truncate=False)

# ============================
# 3. Schema
# ============================
print("\n=== SCHEMA ===")
df.printSchema()

# ============================
# 4. Record count
# ============================
print("\n=== TOTAL RECORD COUNT ===")
print(df.count())

# ============================
# 5. Partition validation
# ============================
print("\n=== DISTINCT PARTITIONS (dt) ===")
df.select("dt").distinct().orderBy("dt").show(20, truncate=False)

# ============================
# 6. Null checks
# ============================
print("\n=== NULL CHECKS ===")
df.select(
    count(isnull("user_id")).alias("user_id_nulls"),
    count(isnull("paid_loans_count")).alias("paid_loans_count_nulls"),
    count(isnull("days_since_last_late_payment")).alias("days_since_last_late_payment_nulls"),
    count(isnull("profit_in_last_90_days_rate")).alias("profit_in_last_90_days_rate_nulls"),
    count(isnull("event_time")).alias("event_time_nulls"),
    count(isnull("dt")).alias("dt_nulls")
).show(truncate=False)

# ============================
# 7. Metric sanity checks
# ============================
print("\n=== INVALID METRIC VALUES ===")
df.filter(
    (col("paid_loans_count") < 0) |
    (col("days_since_last_late_payment") < 0) |
    (col("profit_in_last_90_days_rate") < 0)
).show(20, truncate=False)

# ============================
# 8. Duplicate check
# ============================
print("\n=== DUPLICATES (user_id, dt) ===")
df.groupBy("user_id", "dt") \
  .count() \
  .filter(col("count") > 1) \
  .show(20, truncate=False)

print("\n=== VALIDATION COMPLETED SUCCESSFULLY ===")
