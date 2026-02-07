from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp,lit,sum as spark_sum,coalesce, datediff, current_date,date_sub,to_date,when, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from datetime import datetime,date, timedelta
import os

#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4 Features_Stream_s3.py
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# =====================================================
# 1. Spark Session
# =====================================================
spark = SparkSession.builder \
    .appName("CDC_Kafka_Console_Features") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# =====================================================
# 2. S3 / MinIO configuration
# =====================================================
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

S3_FEATURE_PATH = "s3a://creditstartdevrawbucket/user_features/"
CHECKPOINT_PATH = "s3a://creditstartdevrawbucket/checkpoints/user_features/"


# =====================================================
# 3. Kafka Source
# =====================================================
kafka_bootstrap_servers = "localhost:9092"
kafka_topic = "dbserver1.public.user"

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_str")


# =====================================================
# 4. Debezium schema
# =====================================================
payload_schema = StructType([
    StructField("before", StructType([
        StructField("id", IntegerType(), True),
        StructField("created_on", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("personal_code", StringType(), True)
    ]), True),
    StructField("after", StructType([
        StructField("id", IntegerType(), True),
        StructField("created_on", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("personal_code", StringType(), True)
    ]), True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

cdc_schema = StructType([
    StructField("payload", payload_schema, True)
])

# -------------------------------
# 4. Parse JSON
# -------------------------------
df_parsed = df_string.select(from_json(col("json_str"), cdc_schema).alias("data"))

# -------------------------------
# 5. Flatten 'payload.after' fields
# -------------------------------
df_final = df_parsed.select(
    col("data.payload.after.id").alias("user_id"),
    col("data.payload.after.created_on").alias("created_on"),
    col("data.payload.after.first_name").alias("first_name"),
    col("data.payload.after.last_name").alias("last_name"),
    col("data.payload.after.birth_date").alias("birth_date"),
    col("data.payload.after.personal_code").alias("personal_code"),
    col("data.payload.op").alias("operation"),
    col("data.payload.ts_ms").alias("event_timestamp")
)

# =====================================================
# 5. Postgres connection (psycopg2)
# =====================================================
import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    database="creditstar",
    user="credit_dev",
    password="nag6895"
)

def fetch_table(query):
    return pd.read_sql(query, conn)

df_final_inserts = df_final.filter(col("operation") == "c")

# =====================================================
# 6. foreachBatch logic
# =====================================================
def foreach_batch_function(df, batch_id):
    # Keep only latest insert per user in this micro-batch
    if df.rdd.isEmpty():
        return
    
    loans_pd = fetch_table("SELECT id, client_id, amount, status, created_on FROM public.loan")
    payments_pd = fetch_table("SELECT id, loan_id, interest, status, created_on FROM public.payment")

    # Rename columns to avoid ambiguity
    loans_pd = loans_pd.rename(columns = {"id": "loan_id","status": "loan_status", "created_on": "loan_created_on"})
    payments_pd = payments_pd.rename(columns={"id": "payment_id","status": "payment_status", "created_on": "payment_created_on"})


    # Convert pandas to Spark DataFrames
    loans_df = spark.createDataFrame(loans_pd)
    payments_df = spark.createDataFrame(payments_pd)

    # Join streaming users with loans
    user_loans = df.join(loans_df, df.user_id == loans_df.client_id, "left")

    # Feature 1: number of paid loans
    feature_row = (
        user_loans.groupBy("user_id")
    .agg(coalesce(spark_sum((col("loan_status") == "paid").cast("int")),lit(0)).alias("paid_loans_count")).
    withColumn("event_time", current_timestamp())
    )
    # Feature 2: days since last late payment
    # Only consider payments for the loans of these users
    p = payments_df.alias("p")
    l = user_loans.alias("l")
    user_payments = (
    p.join(l, col("p.loan_id") == col("l.loan_id"), "inner")
     .select(
         col("p.payment_id"),
         col("p.loan_id").alias("loan_id"),
         col("p.interest"),
         col("p.payment_status"),
         col("p.payment_created_on"),
         col("l.user_id"),
         col("l.amount"),
         col("l.loan_status"),
         col("l.loan_created_on")
     ))



    # Filter only late payments
    late_payments = user_payments.filter(col("payment_status") == "late")

    # Get last late payment date per user
    last_late_payment = (
        late_payments.groupBy("user_id")
        .agg(spark_max(col("payment_created_on")).alias("last_late_payment_date"))
    )

    # Join with feature_row
    feature_row = feature_row.join(last_late_payment, "user_id", "left")
    
    # Calculate days since last late payment
    feature_row = feature_row.withColumn(
        "days_since_last_late_payment",
        coalesce(datediff(current_date(), col("last_late_payment_date")), lit(0))
    ).withColumn("event_time", current_timestamp()) \
     .select("user_id", "paid_loans_count", "days_since_last_late_payment", "event_time")


    # -------------------------------
    # Feature 3: Profit in last 90 days
    # -------------------------------
    cutoff_date = date_sub(current_date(), 90)



    recent_loans = (user_payments
    .withColumn("loan_created_on_date", to_date("loan_created_on"))
    .filter(col("loan_created_on_date") >= cutoff_date))

    feature3 = (
    recent_loans
    .groupBy("user_id")
    .agg(
        coalesce(spark_sum("interest"), lit(0)).alias("recent_interest_received"),
        coalesce(spark_sum("amount"), lit(0)).alias("recent_loan_amount")
    )
    .withColumn(
        "profit_in_last_90_days_rate",
        when(col("recent_loan_amount") > 0,
             col("recent_interest_received") / coalesce(col("recent_loan_amount"),lit(0)))
        .otherwise(lit(0.0))
    )
    .select("user_id", "profit_in_last_90_days_rate")
)

    # -------------------------------
    # Combine all features
    # -------------------------------
    feature_row = feature_row.join(feature3, "user_id", "left") \
        .withColumn("event_time", current_timestamp()) \
        .withColumn(
        "profit_in_last_90_days_rate",
        coalesce(col("profit_in_last_90_days_rate"), lit(0.0)))\
        .withColumn("dt",to_date(col("event_time")))\
        .select(
            "user_id",
            "paid_loans_count",
            "days_since_last_late_payment",
            "profit_in_last_90_days_rate",
            "event_time",
            "dt"
        )

    # Write features to console
    feature_row.show(feature_row.count(),truncate=False)

    # -------------------------
    # Write to S3 (PARTITIONED)
    # -------------------------
    (   
        feature_row
        .write
        .mode("append")
        .partitionBy("dt")
        .parquet(S3_FEATURE_PATH)
    )    

# -------------------------------
# 8. Write stream with foreachBatch
# -------------------------------
query = (df_final.writeStream
    .foreachBatch(foreach_batch_function)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .start()
)

query.awaitTermination()
