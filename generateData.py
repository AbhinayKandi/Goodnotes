# Databricks notebook source
# MAGIC %md
# MAGIC This script is only meant to be run once to generate the Initial Datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC Below is the script to generate 2 datasets namely User Interactions (~ 1 TB, partitioned by date) and User Metadata (~ 100 GB, partitioned by country).
# MAGIC
# MAGIC The datasets have been generated in both csv and parquet format.

# COMMAND ----------

spark

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/persistent/Goodnotes/input/", recurse=True)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------

import csv
import random
from datetime import datetime, timedelta

def generate_user_id():
    return f"u{random.randint(1, 1000000):06d}"

def generate_timestamp():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 12, 31)
    return start_date + timedelta(seconds=random.randint(0, int((end_date - start_date).total_seconds())))

def generate_action_type():
    return random.choice(['page_view', 'edit', 'create', 'delete', 'share'])

def generate_page_id():
    return f"p{random.randint(1, 1000000):06d}"

def generate_duration_ms():
    return random.randint(100, 300000)

def generate_app_version():
    major = random.randint(5, 7)
    minor = random.randint(0, 9)
    patch = random.randint(0, 9)
    return f"{major}.{minor}.{patch}"

def generate_join_date():
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2024, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def generate_country():
    return random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'IN', 'BR', 'MX'])

def generate_device_type():
    return random.choice(['iPhone', 'iPad', 'Android Phone', 'Android Tablet', 'Windows', 'Mac'])

def generate_subscription_type():
    return random.choice(['free', 'basic', 'premium', 'enterprise'])

def generate_user_interactions(num_records):
    data = []
    for _ in range(num_records):
        data.append([
            generate_user_id(),
            generate_timestamp(),
            generate_action_type(),
            generate_page_id(),
            generate_duration_ms(),
            generate_app_version()
        ])
    return data

def generate_user_metadata(num_records):
    data = []
    for _ in range(num_records):
        data.append([
            generate_user_id(),
            generate_join_date(),
            generate_country(),
            generate_device_type(),
            generate_subscription_type()
        ])
    return data

# Create directory in DBFS
dbutils.fs.mkdirs("/mnt/persistent/Goodnotes/input/csv")
dbutils.fs.mkdirs("/mnt/persistent/Goodnotes/input/parquet")

# Generate user interactions
user_interactions_data = generate_user_interactions(1000000)
user_interactions_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("action_type", StringType(), True),
    StructField("page_id", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("app_version", StringType(), True)
])
user_interactions_df = spark.createDataFrame(user_interactions_data, schema=user_interactions_schema)

# Add a new column for the date part of the timestamp
user_interactions_df = user_interactions_df.withColumn("date", to_date(col("timestamp")).cast(StringType()))

user_interactions_df.write.partitionBy("date").mode('overwrite').option("compression", "snappy").parquet("/mnt/persistent/Goodnotes/input/parquet/user_interactions")
user_interactions_df.write.partitionBy("date").mode('overwrite').csv("/mnt/persistent/Goodnotes/input/csv/user_interactions")

# Generate user metadata
user_metadata_data = generate_user_metadata(100000)
user_metadata_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("join_date", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("subscription_type", StringType(), True)
])
user_metadata_df = spark.createDataFrame(user_metadata_data, schema=user_metadata_schema)
user_metadata_df.write.partitionBy("country").mode('overwrite').option("compression", "snappy").parquet("/mnt/persistent/Goodnotes/input/parquet/user_metadata")
user_metadata_df.write.partitionBy("country").mode('overwrite').csv("/mnt/persistent/Goodnotes/input/csv/user_metadata")

print("Sample datasets generated and partitioned successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if the files have been written correctly - checking for only user_interactions- csv type files

# COMMAND ----------

# MAGIC %fs ls /mnt/persistent/Goodnotes/input/csv/user_interactions/

# COMMAND ----------

# MAGIC %md
# MAGIC I've configured Databricks with S3 so the datasets created in s3 like below:

# COMMAND ----------

# MAGIC %md
# MAGIC s3://databricks-workspace-stack-3cd72-bucket/ireland-prod/627902140611841/mnt/persistent/Goodnotes/input/csv/user_interactions/date=2024-01-01/

# COMMAND ----------


