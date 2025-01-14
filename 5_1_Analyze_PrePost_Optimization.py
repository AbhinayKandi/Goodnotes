# Databricks notebook source
# MAGIC %md
# MAGIC Analyze and optimize operations - Provide two files: pre-optimization and post-optimization
# MAGIC
# MAGIC Provide a detailed analysis of the execution plan, identifying shuffle bottlenecks.
# MAGIC Explain how changes are implemented to improve the plan
# MAGIC [Optional]: Justify your chosen approach with performance metrics

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Fetching user_interactions data
schema=StructType([
    StructField("user_id", StringType(), True), StructField("timestamp", TimestampType(), True), StructField("action_type", StringType(), True), StructField("page_id", StringType(), True), StructField("duration_ms", LongType(), True), StructField("app_version", StringType(), True),StructField("date", DateType(), True)
    ])

user_interactions_df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/csv/user_interactions")

# COMMAND ----------

# Fetching user_metadata data
metadata_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("join_date", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("subscription_type", StringType(), True)
])

user_metadata_df = spark.read.format("csv").option("header", True).schema(metadata_schema).load("dbfs:/mnt/persistent/Goodnotes/input/csv/user_metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC I would like to join user_interactions and user_metadata datasets to analyze user activity trends by country and subscription type.

# COMMAND ----------

# MAGIC %md
# MAGIC # Pre-Optimization Implementation

# COMMAND ----------

# Inner joining the datsets on user_id column
joined_df = user_interactions_df.join(user_metadata_df, on="user_id", how="inner")

# COMMAND ----------

# Analyze user activity by country and subscription type
result = joined_df.groupBy("country", "subscription_type").count()

# COMMAND ----------

result.write.format('noop').mode('overwrite').save()

# COMMAND ----------

joined_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detailed analysis of the JOIN execution plan
# MAGIC The query is performing a broadcast hash join between two CSV datasets. Both datasets are filtered to exclude rows where user_id is NULL. A broadcast join is used. Spark broadcasts the smaller dataset - user_metadata to all executors, enabling an efficient join with the larger dataset - user_interactions based on the user_id column. The final result selects specific columns from both datasets

# COMMAND ----------

result.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detailed analysis of the groupBy-repartition execution plan
# MAGIC This physical plan represents a query to aggregate user data grouped by country and subscription_type. The query calculates the count of rows for each group. 
# MAGIC
# MAGIC Reads CSV files excludes rows where user_id is NULL.
# MAGIC Joins user_interactions and user_metadata tables on user_id.(BuildRight) The smaller user_metadata table is broadcast to all executors for efficient joining.
# MAGIC Hash Partitioning: The data is repartitioned based on country and subscription_type into 35 partitions.
# MAGIC HashAggregate:The final aggregation stage groups data by country and subscription_type.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identidified shuffle bottlenecks are as below:
# MAGIC > Spark by default created 200 partitions but only 35 were used to process the data wasting resource time and compute on the unused partitions.
# MAGIC > Also the stage to read 1000000 records took 1.2 minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC # Post-Optimization Implementation

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "35")

# COMMAND ----------

# Fetching user_interactions data
schema=StructType([
    StructField("user_id", StringType(), True), StructField("timestamp", TimestampType(), True), StructField("action_type", StringType(), True), StructField("page_id", StringType(), True), StructField("duration_ms", LongType(), True), StructField("app_version", StringType(), True),StructField("date", DateType(), True)
    ])

user_interactions_df_pq = spark.read.format("parquet").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/parquet/user_interactions")

# Fetching user_metadata data
metadata_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("join_date", TimestampType(), True),
    StructField("country", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("subscription_type", StringType(), True)
])

user_metadata_df_pq = spark.read.format("parquet").option("header", True).schema(metadata_schema).load("dbfs:/mnt/persistent/Goodnotes/input/parquet/user_metadata")

# COMMAND ----------

optimized_joined_df_pq = user_interactions_df_pq.join(broadcast(user_metadata_df_pq), on="user_id", how="inner")
optimized_result_pq = optimized_joined_df_pq.groupBy("country", "subscription_type").count().repartition("country")
optimized_result_pq.write.format('noop').mode('overwrite').save()

# COMMAND ----------

optimized_joined_df_pq.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detailed analysis of the JOIN execution plan
# MAGIC The query is performing a broadcast hash join between two parquet datasets. Both datasets are filtered to exclude rows where user_id is NULL. A broadcast join is used. Spark broadcasts the smaller dataset - user_metadata to all executors, enabling an efficient join with the larger dataset - user_interactions based on the user_id column. The final result selects specific columns from both datasets
# MAGIC
# MAGIC Optimizations Applied

# COMMAND ----------

optimized_result_pq.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Detailed analysis of the groupBy-repartition execution plan
# MAGIC This physical plan represents a query to aggregate user data grouped by country and subscription_type. The query calculates the count of rows for each group.
# MAGIC
# MAGIC Reads CSV files excludes rows where user_id is NULL. Joins user_interactions and user_metadata tables on user_id.
# MAGIC BroadcastHash Join: The smaller user_metadata table is broadcast to all executors for efficient joining. 
# MAGIC Hash Partitioning: The data is repartitioned based on country and subscription_type into 35 partitions.
# MAGIC HashAggregate:The Partial and final aggregation stage groups data by country and subscription_type.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimizations Applied:
# MAGIC > AQE was applied by default.
# MAGIC > Spark shuffle partitions were set to 35 which made the join operation complete a bit faster.
# MAGIC > Predicate Pushdown: Now the time taken to read 1000000 records took 7 seconds due to Parquet file format.

# COMMAND ----------

# MAGIC %md
# MAGIC
