# Databricks notebook source
# MAGIC %md
# MAGIC ### Pre-Optimization: Identifying Bottlenecks

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

schema=StructType([StructField("user_id", StringType(), True), StructField("timestamp", TimestampType(), True), StructField("action_type", StringType(), True), StructField("page_id", StringType(), True), StructField("duration_ms", LongType(), True), StructField("app_version", StringType(), True),StructField("date", DateType(), True)])

user_interactions_df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/csv/user_interactions").select('user_id','timestamp','date') 

# COMMAND ----------

user_interactions_df.rdd.getNumPartitions()

# COMMAND ----------

user_interactions_df = user_interactions_df.filter((col('date') >= '2024-01-01') & (col('date') < '2025-01-01')).select('user_id','timestamp','date',month(col('timestamp')).alias('month'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Active Users (MAU)

# COMMAND ----------

window=Window.partitionBy([col('user_id')]).orderBy(col('month'))
active_users=user_interactions_df.groupBy(['user_id','month']).agg(max(col('date')).alias('last_active_date'))

monthly_active_users=active_users.select('user_id',row_number().over(window).alias('row_num')).filter(col('row_num')==lit(12)).select(count(col('user_id')).alias('monthly_acitve_users'))
monthly_active_users.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Active Users (DAU)

# COMMAND ----------

window=Window.partitionBy([col('user_id')]).orderBy(col('date'))

daily_acitve_users=user_interactions_df.select('user_id',col('date'),lag(col('date')).over(window).alias('prev_date')).select('*',datediff(col("date"), col("prev_date")).alias('days_diff')).fillna({'days_diff': 1}).filter(col('days_diff') == lit(1))
daily_acitve_users=daily_acitve_users.groupBy("days_diff").sum().select(col('sum(days_diff)').alias('daily_acitve_users'))
daily_acitve_users.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session Based Metrics

# COMMAND ----------

user_interactions_df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/csv/user_interactions").select('user_id','timestamp','date') 

# COMMAND ----------

window = Window.partitionBy("user_id").orderBy(col("timestamp"))

# COMMAND ----------

user_window = user_interactions_df.withColumn('time_diff',when(
                lag("timestamp").over(window).isNotNull(),
                unix_timestamp(col("timestamp")) - 
                unix_timestamp(lag("timestamp").over(window))
            ).otherwise(0))

# COMMAND ----------

user_window=user_window.select('*',when(col('time_diff')> 30 * 60,lit(1)).otherwise(lit(0)).alias('is_new_session'))

# COMMAND ----------

session_df=user_window.select('*',concat(col('user_id'),lit('_'),sum(col('is_new_session')).over(window)).alias('session_id'))

# COMMAND ----------

session_metrics = session_df.groupBy(col('session_id')).agg(round(
                (unix_timestamp(max("timestamp")) - 
                 unix_timestamp(min("timestamp"))) / 60,
                2).alias("session_duration_minutes"),count(col('is_new_session')).alias("action_count")).orderBy(col('session_duration_minutes').desc())
session_metrics.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Potential Optimizations are as below:
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. File Reading (CSV Format)
# MAGIC
# MAGIC - Hypothesis: CSV file format is slower to read compared to columnar formats like Parquet due to the lack of optimization for parallel reads and compression.
# MAGIC
# MAGIC - Proposed Solution: Convert the source files to Parquet format for faster I/O and smaller file sizes.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Repeated Data Shuffling
# MAGIC
# MAGIC - Hypothesis: Multiple transformations, such as filtering, window operations, and aggregations, involve repeated shuffles, leading to increased execution time.
# MAGIC
# MAGIC -  Solution: Use caching for intermediate DataFrames reused across multiple stages.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Repartition
# MAGIC
# MAGIC - Hypothesis: We are partitioning dataset on 'user_id' but the dataset is primarily partitioned on 'date'.
# MAGIC
# MAGIC - Solution: Repartition dataset on 'user_id' once read or before writing to parquet which also benefits if we are bucketing when joining with other dataset where join is done on 'user_id' column. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selected Optimization: File Format Conversion (CSV -> Parquet)
# MAGIC
# MAGIC This optimization addresses the foundational issue of slow I/O during file reads. Switching to Parquet can reduce the job's total execution time significantly. I will compare metrics such as execution time,shuffle_size and task distribution in the Spark UI.

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", "64MB")

# COMMAND ----------

user_interactions_df = spark.read.format("parquet").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/parquet/user_interactions").select('user_id','timestamp','date') 

user_interactions_df=user_interactions_df.coalesce(4)

user_interactions_df = user_interactions_df.filter((col('date') >= '2024-01-01') & (col('date') < '2025-01-01')).select('user_id','timestamp','date',month(col('timestamp')).alias('month'))

# COMMAND ----------

user_interactions_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Active Users (MAU)

# COMMAND ----------

window=Window.partitionBy([col('user_id')]).orderBy(col('month'))
active_users=user_interactions_df.groupBy(['user_id','month']).agg(max(col('date')).alias('last_active_date'))

monthly_active_users=active_users.select('user_id',row_number().over(window).alias('row_num')).filter(col('row_num')==lit(12)).select(count(col('user_id')).alias('monthly_acitve_users'))
monthly_active_users.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Active Users (DAU)

# COMMAND ----------

window=Window.partitionBy([col('user_id')]).orderBy(col('date'))

daily_acitve_users=user_interactions_df.select('user_id',col('date'),lag(col('date')).over(window).alias('prev_date')).select('*',datediff(col("date"), col("prev_date")).alias('days_diff')).fillna({'days_diff': 1}).filter(col('days_diff') == lit(1))
daily_acitve_users=daily_acitve_users.groupBy("days_diff").sum().select(col('sum(days_diff)').alias('daily_acitve_users'))
daily_acitve_users.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session Based Metrics

# COMMAND ----------

user_interactions_df = spark.read.format("parquet").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/parquet/user_interactions").select('user_id','timestamp','date') 

user_interactions_df=user_interactions_df.coalesce(4)

# COMMAND ----------

window = Window.partitionBy("user_id").orderBy(col("timestamp"))

# COMMAND ----------

user_window = user_interactions_df.withColumn('time_diff',when(
                lag("timestamp").over(window).isNotNull(),
                unix_timestamp(col("timestamp")) - 
                unix_timestamp(lag("timestamp").over(window))
            ).otherwise(0))

# COMMAND ----------

user_window=user_window.select('*',when(col('time_diff')> 30 * 60,lit(1)).otherwise(lit(0)).alias('is_new_session'))

# COMMAND ----------

session_df=user_window.select('*',concat(col('user_id'),lit('_'),sum(col('is_new_session')).over(window)).alias('session_id'))

# COMMAND ----------

session_metrics = session_df.groupBy(col('session_id')).agg(round(
                (unix_timestamp(max("timestamp")) - 
                 unix_timestamp(min("timestamp"))) / 60,
                2).alias("session_duration_minutes"),count(col('is_new_session')).alias("action_count")).orderBy(col('session_duration_minutes').desc())
session_metrics.show()
