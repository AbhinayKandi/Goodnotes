# Databricks notebook source
# MAGIC %md
# MAGIC 2. Calculate session-based metrics:
# MAGIC
# MAGIC Calculate metrics including:
# MAGIC
# MAGIC Average session duration
# MAGIC
# MAGIC Actions per session
# MAGIC
# MAGIC Define clear criteria for what constitutes a "session"
# MAGIC [Optional] Provide analysis of the results if time permits

# COMMAND ----------

# MAGIC %md
# MAGIC Definition of a **session** is subjective; a session may vary for several seconds to few minutes. I have considered a session to end after 30 minutes of inactivity; which means new session starts after the timeout.
# MAGIC
# MAGIC The requirement is to calculate the avg session duration and avg number of actions performed during those particular sessions. A session consitutes of actions performed by a user in a time frame so we would be grouping data based on user_id and active session.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

schema=StructType([StructField("user_id", StringType(), True), StructField("timestamp", TimestampType(), True), StructField("action_type", StringType(), True), StructField("page_id", StringType(), True), StructField("duration_ms", LongType(), True), StructField("app_version", StringType(), True),StructField("date", DateType(), True)])
user_interactions_df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/csv/user_interactions")

# COMMAND ----------

window = Window.partitionBy("user_id").orderBy(col("timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC Lets calculate time between actions

# COMMAND ----------

user_window = user_interactions_df.withColumn('time_diff',when(
                lag("timestamp").over(window).isNotNull(),
                unix_timestamp(col("timestamp")) - 
                unix_timestamp(lag("timestamp").over(window))
            ).otherwise(0))

# COMMAND ----------

# MAGIC %md
# MAGIC Lets mark those records that exceed the 30 minutes threshold

# COMMAND ----------

user_window=user_window.select('*',when(col('time_diff')> 30 * 60,lit(1)).otherwise(lit(0)).alias('is_new_session'))

# COMMAND ----------

# MAGIC %md
# MAGIC Now, Lets assign a generated session_id - combination of user_id and running_sum of is_new_session flag.

# COMMAND ----------

session_df=user_window.select('*',concat(col('user_id'),lit('_'),sum(col('is_new_session')).over(window)).alias('session_id'))

# COMMAND ----------

# MAGIC %md
# MAGIC Below code calculates the specified metrics per session

# COMMAND ----------

session_metrics = session_df.groupBy(col('session_id')).agg(round(
                (unix_timestamp(max("timestamp")) - 
                 unix_timestamp(min("timestamp"))) / 60,
                2).alias("session_duration_minutes"),count(col('is_new_session')).alias("action_count")).orderBy(col('session_duration_minutes').desc())
session_metrics.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The highest average session was close to 30mins with 2 actions performed during the session. I can do a collect_list() to describe the actions performed during the sessions.

# COMMAND ----------

# MAGIC %md
# MAGIC
