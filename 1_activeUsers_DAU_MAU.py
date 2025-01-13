# Databricks notebook source
# MAGIC %md
# MAGIC 1. Calculate Daily Active Users (DAU) and Monthly Active Users (MAU) for the past year.
# MAGIC
# MAGIC Define clear criteria for what constitutes an "active" user
# MAGIC
# MAGIC Implement a solution that scales efficiently for large datasets
# MAGIC
# MAGIC [Optional] 
# MAGIC
# MAGIC Explain how you:
# MAGIC Handle outliers and extremely long duration values, 
# MAGIC OR
# MAGIC Describe challenges you might face while creating similar metrics and how you would address them

# COMMAND ----------

# MAGIC %md
# MAGIC An active user is defined as a user who has performed actions such as 'page_view', 'edit', 'create', 'delete', 'share' in the user interactions dataset within a given period of time (hourly/daily/monthly/yearly).
# MAGIC
# MAGIC Outliers and extremely long durations will not affect the calculation, as they do not directly influence the "active" status.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %fs ls /mnt/persistent/Goodnotes/input/csv/

# COMMAND ----------

schema=StructType([StructField("user_id", StringType(), True), StructField("timestamp", TimestampType(), True), StructField("action_type", StringType(), True), StructField("page_id", StringType(), True), StructField("duration_ms", LongType(), True), StructField("app_version", StringType(), True),StructField("date", DateType(), True)])
user_interactions_df = spark.read.format("csv").option("header", True).schema(schema).load("dbfs:/mnt/persistent/Goodnotes/input/csv/user_interactions").select('user_id','timestamp','date') 


# COMMAND ----------

user_interaction.rdd.getNumPartitions()

# COMMAND ----------

user_interactions_df = user_interactions_df.filter((col('date') >= '2024-01-01') & (col('date') < '2025-01-01')).select('user_id','timestamp','date',month(col('timestamp')).alias('month'))

# COMMAND ----------

user_interactions_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Monthly Active Users (MAU)

# COMMAND ----------

window=Window.partitionBy([col('user_id')]).orderBy(col('month'))
active_users=user_interactions_df.groupBy(['user_id','month']).agg(max(col('date')).alias('last_active_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC The above code calculates the number of users who were active for 12 consecutive months from the user_interactions_df DataFrame. It first groups user interactions by user_id and month, aggregating to find the last active date for each user per month.

# COMMAND ----------

# MAGIC %md
# MAGIC - Randomly checking for few user_ids to see if the user is active in consecutive months - looks like the user_id were not active.

# COMMAND ----------

user_interactions_df.where((col('user_id') == 'u994647') | (col('user_id') =='u506724') | (col('user_id') =='u587822')).select('*').orderBy(col('user_id')).show()

# COMMAND ----------

monthly_active_users=active_users.select('user_id',row_number().over(window).alias('row_num')).filter(col('row_num')==lit(12)).select(count(col('user_id')).alias('monthly_acitve_users'))
monthly_active_users.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The above code implements a window function partitioned by user_id and ordered by month, it assigns a row number to each month of activity for each user. Finally, it filters to include only users with 12 rows (indicating activity in 12 consecutive months) and counts the total number of such users, displaying the result as the monthly_active_users

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Active Users (DAU)

# COMMAND ----------

window=Window.partitionBy([col('user_id')]).orderBy(col('date'))

# COMMAND ----------

# MAGIC %md
# MAGIC A window is defined to partition the data by user_id and order the records by date. This allows analysis within each user's activity timeline.

# COMMAND ----------

daily_acitve_users=user_interactions_df.select('user_id',col('date'),lag(col('date')).over(window).alias('prev_date')).select('*',datediff(col("date"), col("prev_date")).alias('days_diff')).fillna({'days_diff': 1}).filter(col('days_diff') == lit(1))
daily_acitve_users=daily_acitve_users.groupBy("days_diff").sum().select(col('sum(days_diff)').alias('daily_acitve_users'))
daily_acitve_users.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The lag function computes the previous activity date (prev_date) for each user, enabling a comparison between consecutive activity dates.
# MAGIC
# MAGIC The difference in days between the current date (date) and the previous date (prev_date) is calculated using the datediff function. Any null differences (indicating the first activity for a user) are replaced with 1 using fillna.
# MAGIC
# MAGIC Rows where the difference is exactly 1 day are retained, representing users who were active on a new day. The filtered dataset is grouped by diff (always 1 in this case) and summed to count the number of daily active users.

# COMMAND ----------


