# Databricks notebook source
# MAGIC %md
# MAGIC Data Processing and Optimization
# MAGIC
# MAGIC a. Join the User Interactions and User Metadata datasets efficiently while handling data skew:
# MAGIC
# MAGIC Explain your approach to mitigating skew in the join operation, considering that certain user_ids may be significantly more frequent
# MAGIC Implement and justify your choice of join strategy (e.g., broadcast join, shuffle hash join, sort merge join)

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
# MAGIC I'm checking to find the number of partitions that data has been paritioned into.

# COMMAND ----------

user_interactions_df.rdd.getNumPartitions()

# COMMAND ----------

user_metadata_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC I'm checking if there is any skewed data based on the joining key - user_id.

# COMMAND ----------

user_interactions_df.select(spark_partition_id().alias('partition_id')).groupBy(col('partition_id')).agg(count(lit(1)).alias('count')).orderBy(col('count').desc()).show()

# COMMAND ----------

user_metadata_df.select(spark_partition_id().alias('partition_id')).groupBy(col('partition_id')).agg(count(lit(1)).alias('count')).orderBy(col('count').desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Looks like the data has been equally partitioned and there is no skew in the datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC Disabling AQE to see how the join performs on 2 large datasets.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",False)

# COMMAND ----------

# MAGIC %md
# MAGIC The below join is executed to enrich the user_interactions_df table with subscription_type data from user_metadata_df table.

# COMMAND ----------

# joining based on user_id
df_joined=user_interactions_df.join(user_metadata_df,on=user_interactions_df.user_id==user_metadata_df.user_id,how='left_outer').select(user_interactions_df["*"],user_metadata_df["subscription_type"])

df_joined.write.format('noop').mode('overwrite').save()
# noop stands for no operation - usually used for performance benchmarking - which reads the data but does not write the data anywhere - can be called as fake write.

# COMMAND ----------

# MAGIC %md
# MAGIC Join without AQE has created 3 stages - 2 stages to read the read and 1 stages to perform join with 200 partitions.

# COMMAND ----------

df_joined.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Bydefault, spark has performed sort-merge join which has resulted in data has being exchanged across executors.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enabling AQE
# MAGIC
# MAGIC AQE has the flexibility to change the query at runtime. Broadly, AQE can help us in following ways:
# MAGIC 1. Dynamically coalescing shuffle partitions - AQE does the coalescing the shuffle partitions to mitigate the skewness.
# MAGIC 2. Dynamically  switching Joins - Based on the initial DAG, spark decides to do a sort merge join. If AQE is enabled, a broadcast join would be performed if the small table is small enough to fit in the driver memory.
# MAGIC 3. Dynamically Optimizing Skew joins - AQE has shuffle reader which gets to know that data volume in bo the tables are skewed.
# MAGIC
# MAGIC If the all the above fail then the last resort is to perform **Salting** - which is to add noise/salt to the skewed keys such that the join keys are distributed evenly across partitions.

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled",True)

# COMMAND ----------

# Set the number of shuffle partitions - default - 200
# spark.conf.set("spark.sql.shuffle.partitions", 200)
# Too many shuffle partitions will keep the job busy in netwrok and disk IO operations

# COMMAND ----------

df_joined_broadcast=user_interactions_df.join(broadcast(user_metadata_df),on=user_interactions_df.user_id==user_metadata_df.user_id,how='left_outer').select(user_interactions_df["*"],user_metadata_df["subscription_type"])

df_joined_broadcast.write.format('noop').mode('overwrite').save()

# COMMAND ----------

# MAGIC %md
# MAGIC Join with AQE enabled reading the large table and broadcasting the small table across executors to perfrom join thereby avoiding the data exchange. you can see the stage 116 has been skipped.

# COMMAND ----------

df_joined_broadcast.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC I would execute the join via broadcast join method to reduce the shuffling as shuffling is an expensive operation and the spark UI proved the same.
# MAGIC
# MAGIC In **Broadcast join** - The samll table is sent to executors where the big data resides such that shuffle is minimized. Driver is responsible for sending the small table so driver memory has to be big enough to accomodate the size of small table else we would encounter **Driver Out of Memory**.
# MAGIC
# MAGIC In **sort merge join** - first shuffle is performed to group the keys of same value then the keys are sorted with in the partition and then the keys are merged to produce a final result.
# MAGIC
# MAGIC I can optimize this join further either by  
# MAGIC 1. **Bucketing** where both the datasets are placed in equal number of buckets based on user_id field. Spark uses Murmur hash algorithm to hash the join key into buckets. During the join bucket with same number will be sent to the same executor. We may encounter spillage to disk while using Bucketing but I can optimize it further to avoid the spillage by using a technique called Salting.
# MAGIC 2. **Salting** - is to add noise/salt to the skewed join keys such that the join keys are distributed evenly across partitions. We usually choose the range of noise with respect to shuffle partitions such that the keys can be evenly distributed among partitions.
# MAGIC
# MAGIC Below are some points to remember while bucketing:
# MAGIC
# MAGIC -> Joining key should be same as bucket key.
# MAGIC
# MAGIC -> Both tables should be bucketed into same number of buckets.
# MAGIC
# MAGIC So it is important to decide the bucket key and number of buckets as too many buckets can lead to small file issue.
# MAGIC

# COMMAND ----------


