# Databricks notebook source
# MAGIC %md
# MAGIC This script is only meant to be run once to generate the Initial Datasets.

# COMMAND ----------

# MAGIC %md
# MAGIC Below is the script to generate 2 datasets namely User Interactions (~ 1 TB, partitioned by date) and User Metadata (~ 100 GB, partitioned by country)

# COMMAND ----------

import csv
import random
from datetime import datetime, timedelta

def generate_user_id():
    return f"u{random.randint(1, 1000000):06d}"

def generate_timestamp():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 12, 31)
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
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2023, 12, 31)
    return start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

def generate_country():
    return random.choice(['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'IN', 'BR', 'MX'])

def generate_device_type():
    return random.choice(['iPhone', 'iPad', 'Android Phone', 'Android Tablet', 'Windows', 'Mac'])

def generate_subscription_type():
    return random.choice(['free', 'basic', 'premium', 'enterprise'])

def generate_user_interactions(num_records, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['user_id', 'timestamp', 'action_type', 'page_id', 'duration_ms', 'app_version'])
        for i in range(num_records):
            writer.writerow([
                generate_user_id(),
                generate_timestamp().strftime("%Y-%m-%d %H:%M:%S"),
                generate_action_type(),
                generate_page_id(),
                generate_duration_ms(),
                generate_app_version()
            ])
            if i % 10000 == 0:  # Progress update for large datasets
                print(f"{i} records written...")

def generate_user_metadata(num_records, filename):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['user_id', 'join_date', 'country', 'device_type', 'subscription_type'])
        for i in range(num_records):
            writer.writerow([
                generate_user_id(),
                generate_join_date().strftime("%Y-%m-%d"),
                generate_country(),
                generate_device_type(),
                generate_subscription_type()
            ])
            if i % 10000 == 0:  # Progress update for large datasets
                print(f"{i} records written...")

# Create directory in DBFS
dbutils.fs.mkdirs("/mnt/persistent/Goodnotes/input")

# Generate user interactions
generate_user_interactions(1000000, '/dbfs/mnt/persistent/Goodnotes/input/user_interactions.csv')

# Generate user metadata
generate_user_metadata(100000, '/dbfs/mnt/persistent/Goodnotes/input/user_metadata.csv')


print("Sample datasets generated successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC Checking if the files have been written correctly

# COMMAND ----------

# MAGIC %fs ls /mnt/persistent/Goodnotes/input

# COMMAND ----------


