# Goodnotes
Insights: DE Challenge
The task is to design and implement a highly optimized Spark-based system to process and analyze the data, deriving valuable insights about user behavior and app performance.

Setup Instructions:
I've used Databricks premium to create the datasets and process them in association with S3 to store the data. I went ahead with the approach to bypass the requirement to set up Spark on my personal computer. I've integrated git with Databricks for version control and scheduled the jobs via Databricks Job Runs in batch mode. The schema of the datasets and tasks to do are as follows:

Dataset
You are provided with two large datasets:

User Interactions (~ 1 TB, partitioned by date)

Schema: (user_id: String, timestamp: Timestamp, action_type: String, page_id: String, duration_ms: Long, app_version: String)
Example: ("u123", "2023-07-01 14:30:15", "page_view", "p456", 12000, "5.7.3")
User Metadata (~ 100 GB, partitioned by country)

Schema: (user_id: String, join_date: Date, country: String, device_type: String, subscription_type: String)
Example: ("u123", "2022-01-15", "US", "iPad", "premium")

Tasks
1) Calculate Daily Active Users (DAU) and Monthly Active Users (MAU) for the past year:

Define clear criteria for what constitutes an "active" user
Implement a solution that scales efficiently for large datasets
[Optional] Explain how you:
Handle outliers and extremely long duration values, OR
Describe challenges you might face while creating similar metrics and how you would address them

2) Calculate session-based metrics:

Calculate metrics including:
Average session duration
Actions per session
Define clear criteria for what constitutes a "session"
[Optional] Provide analysis of the results if time permits

3) Spark UI Analysis

a. Analyze the Spark UI for the above data pipeline job and identify any findings or bottlenecks:

Provide a detailed walkthrough (using screenshots or a screen recording with loom.com) of your analysis process
Identify specific areas for potential optimization based on the Spark UI data
b. Explain or implement improvements based on your Spark UI analysis:

Choose one of your proposed optimizations and implement it (a high-level explanation via loom.com recording is also acceptable)
[Optional] Provide before and after comparisons of relevant Spark UI metrics to demonstrate the improvement

c. Explain key areas for optimization:

For each identified bottleneck, provide a hypothesis about its cause and a proposed solution (a high-level explanation via loom.com recording is also acceptable)
[Optional] Discuss how you would validate the impact of your proposed optimizations
NOTE: If you don't have time to document comprehensively, you can record a video walkthrough using loom.com to explain your findings, analysis, and answers to questions 4 and 5.

4) Data Processing and Optimization

a. In the above Spark job, join the User Interactions and User Metadata datasets efficiently while handling data skew:

Explain your approach to mitigating skew in the join operation, considering that certain user_ids may be significantly more frequent
Implement and justify your choice of join strategy (e.g., broadcast join, shuffle hash join, sort merge join)

5) Design and implement optimizations for the Spark job to handle large-scale data processing efficiently:

a. Analyze and optimize operations - Provide two files: pre-optimization and post-optimization

Provide a detailed analysis of the execution plan, identifying shuffle bottlenecks [a high-level analysis via loom.com recording is also acceptable]
Explain how changes are implemented to improve the plan
[Optional]: Justify your chosen approach with performance metrics

b. Memory Management and Configuration:

Design a memory tuning strategy for executors and drivers
Document memory allocation calculations for different data scales (100GB, 500GB, 1TB)
[Optional]: Implement safeguards against OOM errors for skewed data

c. Parallelism and Resource Utilization:

Determine optimal parallelism based on data volume and cluster resources
Implement dynamic partition tuning
Provide configuration recommendations for:
Number of executors
Cores per executor
Memory settings
Shuffle partitions
[Optional]: Include benchmarks comparing different configurations

d. [Optional] Advanced optimization techniques:

Implement custom partitioning strategies for skewed keys
Design and implement data preprocessing steps to optimize downstream operations
[Optional]: Provide metrics showing the impact of your optimizations
