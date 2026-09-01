# Databricks Data Engineer Practice Quiz — Aug 22
*(Copy each question + options into Google Forms. Q7 is a "Checkboxes" question — Choose 2; all others are "Multiple choice")*

---

**Q1.** A data engineer is ingesting JSON files from cloud object storage using Databricks Auto Loader. The source folder may occasionally receive large files of data, which risks overwhelming the stream. To ensure predictable micro-batch sizes, the team wants to throttle ingestion based on the volume of data scanned at 1GB, regardless of the number of files.

Which Auto Loader configuration should the data engineer use to achieve this?

- A. Configure cloudFiles.maxSizePerTrigger with 1GB to place a limit.
- B. Configure cloudFiles.maxPartitionBytes with 1GB to limit data in each partition.
- C. Configure cloudFiles.maxFilesPerTrigger and estimate the average file size to approximate a size-based throttle of 1GB.
- D. Configure cloudFiles.maxBytesPerTrigger with 1GB to place a limit.

---

**Q2.** A data engineer is analyzing transactional data in a PySpark DataFrame sales_df. The objective is to compute a cumulative sum... The data engineer needs to generate a DataFrame that ranks salespeople within each region based on their total cumulative sales, with the highest seller ranked as 1.

Which code snippet will perform this ranking?

- A.
```
window = Window.partitionBy("region").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = sales_df.withColumn("rank", F.sum("sales").over(window))
```
- B.
```
# Aggregate first to get total sales per person
agg_df = sales_df.groupBy("region", "salesperson").agg(F.sum("sales").alias("total_sales"))

# Define Window to rank within region by total sales
window = Window.partitionBy("region").orderBy(F.desc("total_sales"))

# Apply Rank
result_df = agg_df.withColumn("rank", F.rank().over(window))
```
- C.
```
window = Window.orderBy(F.desc("sales"))
df = sales_df.withColumn("rank", F.rank().over(window))
```
- D.
```
window = Window.partitionBy("region")
df = sales_df.withColumn("rank", F.rank().over(window))
```

---

**Q3.** A company processes task events in near real time using Lakeflow Spark Declarative Pipelines and exposes a streaming table tasks_status for BI. This table stores the latest status per task with columns: task_id, task_name, task_owner, task_status, task_event_time, and has deletion vectors, row tracking, and change data feed enabled.

A data engineer must build a new Lakeflow Spark Declarative Pipeline to enrich tasks_status in near real time by adding a new column with the task owner's department, looked up from a static dimension table employee.

How should this enrichment be implemented?

- A. Create a new Lakeflow Spark Declarative Pipeline: use the readStream() function with the option skipChangeCommits...
- B. Create a new Lakeflow Spark Declarative pipeline; use the readStream() function to read tasks_status table; enrich with the employee table; store the result in a new streaming table.
- C. Create a new Lakeflow Spark Declarative Pipeline: use the read() function to read tasks_status table; enrich with employee table; store the result in a materialized view.
- D. Create a new Lakeflow Spark Declarative Pipelines: use readStream() function with option readChangeFeed to read tasks_status table CDF; enrich with the employee table; create a new streaming table as the result table and use apply_changes() function to process the changes from the enriched CDF.

---

**Q4.** A high-throughput streaming job inserts millions of small rows per minute into a Delta table. The job is becoming unstable due to "driver overhead" managing the commits, and query performance is degrading due to the "Small Files Problem". The engineer wants to optimize the write side to produce larger files automatically during the ingestion, rather than relying solely on post-hoc OPTIMIZE jobs.

Which table property enables the write job to shuffle data before writing to ensure optimal file sizing?

- A. delta.targetFileSize = '128mb'
- B. spark.databricks.delta.properties.manager.enable = true
- C. delta.autoOptimize.autoCompact = true
- D. delta.autoOptimize.optimizeWrite = true

---

**Q5.** A bank must archive a snapshot of their loans table exactly as it existed at 2023-12-31 23:59:59 for regulatory auditing. This archive must be immutable and stored in a separate "Audit" storage container to prevent tampering.

Which SQL command performs this archival correctly?

- A.
```
RESTORE TABLE prod_catalog.loans
TO TIMESTAMP '2023-12-31 23:59:59';
```
- B.
```
CREATE TABLE audit_catalog.loans_archive
AS SELECT * FROM prod_catalog.loans
TIMESTAMP AS OF '2023-12-31 23:59:59';
```
- C.
```
COPY INTO audit_catalog.loans_archive
FROM prod_catalog.loans
OPTIONS (timestamp '2023-12-31 23:59:59');
```
- D.
```
CREATE TABLE audit_catalog.loans_archive
DEEP CLONE prod_catalog.loans
VERSION AS OF '2023-12-31 23:59:59'
LOCATION 's3://audit-bucket/loans_2023';
```

---

**Q6.** A data engineer attempts to run `VACUUM target_table RETAIN 24 HOURS` on a large Delta table. The command fails immediately with an exception stating that the retention period is shorter than the configured safety check threshold.

What is the primary reason Databricks enforces this safety check by default?

- A. To ensure that the Delta Log size does not become too small, which would impact performance.
- B. To prevent the deletion of files that might currently be in use by long-running snapshot queries or concurrent streams, ensuring consistency.
- C. To prevent the accidental deletion of the current version of the data.
- D. To force users to pay for at least 7 days of storage costs on cloud providers.

---

**Q7.** *(Choose 2)* A data engineer is tuning a slow MERGE INTO operation on a large Delta table. The merge condition is `target.id = source.id`. The query profile indicates massive shuffling and scanning of the entire target table.

Which two optimization techniques are most effective for improving the performance of this specific operation? *(Select all that apply)*

- A. Add a Source Predicate to the join condition (e.g., target.date = source.date) if date is a partition column.
- B. Use a Broadcast Hint on the target table side of the merge.
- C. Increase the log retention duration.
- D. Enable Low Shuffle Merge using spark.databricks.delta.merge.enableLowShuffle = true.
- E. Apply Z-Ordering on the id column of the target table

---

**Q8.** A data engineer creates a new Delta table web_events intended to store petabytes of clickstream data. The primary query patterns filter by event_timestamp and user_id. However, query patterns are expected to evolve over the next year as new product features are launched (e.g., filtering by session_id). The engineer wants a layout strategy that adapts to these changes without requiring a full table rewrite.

Which optimization strategy should be selected?

- A. Partition by event_timestamp.
- B. Enable Liquid Clustering with the initial keys (event_timestamp, user_id)
- C. Use Hive-style partitioning on event_timestamp and user_id.
- D. Z-Order by (event_timestamp, user_id)

---

**Q9.** A data engineer needs to build an ingestion pipeline moving CSV files from Azure Data Lake Storage (ADLS) Gen2 into a Delta table. The pipeline must run in near-real-time. The source system occasionally uploads the same file twice (duplicate filename and content). The goal is to ensure exactly-once processing of files with minimal custom coding.

Which solution should the data engineer implement?

- A. Use Auto Loader (cloudFiles) with cloudFiles.allowDuplicates=false and Directory Listing mode
- B. Use Auto Loader (cloudFiles) with File Notification mode.
- C. Use spark.readStream with ignoreDuplicates=true to deduplicate rows based on a file hash column.
- D. Use a scheduled batch job with COPY INTO and a custom logic to check file existence.

---

**Q10.** A data engineer needs to ingest millions of image files (.jpg) from S3 into a Delta table for a Machine Learning pipeline. The table should contain the raw bytes of the image and the file metadata (path).

Which Auto Loader format configuration should be used?

- A. .option("cloudFiles.format", "bytes")
- B. .option("cloudFiles.format", "binaryFile")
- C. .option("cloudFiles.format", "image")
- D. .option("cloudFiles.format", "text")

---

**Q11.** A Structured Streaming pipeline joins a high-velocity stream of ad_impressions (Fact) with a static Delta table ad_campaigns (Dimension) to enrich the stream with campaign metadata. The ad_campaigns table is updated once a day. The engineer notices that the join performance degrades over time because the stream does not pick up the updates to the static table automatically, forcing a restart of the stream to refresh the cache.

How should the pipeline be architected to ensure the stream sees the latest static data without restarting, while maintaining performance?

- A. Apply a UDF to lookup the value from the static table for every row.
- B. This is a limitation of Spark; the stream must always be restarted to refresh static data.
- C. No action is needed; Spark Structured Streaming automatically refreshes static tables in a join every time they are updated.
- D. Configure the stream to read the static table as a streaming source with readChangeFeed, enabling a Stream-Stream join.
