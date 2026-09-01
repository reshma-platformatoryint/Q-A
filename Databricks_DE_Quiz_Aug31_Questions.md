# Databricks Data Engineer Practice Quiz — Aug 31
*(Copy each question + options into Google Forms. Q19 is a "Checkboxes" question — Choose 2; all others are "Multiple choice")*

---

**Q1.** A data engineer is processing a stream of user profile updates (user_id, email, updated_at). They need to upsert (update if exists, insert if new) these records into a Delta table.

Since Structured Streaming does not have a native `.upsert()` sink, which pattern should the engineer use to implement this logic?

- A. Use `df.writeStream.foreachBatch(upsert_function).start()` where upsert_function performs a MERGE INTO operation.
- B. Use `df.writeStream.outputMode("update")` directly to the Delta table.
- C. Convert the stream to a static DataFrame and run MERGE once.
- D. Use `df.writeStream.trigger(processingTime='1 minute')` to force batch processing.

---

**Q2.** An ETL pipeline runs nightly to ingest data from various sources. As part of the pipeline logic, the script determines the Data Sensitivity Level (e.g., 'Confidential', 'Public') based on the source metadata. The engineer wants to apply this classification as a Tag on the target table in Unity Catalog programmatically within the SQL transformation script.

Which SQL command achieves this?

- A. GRANT TAG 'Confidential' ON TABLE my_table
- B. UPDATE TABLE my_table SET ATTRIBUTE tags = ('sensitivity' = 'Confidential')
- C. ALTER TABLE my_table SET TAGS ('sensitivity' = 'Confidential')
- D. COMMENT ON TABLE my_table IS 'sensitivity: Confidential'

---

**Q3.** A data engineer has deployed a Lakeflow Spark Declarative pipeline currently running in Continuous mode to minimize latency. However, the costs are too high, and the business stakeholders have agreed that a latency of "Updated once every hour" is acceptable.

The engineer wants to switch the pipeline execution to process all available data exactly once per run and then shut down the cluster to save costs, scheduled by an external orchestration workflow every hour.

Which setting in the Lakeflow Spark Declarative pipeline configuration (JSON) achieves this behavior?

- A.
```
"trigger": {
    "interval": "1 hour"
}
```
- B.
```
{
    "configuration":{
        "pipelines.trigger.interval": "1 hour"
    }
}
```
- C.
```
{
    "continuous": false
}
```
- D.
```
"trigger": {
    "once": true
}
```

---

**Q4.** A data engineer is designing a Lakeflow Spark Declarative Pipeline. The requirement for the Silver table is strictly: "Do not allow any record with a negative price to be written to the table. However, do not stop the pipeline; simply discard the invalid records and log the occurrence in the event log so we can track data quality trends."

Which Lakeflow Spark Declarative Pipeline Expectation is the correct implementation of this rule?

- A. `@dp.expect_or_fail("positive_price", "price > 0")`
- B. `@dp.expect("positive_price", "price > 0")`
- C. `@dp.expect_or_drop("positive_price", "price > 0")`
- D. `@dp.constraint("positive_price", "price > 0")`

---

**Q5.** A retail system sends data updates to Databricks. Occasionally, a "Return" (Refund) event arrives before the "Purchase" event due to weird asynchronous processing in the legacy mainframe. The LSDP pipeline uses APPLY CHANGES INTO to create a customer_orders state table.

If LSDP processes the "Return" (Sequence 100) first, and the "Purchase" (Sequence 90) arrives later, how does LSDP prevent the "Purchase" from overwriting the "Return" state?

- A. It relies on the VACUUM command to remove the out-of-order files.
- B. It deletes the row because conflicting updates are always treated as invalid data.
- C. It triggers a warning in the event log but applies the Purchase anyway because it is an INSERT operation.
- D. It compares the transaction timestamps; since the Purchase (Seq 90) is older than the current state (Seq 100), LSDP ignores the Purchase event for that key.

---

**Q6.** A data engineer is building a Structured Streaming application to track user sessions on a website. A "session" is defined as a series of events from the same user_id where no two events are more than 30 minutes apart. If 30 minutes pass without an event, the session closes. The engineer needs to implement this custom stateful logic using PySpark.

Which code pattern correctly initializes the stateful operation required for this sessionization logic?

- A.
```
df.groupBy("user_id").applyInPandas(
    session_func,
    schema="user_id string, session_id string, duration long"
)
```
- B.
```
session_table = spark.read.table("sessions")
df.join(session_table, "user_id", "left_outer")
```
- C.
```
df.groupByKey(lambda x: x.user_id).flatMapGroupsWithState(
    session_func,
    OutputMode.Append(),
    GroupStateTimeout.EventTimeTimeout()
)
```
- D.
```
window = Window.partitionBy("user_id").orderBy("timestamp")
df.withColumn("session_flag", F.lag("timestamp", 1).over(window))
```

---

**Q7.** A data engineer runs a daily ETL job that uses MERGE to upsert a source DataFrame into a target Delta table. Sometimes the source adds new columns (e.g., user_segment), causing the job to fail with a schema mismatch. The engineer wants MERGE to automatically evolve the target schema to include new source columns, without manual updates.

Which Delta configuration and MERGE syntax should be used to enable automatic schema evolution?

- A. Enable `spark.databricks.delta.schema.autoMerge.enabled = true` and use `MERGE INTO target USING source` with `WHEN MATCHED THEN UPDATE SET *` and `WHEN NOT MATCHED THEN INSERT *`.
- B. Use `.option("mergeSchema", "true")` in the DataStreamWriter.
- C. It is not possible; Schema Evolution is only supported in append (Insert) operations, not Merge.
- D. Run `ALTER TABLE target ADD COLUMNS ...` inside a try/except block before the Merge.

---

**Q8.** A data engineer is building a Lakeflow Spark Declarative pipeline to process a CDC stream with INSERT and UPDATE events. The stream can contain duplicate events and late-arriving updates for the same transaction ID. The target table must reflect only the latest state of each record.

How should the APPLY CHANGES INTO command be configured to handle duplicates and late updates correctly?

- A. You must set the table property `delta.enableChangeDataFeed = true` on the target table to automatically filter out duplicates.
- B. You must use `IGNORE DUPLICATES` in the `dp.read_stream()` method configuration.
- C. You must define a SEQUENCE BY clause using a monotonically increasing column (like a timestamp or version number) to correctly order events and resolve conflicts.
- D. You must perform a `groupBy("id").agg(max("timestamp"))` on the source dataframe before passing it to APPLY CHANGES INTO

---

**Q9.** A data engineer is creating a new Structured Streaming pipeline to process a Delta table user_events. This table has been accumulating data for 2 years. The business requirement is to process all existing historical data first, and then continue processing new data as it arrives.

Which option must be configured in the DataStreamReader?

- A. .option("readChangeFeed", "true")
- B. .option("ignoreChanges", "true")
- C. .option("startingVersion", "latest")
- D. .option("startingVersion", 0)

---

**Q10.** A data engineer suspects that a Join operation in their nightly ETL job is suffering from Data Skew, causing the job to hang at 99% completion for hours. They navigate to the Spark UI to confirm this hypothesis.

Which specific visual indicator in the Stages tab of the Spark UI would confirm the presence of severe data skew?

- A. The DAG Visualization shows a BroadcastExchange operator.
- B. The Event Timeline shows a "waterfall" pattern where most tasks finish quickly (green bars), but one or two tasks remain running (long green bars) for much longer than the others.
- C. The "Input Size" column shows 0 bytes for most tasks.
- D. The "Shuffle Write" metric is exactly equal to the "Shuffle Read" metric for all tasks.

---

**Q11.** A Data Engineering lead is investigating a complaint that the "Daily Sales Dashboard" in Databricks SQL is loading slowly. The dashboard is backed by a Serverless SQL Warehouse. The lead suspects that a specific query scanning the fact_sales table is performing a full table scan instead of using partition pruning.

Which approach allows the lead to identify the specific SQL query text, its duration, and the "Pruning" metrics (files read vs. files skipped) for queries executed in the last 24 hours?

- A. Query the system.query.history table filtering by warehouse_id and start_time, and inspect the statement_text and metrics columns.
- B. Run DESCRIBE HISTORY fact_sales to see the read operations performed on the table.
- C. Check the system.billing.usage table for records where sku_name contains "Serverless".
- D. Go to the Spark UI of the Serverless Warehouse and look at the "Executors" tab.

---

**Q12.** A data engineer runs the following PySpark code to query a large Delta table sensor_logs (10 TB). The table is partitioned by date.
```
# Query filtering by a high-cardinality column 'sensor_id'
df = spark.read.format("delta") \
    .load("/data/sensor_logs") \
    .filter("sensor_id = 'SN-998877'")

# Trigger action
count = df.count()
```

Upon analyzing the query profile, the engineer notices that the Scan operator read 95% of the files in the table, even though SN-998877 only accounts for 0.01% of the data. The table properties show standard settings.

What is the technical reason for this ineffective data skipping?

- A. The sensor_id column type is String, which Delta Lake does not support for Min/Max statistics collection by default.
- B. The table is missing the delta.enableDataSkipping = true property, which is disabled by default on large tables.
- C. The data within the date partitions is randomly distributed regarding sensor_id, causing the Min/Max ranges of almost every file to overlap with the requested ID.
- D. The query engine failed to push down the predicate because .filter() was called after .load()

---

**Q13.** A data engineer creates a temporary view v_eur_sales in a Lakeflow Spark Declarative Pipeline that converts USD sales to EUR using a hardcoded exchange rate of 0.85. The engineer pushes a code update changing the rate to 0.90.

What happens to v_eur_sales when the pipeline performs the next update?

- A. The temporary view definition used by the pipeline is updated. When the view is evaluated, results are calculated using 0.90. No physical data for the view is rewritten.
- B. The view is incrementally updated; only new rows use the 0.90 rate, while old rows remain at 0.85
- C. The underlying storage for the view is deleted and recomputed from scratch.
- D. The pipeline fails because changing logic on an existing view requires a Full Refresh

---

**Q14.** A data engineer is building a downstream pipeline that consumes data from a "Silver" Delta table named user_profiles. This Silver table is maintained as an SCD Type 1 table (overwritten in-place) using APPLY CHANGES INTO. The downstream job needs to stream only the incremental updates (e.g., email changes, new user registrations) to propagate them to a "Gold" aggregation table.

Which PySpark configuration correctly enables reading the stream of updates from this overwriting source?

- A.
```
spark.readStream.format("delta") \
    .option("maxFilesPerTrigger", 1) \
    .load("/data/user_profiles")
```
- B.
```
spark.readStream.format("delta") \
    .option("ignoreDeletes", "true") \
    .load("/data/user_profiles")
```
- C.
```
spark.readStream.format("delta") \
    .option("ignoreChanges", "true") \
    .load("/data/user_profiles")
```
- D.
```
spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .load("/data/user_profiles")
```

---

**Q15.** A logistics company tracks the location of vehicles. The table vehicle_pings contains vehicle_id, latitude, longitude, and timestamp. The data engineer needs to calculate the distance traveled between the current ping and the previous ping for each vehicle to determine speed.

Which PySpark window logic correctly accesses the previous ping's location for the same vehicle?

- A.
```
window = Window.partitionBy("vehicle_id")
prev_lat = F.rank().over(window)
```
- B.
```
window = Window.orderBy("timestamp")
prev_lat = F.lag("latitude", 1).over(window)
```
- C.
```
window = Window.partitionBy("vehicle_id").orderBy("timestamp")
prev_lat = F.lead("latitude", 1).over(window)
```
- D.
```
window = Window.partitionBy("vehicle_id").orderBy("timestamp")
prev_lat = F.lag("latitude", 1).over(window)
```

---

**Q16.** A data engineer has a sales table with columns region, amount, and status ('completed', 'failed'). They need to produce a report showing the total sales amount per region, but they want two separate columns: total_completed_sales and total_failed_sales. They want to achieve this with a single pass over the data (one groupBy) for performance.

Which PySpark code snippet achieves this pivoted aggregation efficiently?

- A. `df.select("region", "amount").where("status IN ('completed', 'failed')").groupBy("region").sum()`
- B. `df.groupBy("region", "status").agg(F.sum("amount")).pivot("status")`
- C.
```
df_completed = df.filter("status = 'completed'").groupBy("region").agg(F.sum("amount"))
df_failed = df.filter("status = 'failed'").groupBy("region").agg(F.sum("amount"))
df_completed.join(df_failed, "region")
```
- D.
```
df.groupBy("region").agg(
    F.sum(F.when(F.col("status") == "completed", F.col("amount")).otherwise(0)).alias("total_completed"),
    F.sum(F.when(F.col("status") == "failed", F.col("amount")).otherwise(0)).alias("total_failed")
)
```

---

**Q17.** A data engineer analyzes IoT sensor data to detect anomalies. They need to calculate a 5-minute moving average of the temperature for each sensor_id. The window must be based on the Event Time (timestamp), not the row count (e.g., "last 5 rows"), because sensors might report data at irregular intervals (gaps or bursts).

Which PySpark Window definition correctly defines this time-based frame?

- A.
```
w = Window.partitionBy("sensor_id").orderBy("timestamp") \
    .rowsBetween(-5, Window.currentRow)
```
- B.
```
w = Window.partitionBy("sensor_id") \
    .rowsBetween(Window.currentRow - 300, Window.currentRow)
```
- C.
```
w = Window.partitionBy("sensor_id").orderBy("timestamp") \
    .rangeBetween(Window.unboundedPreceding, Window.currentRow)
```
- D.
```
w = Window.partitionBy("sensor_id").orderBy("timestamp") \
    .rangeBetween(Window.currentRow - 300, Window.currentRow)
```

---

**Q18.** A data engineering team wants to refactor a monolithic, 500-line PySpark script into a modular, testable codebase. The specific goal is to normalize customer emails to lowercase and filter out inactive users. They want to be able to unit test the "normalization" logic and the "filtering" logic independently using small mock DataFrames, without running the full pipeline.

Which code structure demonstrates the correct usage of DataFrame.transform() to achieve this modularity?

- A.
```
def process_customers(df):
    df_lower = df.withColumn("email", F.lower(F.col("email")))
    df_active = df_lower.filter(F.col("is_active") == True)
    return df_active

final_df = process_customers(raw_df)
```
- B.
```
final_df = raw_df.transform(lambda d: d.withColumn("email", F.lower(F.col("email")))) \
                  .transform(lambda d: d.filter(F.col("is_active") == True))
```
- C.
```
def normalize_email(df):
    return df.withColumn("email", F.lower(F.col("email")))

def filter_active(df):
    return df.filter(F.col("is_active") == True)

final_df = raw_df.transform(normalize_email).transform(filter_active)
```
- D.
```
class CustomerETL:
    def run(self, df):
        return df.withColumn("email", F.lower(F.col("email"))).filter("is_active = true")
```

---

**Q19.** *(Choose 2)* A data architect is redesigning a massive 500 TB Delta table (iot_events) that is suffering from performance issues. Currently, the table uses Hive-style partitioning by date and Z-Ordering by device_id. However, the team is facing two major problems:

1. Data Skew: Some devices generate 1000 times more data than others, creating unbalanced partitions and slow tasks ("stragglers") during processing.
2. Rigidity: Query patterns are changing, and users now frequently filter by region. However, adding this column as a partition key would require rewriting the entire historical table.

The architect decides to migrate to Liquid Clustering. Which two key technical benefits of Liquid Clustering directly resolve these specific problems? *(Select all that apply)*

- A. Liquid Clustering automatically enforces Primary Key and Foreign Key constraints (PRIMARY KEY, FOREIGN KEY) to guarantee referential integrity during writes.
- B. Liquid Clustering automatically manages Data Skew by decoupling the physical data layout from the directory structure, creating files of uniform size regardless of the key cardinality.
- C. Liquid Clustering uses a rigid physical directory structure based on the clustering keys, which eliminates the need to maintain the transaction log
- D. Liquid Clustering allows changing the clustering keys (CLUSTER BY) at any time without needing to rewrite existing data; new data follows the new layout and old data adapts lazily ("lazy evolution")
- E. Liquid Clustering disables "Data Skipping" optimization to favor faster full scans for aggregation queries.

---

**Q20.** A Lakeflow Spark Declarative Pipeline runs a heavy weekly batch backfill followed by light streaming updates. The batch workload needs fast scale-up to handle high compute and memory demand, while the streaming phase should scale down quickly to reduce costs.

Which Lakeflow Spark Declarative Pipeline cluster configuration or policy is best for this bursty workload?

- A. Enable Enhanced Autoscaling in the Lakeflow Spark Declarative Pipeline settings.
- B. Create a standard Job Cluster with spark.dynamicAllocation.enabled set to true.
- C. Use a "High Concurrency" cluster mode with Spot instances.
- D. Use a fixed-size cluster optimized for the peak load (Maximum capacity).
