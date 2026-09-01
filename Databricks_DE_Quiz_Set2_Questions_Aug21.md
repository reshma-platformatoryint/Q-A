# Databricks Data Engineer Practice Quiz — Set 2
*(Copy each question + options into Google Forms as a "Multiple choice" question)*

---

**Q1.** A data engineer has implemented the following Auto Loader stream to incrementally ingest a large volume of JSON files from cloud storage:
```
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    ___________
    .load("/path/to/files")
)
```

By default, Auto Loader infers the schema by sampling the first 50 GB or 1000 files it discovers. However, the data engineer wants to avoid re-sampling and reduce the cost of schema inference in subsequent runs, while still tracking schema changes over time.

Which option correctly fills in the blank to meet the specified requirement?

- A. .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
- B. .option("checkpointLocation", "/path/to/checkpoint")
- C. .option("mergeSchema", true)
- D. .option("cloudFiles.schemaLocation", "/path/to/checkpoint")

---

**Q2.** A junior data engineer creates a Databricks job with 15 notebook tasks, each performing the same data validation logic on 15 different tables. Each task depends on the completion of the previous one, making the workflow long and difficult to maintain.

What would be a more efficient and scalable solution for this use case?

- A. Use a foreach task to run the same validation notebook for each table in parallel, passing the table name as a parameter
- B. Configure the 15 notebook tasks to run in parallel, each with a separate cluster configuration
- C. Combine all table validations into one large notebook and loop through all tables sequentially
- D. Schedule 15 separate jobs instead of having multiple tasks in one job

---

**Q3.** The data engineering team has a Delta Lake table named 'daily_activities' that is completely overwritten each night with new data received from the source system.

For auditing purposes, the team wants to set up a post-processing task that uses Delta Lake Time Travel functionality to determine the difference between the new version and the previous version of the table. They start by getting the current table version via this code:
```
current_version = spark.sql("SELECT max(version) FROM (DESCRIBE HISTORY daily_activities)").collect()[0][0]
```

Which of the following queries can be used by the team to complete this task?

- A.
```
SELECT * FROM daily_activities
MINUS
SELECT * FROM daily_activities AS VERSION = {current_version-1}
```
- B.
```
SELECT * FROM daily_activities
EXCEPT
SELECT * FROM daily_activities@v{current_version-1}
```
- C.
```
SELECT * FROM daily_activities
INTERSECT
SELECT * FROM daily_activities AS VERSION = {current_version-1}
```
- D.
```
SELECT * FROM daily_activities
UNION
SELECT * FROM daily_activities AS VERSION = {current_version-1}
```

---

**Q4.** A data engineer has the following logic to handle duplicates in Spark Structured Streaming:
```
(spark.readStream
    .table("bronze")
    .filter("topic = 'orders'")
    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
    .select("v.*")
    .withWatermark("order_timestamp", "30 seconds")
    .dropDuplicates(["order_id", "order_timestamp"]))
```

However, they notice that this logic is not sufficient to prevent duplicates for events that arrive later than the watermark threshold.

Which of the following code snippets can the data engineer include in a `foreachBatch` function to completely handle streaming duplicates?

- A.
```
(spark.readStream
.table("microbatch")
.withWatermark("order_timestamp", "7 days")
.dropDuplicates(["order_id", "order_timestamp"]))
```
- B.
```
COPY INTO orders_silver
FROM microbatch
DISTINCT ALL
COPY_OPTIONS ('mergeSchema' = 'true');
```
- C.
```
APPLY CHANGES INTO orders_silver a
FROM STREAM(microbatch)
KEYS (order_id, order_timestamp)
SEQUENCE BY order_timestamp
COLUMNS *
```
- D.
```
MERGE INTO orders_silver a
USING microbatch b
ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
WHEN NOT MATCHED THEN INSERT *
```

---

**Q5.** The data engineering team has a table 'orders_backup' that was created using Delta Lake's SHALLOW CLONE functionality from the table 'orders'. Recently, the team started getting an error when querying the 'orders_backup' table indicating that some data files are no longer present.

Which of the following correctly explains this error?

- A. The OPTIMIZE command was run on the orders_backup table
- B. The VACUUM command was run on the orders_backup table
- C. The OPTIMIZE command was run on the orders table
- D. The VACUUM command was run on the orders table

---

**Q6.** A data engineer has defined the following data quality constraint in a SDP pipeline:
```
CONSTRAINT valid_id EXPECT (id IS NOT NULL) ___________
```

Which clause correctly fills in the blank to immediately stop execution when a record violates this constraint?

- A. ON VIOLATION STOP
- B. ON VIOLATION FAIL UPDATE
- C. ON VIOLATION FAIL PIPELINE
- D. ON VIOLATION DROP ROW

---

**Q7.** Given the following Structured Streaming query:
```
(spark.table("orders")
    .withColumn("total_after_tax", col("total")+col("tax"))
    .writeStream
    .option("checkpointLocation", checkpointPath)
    .outputMode("append")
    ___________
    .table("new_orders")
)
```

Fill in the blank to make the query execute a micro-batch to process data every 2 minutes

- A. trigger(once="2 minutes")
- B. processingTime("2 minutes")
- C. trigger("2 minutes")
- D. trigger(processingTime="2 minutes")

---

**Q8.** A data engineer is configuring the following Databricks Auto Loader stream to ingest JSON data from an S3 bucket:
```
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "s3://shop/checkpoints/orders") \
    .option("cloudFiles.schemaEvolutionMode", "___________") \
    .load("s3://shop/raw/orders/json/") \
.writeStream \
    .option("checkpointLocation", "s3://shop/checkpoints/orders") \
    .start("orders_table")
```

The pipeline should fail when new columns are detected in the incoming data, but those new columns should still be added to the schema so that subsequent runs can resume successfully with the updated schema. Existing columns must retain their data types.

Which option correctly fills in the blank to meet the specified requirement?

- A. rescue
- B. addNewColumns
- C. failOnNewColumns
- D. none

---

**Q9.** Which of the following statements best describes Auto Loader?

- A. Auto loader allows cloning a source Delta table to a target destination at a specific version.
- B. Auto loader monitors a source location, in which files accumulate, to identify and ingest only new arriving files with each command run. While the files that have already been ingested in previous runs are skipped.
- C. Auto loader allows applying Change Data Capture (CDC) feed to update tables based on changes captured in source data.
- D. Auto loader enables efficient insert, update, deletes, and rollback capabilities by adding a storage layer that provides better data reliability to data lakes.
