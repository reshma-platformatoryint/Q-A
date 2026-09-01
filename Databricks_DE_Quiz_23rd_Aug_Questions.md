# Databricks Data Engineer Practice Quiz — 23rd Aug
*(Copy each question + options into Google Forms as a "Multiple choice" question)*

---

**Q1.** A data engineer is building a streaming data pipeline to ingest JSON files from cloud storage into a Delta Lake table. The pipeline must process files incrementally, handle schema evolution automatically, ensure exactly-once processing, and minimize manual infrastructure management.

How should the data engineer fulfill these requirements?

- A. Use traditional Spark Structured Streaming with Auto Loader, manually configuring checkpoints location and enabling schema inference with `mergeSchema"="true"`.
- B. Use Lakeflow Spark Declarative Pipelines with a static DataFrame read.
- C. Use Auto Loader in batch mode with a daily job.
- D. Use Lakeflow Spark Declarative Pipelines with Auto Loader and enabling schema inference with `cloudFiles.schemaEvolutionMode = "addNewColumns"`

---

**Q2.** A data engineer is using Structured Streaming to ingest data. Due to business requirements, ingestion must be performed in an Upsert (update/insert) pattern into a Delta Lake table using the product_id key.

Which Structured Streaming functionality allows executing this Upsert logic in each micro-batch?

- A. writer.mode("merge")
- B. df.writeStream.option("updateMode", "merge")
- C. df.writeStream.trigger(once=True)
- D. df.writeStream.foreachBatch(upsert_to_delta).start()

---

**Q3.** A data engineer is building a customer data pipeline in Lakeflow Spark Declarative Pipelines. The source is a cloud-based event stream with limited retention containing inserts, updates, and deletes for customer records. These changes are being applied using the AUTO CDC INTO syntax to maintain an SCD Type 1 table as the target table, customer_dim.

How should the data engineer build a downstream job that streams from the customer_dim table to only act on updates and delete events, processing data incrementally?

- A. Read change data feed from customer_dim table and apply filters to incrementally act on the change events.
- B. When stored as SCD 1, the target of AUTO CDC INTO includes updates and deletes. Streaming from customer_dim can fail due to these operations. Instead, build another stream from the original source.
- C. Streaming from customer_dim table would only be possible in the case of SCD 2 retention.
- D. Use ignoreChanges flag while streaming from customer_dim to avoid breaking the pipeline during updates and deletes.

---

**Q4.** A data engineer is using Auto Loader to read in JSON data as it arrives. They have configured Auto Loader to quarantine invalid JSON records. They are noticing that over time, some records are being quarantined even though they are well-formed JSON.
```
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("badRecordsPath", "/tmp/path")
    .schema("a int, b int")
    .load("/Volumes/source_data"))
```

What is the cause of the missing data?

- A. The badRecordsPath location is accumulating many small files.
- B. At some point, the upstream data provider switched everything to multi-line JSON.
- C. The source data is valid JSON, but doesn't conform to their defined schema in some way.
- D. The engineer forgot to set the option cloudFiles.quarantineMode, "rescue".

---

**Q5.** A data engineer is implementing liquid clustering on a Delta Lake table and needs to understand how it affects data management operations. The table will be updated frequently with new data. The table is an external table and not managed by Unity Catalog.

How does liquid clustering in Delta Lake handle new data that is inserted after the initial table creation?

- A. New data remains unclustered until the next OPTIMIZE operation.
- B. New data is automatically clustered during write operations.
- C. New data is written to a staging area and clustered during scheduled maintenance.
- D. New data is rejected if it doesn't match the clustering pattern.

---

**Q6.** A data engineer is optimizing a managed table that suffers from data skew and frequently changing query filter columns. The table size is under 1TB. The engineer needs to avoid costly data rewrites when query patterns evolve.

How should the data engineer meet this requirement?

- A. Enable liquid clustering, as it efficiently handles data skew, allows clustering keys to be changed without rewriting existing data, and adapts to evolving query patterns.
- B. Combine partitioning and Z-ordering to maximize flexibility.
- C. Use Hive-style partitioning, as it provides efficient data skipping.
- D. Apply Z-ordering, since it allows flexible reorganization of data layout.

---

**Q7.** A data engineer is tasked with ensuring that a Delta table in Databricks continuously retains deleted files for 15 days (instead of the default 7 days), in order to permanently comply with the organization's data retention policy.

Which code snippet correctly sets this retention period for deleted files?

- A.
```
from delta.tables import *;
deltaTable = DeltaTable.forPath(spark, "/mnt/data/my_table");
deltaTable.deletedFileRetentionDuration = "interval 15 days"
```
- B.
```
spark.sql("ALTER TABLE my_table
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 15 days')")
```
- C.
```
spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", "15 days")
```
- D.
```
spark.sql("VACUUM my_table RETAIN 15 HOURS")
```

---

**Q8.** A data engineer is using the AUTO CDC API (APPLY CHANGES INTO) in Lakeflow Spark Declarative Pipelines to propagate deletions from a source table (orders_source) to a target table (orders_target). The source has Change Data Feed (CDF) enabled.

Due to distributed system delays, some delete events arrive out of order (e.g., a "Delete" event for ID 100 arrives before an "Update" event for ID 100 that happened earlier in time).

How does the AUTO CDC API internally ensure deletions are applied correctly despite these out-of-order events?

- A. It manually sorts all incoming events by timestamp in the driver before applying changes to the target.
- B. It uses the column specified in SEQUENCE BY to order events and retains tombstones for deleted rows until older sequences are processed.
- C. It ignores deletions if they arrive after updates for the same key, assuming the update is the latest truth.
- D. It runs VACUUM on the target table immediately to purge conflicting records from the log.

---

**Q9.** A data engineer is designing a system leveraging Lakeflow Declarative Pipeline technology to process real-time truck telemetry data ingested from JSON files in S3 using Auto Loader. The data includes truck_id, timestamp, location, speed, and fuel_level. The system must support two use cases:

1. Low-latency operational monitoring of the latest location, speed, and fuel_level per truck_id, continuously updated as new telemetry events arrive.
2. Daily aggregated reports of total distance traveled and average fuel efficiency per truck_id for the management team.

Which approach should the data engineer use for streaming tables and materialized views in the Lakeflow Declarative Pipeline to meet these requirements?

- A. Define a materialized view to ingest and store the raw telemetry data, and create a streaming table to compute the latest location, speed, and fuel_level per truck_id for real-time monitoring. Create another materialized view to compute the daily aggregated distance and fuel efficiency per truck_id for reporting.
- B. Define a streaming table to ingest and store the raw telemetry data, and create a streaming table to incrementally compute the latest location, speed, and fuel_level per truck_id for real-time monitoring. Create a materialized view to compute the daily aggregated distance and fuel efficiency per truck_id for reporting.
- C. Define a streaming table to ingest and store the raw telemetry data, and create a materialized view to compute the latest location, speed, and fuel_level per truck_id for real-time monitoring. Create another materialized view to compute the daily aggregated distance and fuel efficiency per truck_id for reporting.
- D. Define a streaming table to ingest and store the raw telemetry data, and create a streaming table to compute the daily aggregated distance and fuel efficiency per truck_id. Create a materialized view to compute the latest location, speed, and fuel_level per truck_id for real-time monitoring.

---

**Q10.** A Delta Lake table is updated via a daily MERGE INTO command that applies updates and deletions. The engineer regularly runs `VACUUM my_table RETAIN 7 DAYS`. However, file deletions caused by MERGE INTO keep failing.

What is the most likely cause of the error?

- A. The delta.deletedFileRetentionDuration is too long (default 7 days), which conflicts with VACUUM.
- B. The VACUUM retention value must be longer than the longest write interval, which is only 24 hours.
- C. The MERGE INTO command does not generate tombstones, so VACUUM has no files to remove.
- D. The delta.logRetentionDuration must be less than 7 days.
