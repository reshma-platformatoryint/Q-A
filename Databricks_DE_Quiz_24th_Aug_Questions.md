# Databricks Data Engineer Practice Quiz — 24th Aug
*(Copy each question + options into Google Forms. Q2 is a "Checkboxes" question — Choose 2; all others are "Multiple choice")*

---

**Q1.** A bronze_data table in a Lakeflow Spark Declarative Pipeline pipeline uses `dp.read_stream("source_data")` without any deduplication logic. The source data is a Kafka topic that occasionally delivers duplicate messages due to retries.

How should the data engineer modify the logic in the bronze_data table to ensure duplicate records are removed before they reach the next layer?

- A. Add a GROUP BY transformation with `collect_list()` to group records by key and select the most recent one.
- B. Use the `df.dropDuplicates("unique_key")` transformation in the pipeline, ensuring the table has an appropriate watermark enabled.
- C. Implement MERGE INTO in a `foreachBatch` to handle deduplication based on the unique key.
- D. Add the option `spark.databricks.delta.delete.ignoreChanges=true` to the pipeline configuration.

---

**Q2.** *(Choose 2)* A Delta Lake table has been partitioned by the order_date column. A data engineer needs to ensure that queries on this table, especially those filtering by customer_id, are as fast as possible.

Which two maintenance commands should the engineer run? *(Select all that apply)*

- A.
```
OPTIMIZE my_table
ZORDER BY (customer_id);
```
- B.
```
VACUUM my_table
RETAIN 0 HOURS;
```
- C.
```
OPTIMIZE my_table;
```
- D.
```
OPTIMIZE my_table
PARTITION BY (order_date);
```
- E.
```
ALTER TABLE my_table
ADD PARTITION (order_date);
```

---

**Q3.** A data engineer uses the APPLY CHANGES INTO pattern in Lakeflow Spark Declarative Pipeline to maintain an SCD Type 1 table (customer_dim) tracking the latest customer states. Source data may contain duplicate records due to source system retries.

How should the engineer configure the pipeline to ensure source duplicates do not break the CDC logic?

- A. Define the SCD Type 1 table as a `@dp.view`
- B. Add a SEQUENCE BY column to order events and handle update conflicts.
- C. Include the `ignoreDuplicates=true` option in the `dlt.read_stream()` function.
- D. Use a GROUP BY before the APPLY CHANGES INTO function.

---

**Q4.** A data engineer is setting up a pipeline to ingest data from a message bus system that occasionally delivers duplicate messages. The duplicate messages can be a week apart. The target is a Databricks Delta Lake table where each record should appear exactly once.

Which Databricks ingestion pattern should be implemented to handle potential duplicates where events can arrive outside of the configured watermark?

- A. Use Delta Lake time travel to identify and remove duplicates.
- B. Configure Structured Streaming with dropDuplicates transformation.
- C. Implement a write operation using MERGE INTO with a unique key.
- D. Use Delta Lake's change data feed to filter duplicate records.

---

**Q5.** A senior data engineer is designing large-scale data workflows and evaluating scalable data models for managing large datasets.
The team has identified core capabilities needed for a modern data platform and is reviewing Delta Lake as a potential solution.
The engineer must determine which proposed capabilities are **not valid considerations** when evaluating Delta Lake.

Which Delta Lake feature can be ignored during this evaluation?

- A. Delta Lake works with various data formats (e.g., Parquet, JSON, CSV) and integrates well with Spark and Databricks tools.
- B. Delta Lake's capability to process data in both batch and streaming modes seamlessly, providing flexibility in data ingestion and processing.
- C. Delta Lake optimizes metadata handling, efficiently managing billions of files and facilitating scalability to petabyte-scale datasets.
- D. Delta Lake provides limited support for monitoring and troubleshooting data pipelines, so relevant partner tools have to be identified and set up for enhanced operational efficiency.

---

**Q6.** A data engineer is implementing a structured streaming pipeline to ingest data from a large Delta Lake table. The pipeline needs to process all existing historical data in the table before starting to process new real-time data.

Which option should the engineer add to the PySpark Structured Streaming code?

- A. .option("startingVersion", 0)
- B. .option("readMode", "historical")
- C. .option("startFromTimestamp", "1970-01-01")
- D. .option("readChangeFeed", "true")

---

**Q7.** A data engineer needs to design a solution to ingest Parquet files from an AWS S3 bucket into a Delta Lake table in near real-time. It is critical that the solution guarantees exactly-once processing despite the possibility of files being rewritten or resent. The engineer wants to minimize boilerplate code.

Which approach should the data engineer use?

- A. Use Auto Loader with event notification mode and enable `cloudFiles.useIncrementalListing=true`.
- B. Use Auto Loader with event notification mode and no additional options.
- C. Use Auto Loader with file listing mode and `cloudFiles.allowDuplicates=false`.
- D. Use Structured Streaming with the `ignoreDuplicates=true` option for the S3 source.

---

**Q8.** A company processes semi-structured JSON files from an external source using Auto Loader in a classic Databricks job. Occasionally, records arrive with null critical fields, invalid types, or unexpected nested schema variations. The engineer must ensure that malformed or non-conforming records are not dropped silently and are captured in a separate quarantine table. The pipeline should continue processing good records into the Bronze layer without failing the job, and the approach must support both batch and streaming ingestion.

Which approach fulfills the quarantine mechanism in this ingestion architecture?

- A. Use Auto Loader with Delta Live Tables (DLT) and implement an EXPECT ... ON VIOLATION DROP ROW constraint combined with a record audit logic to route bad records.
- B. Create a notebook job with inferSchema=True, write a streaming query with .foreachBatch() and catch exceptions using try/except to redirect failed batches to quarantine.
- C. Use Lakeflow Spark Declarative Pipelines with a SQL pipeline; configure it to drop rows with nulls using where critical_field is not null, and rely on audit logs for malformed data.
- D. Use Auto Loader with failFast mode set to false, and enable schema evolution; invalid records will be silently ignored during ingestion.

---

**Q9.** A data engineer uses Structured Streaming to read transaction data from a bronze Delta table. Some rows have data quality issues (transaction value is negative) and must be written to a separate quarantine table. Good data needs low-latency processing for downstream systems, while bad data is only reviewed occasionally and has no production dependencies. The quarantine solution must not impact production streaming and should be low cost.

How should the quarantine process be implemented to meet these requirements?

- A. The streaming job for the good data needs to be modified to filter out records with a transaction value less than 0 before writing. The streaming job for the quarantine data needs to filter out records with a transaction value greater than or equal to 0 before writing. Both should run as separate streams on the same cluster to minimize cost.
- B. The streaming job for the good data needs to be modified to filter out records with a transaction value less than 0 before writing. The streaming job for the quarantine data needs to filter out records with a transaction value greater than or equal to 0 before writing, and should be implemented on a separate small cluster and only run once a day to minimize cost.
- C. The existing streaming job for the good data should be updated to incorporate the quarantining of the bad data. A new boolean column called "quarantine" should be added to the DataFrame, and records should be written to the target table partitioned by this column.
- D. The existing streaming job for the good data should be updated to incorporate the quarantining of the bad data inside a `foreachBatch` function. The function will write good data to the target table and bad data to the quarantine table.

---

**Q10.** A data engineer has created a View named raw_view in a Lakeflow Spark Declarative Pipeline on top of a raw_data table. The engineer needs to update the underlying code of raw_view to add a calculated column.

What is the impact of this modification?

- A. `@dp.view` cannot be modified once created; it must be dropped and recreated.
- B. `@dp.view` will be incrementally updated on the next pipeline update, ensuring low latency.
- C. The raw_data table will have to be recreated from scratch since the view depends on it.
- D. `@dp.view` will be fully recomputed on the next pipeline update, without affecting the raw_data storage.
