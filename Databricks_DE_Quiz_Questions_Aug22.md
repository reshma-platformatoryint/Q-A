# Databricks Data Engineer Practice Quiz
*(Copy each question + options into Google Forms as a "Multiple choice" question)*

---

**Q1.** A data engineering team at a supply chain company uses Lakeflow Spark Declarative Pipelines (SDP) to manage inventory data. The team maintains a streaming table, `inventory_updates`, with Change Data Feed (CDF) enabled. The table captures real-time changes to product inventory levels, with columns: product_id, quantity, and update_timestamp.

The team needs to incrementally propagate all inventory changes from the `inventory_updates` table to downstream layers.

Which implementation approach correctly satisfies this requirement?

- A. Use spark.readStream to consume the inventory_updates table's CDF, and apply the changes into downstream tables using AUTO CDC APIs.
- B. Use spark.readStream to consume the inventory_updates table directly, and propagate the new updates incrementally to downstream tables.
- C. Use spark.readStream to consume the inventory_updates table with skipChangeCommits, and propagate the newly added data incrementally to downstream tables.
- D. Use spark.read to consume the inventory_updates table's CDF, and merge the changes into downstream tables using MERGE INTO.

---

**Q2.** A data engineer wants to ingest input json data into a target Delta table. They want the data ingestion to happen incrementally in near-real-time.

Which option correctly meets the specified requirement?

- A.
```
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(source_path)
.writeStream
    .option("checkpointLocation", checkpointPath)
    .start("target_table")
```
- B.
```
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(source_path)
.writeStream
    .trigger(availableNow=True)
    .start("target_table")
```
- C.
```
spark.readStream
    .format("autoloader")
    .option("autoloader.format", "json")
    .load(source_path)
.writeStream
    .option("checkpointLocation", checkpointPath)
    .trigger(real-time=True)
    .start("target_table")
```
- D.
```
spark.readStream
    .format("autoloader")
    .option("autoloader.format", "json")
    .load(source_path)
.writeStream
    .option("checkpointLocation", checkpointPath)
    .start("target_table")
```

---

**Q3.** A data engineering team is building an SDP pipeline to clean and validate hotel reservations data. Some completed reservations have null check-in or check-out dates, which violates business rules.

To handle this, they implemented the following code:
```
rules = {
  "valid_check_in": "(check_in IS NOT NULL)",
  "valid_check_out": "(check_out IS NOT NULL)"
}
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

@dlt.table(partition_cols=["is_quarantined"])
@dlt.expect_all(rules)
def silver_reservations():
  return (
    spark.readStream.table("bronze_reservations")
      .withColumn("is_quarantined", expr(quarantine_rules))
  )
```

Which of the following correctly describes what this function does?

- A. This function partitions the bronze_reservations table by the is_quarantined flag, streams valid partitions into the silver_reservations table, and drops the invalid partitions.
- B. This function streams all rows into the silver_reservations table, flags those with missing check-in or check-out values as quarantined, and partitions the table by the is_quarantined flag.
- C. This function streams rows based on the quarantine_rules into two separate tables: silver_reservations for valid reservations, and is_quarantined for invalid reservations.
- D. This function streams only rows with valid check-in and check-out values into the silver_reservations table, while writing invalid rows into a separate partition.

---

**Q4.** A data engineering team at a large cloud services company is responsible for ensuring high availability and performance of hundreds of servers. To proactively detect unusual spikes or drops in CPU utilization that could indicate potential issues, the team decides to analyze server CPU usage. They want to capture both the minimum and maximum CPU usage during predefined intervals for each server, allowing them to quickly identify patterns of high or low utilization and trigger alerts if necessary.

The team writes the following query:
```
SELECT
    server_id,
    window.start AS window_start,
    window.end AS window_end,
    MIN(cpu_usage) AS min_cpu,
    MAX(cpu_usage) AS max_cpu
FROM server_metrics
GROUP BY server_id,
    window(event_time, '10 minutes', '10 minutes', '3 minutes')
ORDER BY server_id,
    window_start;
```

What is the primary purpose of this query in the context of the data engineering team's objectives?

- A. To calculate the metrics per server for every non-overlapping 3-minute interval, offset by 10 minutes
- B. To calculate the metrics per server for every 10-minute interval, sliding every 3 minutes, with a 10-minute offset.
- C. To calculate the metrics per server for every non-overlapping 10-minute interval, offset by 3 minutes
- D. To calculate the metrics per server for every 3-minute interval, sliding every 10 minutes, with a 10-minute offset.

---

**Q5.** The data engineering team has a singleplex bronze table called `orders_raw` where new orders data is appended every night. They created a new Silver table called `orders_cleaned` in order to provide a more refined view of the orders data.

The team wants to create a batch processing pipeline to process all new records inserted in the orders_raw table and propagate them to the orders_cleaned table.

Which solution minimizes the compute costs to propagate this batch of data?

- A. Use time travel capabilities in Delta Lake to compare the latest version of orders_raw with one version prior, then write the difference to the orders_cleaned table.
- B. Use batch overwrite logic to reprocess all records in orders_raw and overwrite the orders_cleaned table
- C. Use Spark Structured Streaming's foreachBatch logic to process the new records from orders_raw using trigger(processingTime="24 hours")
- D. Use Spark Structured Streaming to process the new records from orders_raw in batch mode using the trigger availableNow option

---

**Q6.** The data engineering team has a Delta Lake table created with following query:
```
CREATE TABLE target
AS SELECT * FROM source
```

A data engineer wants to drop the source table with the following query:
```
DROP TABLE source
```

Which statement describes the result of running this drop command?

- A. Only the source table will be dropped, but the target table will be no more queryable
- B. Only the source table will be dropped, while the target table will not be affected
- C. Both the target and source tables will be dropped
- D. An error will occur indicating that other tables are based on this source table

---

**Q7.** The data engineering team wants to build a pipeline that receives customers data as change data capture (CDC) feed from a source system. The CDC events logged at the source contain the data of the records along with metadata information. This metadata indicates whether the specified record was inserted, updated, or deleted. In addition to a timestamp column identified by the field update_time indicating the order in which the changes happened. Each record has a primary key identified by the field customer_id.

In the same batch, multiple changes for the same customer could be received with different update_time. The team wants to store only the most recent information for each customer in the target Delta Lake table.

Which of the following solutions meets these requirements?

- A. Enable Delta Lake's Change Data Feed (CDF) on the target table to automatically merge the received CDC feed
- B. Use MERGE INTO with SEQUENCE BY clause on the update_time for ordering how operations should be applied
- C. Use dropDuplicates function to remove duplicates by customer_id, then merge the duplicate records into the table.
- D. Use MERGE INTO to upsert the most recent entry for each customer_id into the table

---

**Q8.** Given the following streaming query:
```
spark.readStream
    .table("orders_cleaned")
    .withWatermark("order_timestamp", "10 minutes")
    .groupBy(
        window("order_timestamp", "5 minutes").alias("time"),
        "author")
    .agg(
        count("order_id").alias("orders_count"),
        avg("quantity").alias("avg_quantity"))
.writeStream
    .option("checkpointLocation", "dbfs:/path/checkpoint")
    .table("orders_stats")
```

Which of the following statements best describe this query?

- A. It calculates business-level aggregates for each non-overlapping ten-minute interval. Incremental state information is maintained for 5 minutes for late-arriving data.
- B. It calculates business-level aggregates for each non-overlapping five-minute interval. Incremental state information is maintained for 10 minutes for late-arriving data.
- C. It calculates business-level aggregates for each overlapping ten-minute interval. Incremental state information is maintained for 5 minutes for late-arriving data.
- D. It calculates business-level aggregates for each overlapping five-minute interval. Incremental state information is maintained for 10 minutes for late-arriving data.

---

**Q9.** The data engineering team maintains a Type 1 table that is overwritten each night with new data received from the source system.

A junior data engineer has suggested enabling the Change Data Feed (CDF) feature on the table in order to identify those rows that were updated, inserted, or deleted.

Which response to the junior data engineer's suggestion is correct?

- A. CDF is useful when only a small fraction of records are updated in each batch
- B. CDF is useful when the table is a Slowly Changing Dimension (SCD) of Type 2
- C. Table's data changes captured by CDF can only be read in streaming mode
- D. CDF can not be enabled on existing tables. It can only be enabled on newly created tables.

---

**Q10.** Given the following query on the Delta table 'customers' on which Change Data Feed is enabled:
```
spark.readStream
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("customers")
    .filter(col("_change_type").isin(["update_postimage"]))
.writeStream
    .option("checkpointLocation", "dbfs:/checkpoints")
    .trigger(availableNow=True)
    .table("customers_updates")
```

Which statement describes the results of this query each time it is executed?

- A. Newly updated records will be appended to the target table.
- B. Newly updated records will overwrite the target table.
- C. The entire history of updated records will overwrite the target table at each execution.
- D. The entire history of updated records will be appended to the target table at each execution, which leads to duplicate entries.
