# Answer Key — Databricks Data Engineer Practice Quiz

| # | Correct Answer | Short Rationale |
|---|---|---|
| Q1 | **A** — Use spark.readStream to consume the CDF and apply changes downstream using AUTO CDC APIs | AUTO CDC (formerly APPLY CHANGES INTO) is the SDP-native way to propagate CDF changes incrementally. |
| Q2 | **A** — cloudFiles + writeStream with checkpointLocation, default trigger, `.start()` | Default (continuous) trigger with Auto Loader gives true incremental, near-real-time ingestion. `availableNow` (B) runs once and stops, so it's not continuous/near-real-time; C/D use a non-existent "autoloader" format string. |
| Q3 | **B** — Streams all rows in, flags missing check-in/out as quarantined, partitions by is_quarantined | `expect_all` (not `expect_all_or_drop`) only flags/warns, it doesn't drop rows; all rows still flow into silver_reservations, partitioned by the computed flag. |
| Q4 | **C** — Non-overlapping 10-minute interval, offset by 3 minutes | `window(col, windowDuration, slideDuration, startTime)` → duration=10min, slide=10min (slide=duration → non-overlapping/tumbling), startTime=3min offset. |
| Q5 | **D** — Structured Streaming in batch mode using `trigger(availableNow=True)` | availableNow processes all new data incrementally (using checkpoint/offsets) then shuts the cluster down — lowest compute cost vs. full reprocessing or a long-running cluster. |
| Q6 | **B** — Only the source table is dropped; target is unaffected | `CREATE TABLE ... AS SELECT` (CTAS) copies data into an independent table; it is not a view, so it has no dependency on the source. |
| Q7 | **D** — Use MERGE INTO to upsert the most recent entry for each customer_id | Standard `MERGE INTO` fails if multiple source rows match the same target key in one batch, so you must first reduce to the most-recent record per customer_id before merging. |
| Q8 | **B** — Non-overlapping five-minute interval; state maintained for 10 minutes for late data | `window("order_timestamp", "5 minutes")` with no slide duration = tumbling (non-overlapping) 5-min windows; `withWatermark(..., "10 minutes")` keeps state for 10 minutes. |
| Q9 | **A** — CDF is useful when only a small fraction of records are updated in each batch | Since this table is fully overwritten every night, CDF would just record a full delete+insert of nearly all rows — CDF's benefit comes from capturing small, targeted changes, so the suggestion isn't well-suited here. |
| Q10 | **A** — Newly updated records will be appended to the target table | Default `writeStream` output mode is append; each `availableNow` run only picks up new versions since the last checkpoint and appends the update_postimage rows — no overwrite, no full history replay. |
