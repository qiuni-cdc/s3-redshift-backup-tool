# Parcel Detail Sync Plan

This plan outlines a process to consolidate tracking information into a central Redshift table.

## Process

1.  **Data Aggregation**: Data from two sources will be extracted and loaded into a temporary staging table.
    *   Source 1: A join between `uni_tracking_info` and `uni_tracking_spath`.
    *   Source 2: A join between `uni_tracking_info` and `uni_tracking_addon_spath`.
2.  **Hourly Synchronization**: Every hour, the aggregated data from the temporary table is synchronized to the permanent `parcel_detail_tool` table in Redshift.
3.  **Cleanup**: After a successful synchronization, all records in the temporary table are deleted to prepare for the next hourly run.

---

## Things to Consider

Here are key considerations based on the plan, expanding on the points in the document:

1.  **Customizable Source/Target Mapping**:
    *   The plan correctly identifies the need to move beyond simple table name mapping. A flexible configuration is required where you can explicitly define the source tables/queries and the destination table. This will make the pipeline more robust and reusable.

2.  **Watermarking for a Temporary Table**:
    *   The use of an `id_only` watermark strategy on a table that is regularly truncated is a significant challenge. Since the `min(id)` will change with each run, you cannot rely on a simple incremental ID.
    *   **Possible Solution**: Instead of truncating, you could "soft delete" rows by adding a `processed_at` timestamp. The sync process would only select rows where `processed_at` is null. A separate, less frequent cleanup job could then archive or permanently delete old, processed rows.

3.  **Failure Handling and Recovery**:
    *   **Sync Failure**: If the sync to Redshift fails, what happens? The data must remain in the temporary table for the next attempt. The cleanup step (deleting from the temp table) should only occur after a confirmed successful sync.
    *   **Extraction Failure**: If one of the initial data extractions fails, the entire run for that hour should be aborted to prevent partial data syncs.
    *   **Idempotency**: The process needs to be idempotent, meaning running it multiple times with the same source data should not result in duplicate entries in the final Redshift table. This is crucial for recovery after a failure.

4.  **Data Integrity and Schema Consistency**:
    *   Data is coming from two different sources (`uni_tracking_spath` and `uni_tracking_addon_spath`). Do they have the same column structure and data types? The extraction process must ensure that the data inserted into the temporary table is consistent to avoid errors during the final sync to Redshift.

5.  **Transactional Integrity**:
    *   The process of "syncing then deleting" is not atomic and can lead to data duplication if a failure occurs after the sync but before the delete. The sync and cleanup operations should be part of a single transaction, or the process should be designed to be safely re-runnable (idempotent).

6.  **Performance and Scalability**:
    *   How much data is expected in each hourly run? The extraction queries on the source tables and the sync process to Redshift need to be efficient enough to complete well within the one-hour window. Consider adding indexes to the source tables to speed up the extraction.

7.  **Monitoring and Alerting**:
    *   A robust monitoring and alerting system is essential for an automated hourly job. You need to know immediately if a sync fails, if it's taking too long, or if it completes with zero rows (which might indicate an upstream data issue).