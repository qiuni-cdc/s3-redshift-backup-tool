
# Migration Plan: From Legacy ETL to a Modern ELT Platform

## 1. Core Principle: Isolation and Parallel Validation

The entire migration strategy is built on one core principle: **do not touch the existing production system.**

We will build, test, and validate the new pipeline (Airflow, dbt, and the sync tool) completely in parallel with the legacy system (Kettle, cron). The old and new systems will run side-by-side for a validation period. The legacy system will only be decommissioned after we have 100% confidence in the new pipeline's correctness and reliability.

This approach ensures a zero-downtime, zero-risk migration.

---

## 2. Phase 1: Infrastructure and Environment Setup (Isolation)

The first step is to create a completely separate and isolated environment for the new services. This prevents any resource contention or interference with the current production jobs.

### 2.1. Provision New, Dedicated Infrastructure for Airflow

*   **Primary Recommendation:** Use a managed Airflow service like **Amazon MWAA (Managed Workflows for Apache Airflow)**. This is the fastest and most reliable way to get a production-ready Airflow environment, as it handles the complex setup, scaling, and maintenance for you.
*   **Alternative:** If a managed service is not an option, provision a new, dedicated EC2 instance (e.g., a `t3.medium` or larger) to host the Airflow components (webserver, scheduler, etc.).
*   **Critical Rule:** Do not install Airflow or dbt on the existing ETL server.

### 2.2. Isolate Workloads in Redshift

To prevent the new dbt jobs from impacting the performance of the legacy Kettle jobs, we must isolate their resource usage within Redshift.

*   **Action Steps:**
    1.  Create a new, dedicated user for dbt (e.g., `dbt_user`) in Redshift.
    2.  Use Redshift's **Workload Management (WLM)** to create a separate query queue for dbt jobs.
    3.  Assign the `dbt_user` to this new queue.
*   **Benefit:** This ensures that long-running dbt transformations cannot consume all the cluster's resources and slow down your existing production jobs. You can monitor and allocate resources to each workload independently.

---

## 3. Phase 2: The Parallel Run and Validation Period

This is the most critical phase of the migration. For a period of time (e.g., 1-4 weeks), both the legacy and the new pipelines will run simultaneously, allowing for a thorough validation of the new system's output.

### 3.1. Deploy the New Pipeline

*   The dbt project and Airflow DAGs should be stored in a Git repository.
*   A simple CI/CD pipeline (e.g., using GitHub Actions) should be set up to automatically deploy changes to your dbt and Airflow code to the new production environment.

### 3.2. Write to a Separate Schema

To prevent any conflicts and to allow for direct comparison, the two pipelines will write to different schemas in Redshift.

*   **Legacy Kettle Jobs** -> `analytics_legacy` schema (your current analytics schema)
*   **New dbt Pipeline** -> `analytics_new` schema

### 3.3. Reconcile and Validate the Data

After each parallel run, you must verify that the data in `analytics_new` is identical to the data in `analytics_legacy`.

*   **Automated Reconciliation:** Create an automated process (e.g., an Airflow task) that runs SQL queries to compare the two schemas. Checks should include:
    *   **Row Counts:** `SELECT COUNT(*) FROM analytics_new.fct_orders` vs. `...analytics_legacy.fct_orders`.
    *   **Metric Totals:** `SELECT SUM(revenue) FROM ...` vs. `...`
    *   The `dbt_audit_helper` dbt package can be used to automate these comparisons.
*   **Business User Validation:** This is a mandatory step. Work with your data analysts and business users to connect their BI tools to the new `analytics_new` schema. They must validate that the numbers on their reports and dashboards match their expectations and the results from the legacy system.

---

## 4. Phase 3: The Cutover Plan

Once the new pipeline has been running reliably and the data has been thoroughly validated for a sufficient period, you can execute the final cutover.

1.  **Communication:** Announce a cutover date to all data consumers. Inform them that their BI tools and reports will be switched to the new, improved data source.
2.  **Final Parallel Run:** Perform one last parallel run and reconciliation check on the day of the cutover to ensure everything is still perfectly aligned.
3.  **Redirect BI Tools:** Work with the BI team to repoint all production dashboards and reports from the old `analytics_legacy` schema to the new `analytics_new` schema.
4.  **Disable Legacy Jobs:** Turn off the cron jobs and Kettle jobs on the old ETL server. **Do not delete them.** Simply disable them for 1-2 weeks. This provides a safe and immediate rollback path if any unexpected issues arise.
5.  **Rename Schema (Optional but Recommended):** After the new pipeline has been the sole source of truth for a week, perform the final schema rename to make it the official `analytics` schema.
    *   `ALTER SCHEMA analytics_legacy RENAME TO analytics_archive;`
    *   `ALTER SCHEMA analytics_new RENAME TO analytics;`
6.  **Decommission:** Once you are fully confident in the new system, you can safely decommission the old ETL server.
