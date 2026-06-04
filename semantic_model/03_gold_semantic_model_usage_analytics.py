# Fabric Notebook — Gold Analytics: SemanticModelLogs Usage Intelligence
# Portfolio Reference: Fabric Workspace Observability Project
# Layer: Gold (analytics built on top of gold_semantic_model_logs_parsed)
#
# Description:
#   Produces usage intelligence Gold tables from gold_semantic_model_logs_parsed:
#     - gold_report_visual_usage      — which reports/visuals/pages are used most
#     - gold_dax_query_performance    — slow query detection per model/report/user
#     - gold_user_report_activity     — per-user report engagement summary
#     - gold_model_usage_summary      — daily rollup per semantic model
#
#   These tables power the Power BI usage analytics dashboard and feed
#   back into the semantic model as a usage layer on top of the Gold data tables.
#
# Note: gold_semantic_model_logs_parsed is the ONLY source here.
#       No Bronze or Silver layers exist for SemanticModelLogs.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, avg, max as spark_max,
    min as spark_min, sum as spark_sum,
    round as spark_round, percentile_approx,
    to_date, current_timestamp, when, lit,
    isnull
)
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE        = "gold_semantic_model_logs_parsed"
OUT_VISUAL_USAGE    = "gold_report_visual_usage"
OUT_DAX_PERF        = "gold_dax_query_performance"
OUT_USER_ACTIVITY   = "gold_user_report_activity"
OUT_MODEL_SUMMARY   = "gold_model_usage_summary"

print("[Gold-Analytics] Building usage intelligence from SemanticModelLogs")

df_parsed = spark.read.format("delta").table(SOURCE_TABLE)
df_parsed.createOrReplaceTempView("sem_logs")

print(f"[Gold-Analytics] Source rows: {df_parsed.count():,}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Gold Table: Report + Visual Usage
#           Answers: Which reports and visuals are used most?
#                    Which pages do users land on?
#                    Which visuals are slow to render?
# ──────────────────────────────────────────────────────────────
df_visual_usage = spark.sql("""
    SELECT
        to_date(Timestamp)                              AS log_date,
        WorkspaceName,
        ItemName                                        AS model_name,
        report_id,
        report_name,
        page_id,
        page_name,
        visual_id,
        visual_name,
        client_type,

        COUNT(*)                                        AS total_renders,
        COUNT(DISTINCT upn)                             AS distinct_users,
        ROUND(AVG(DurationMs), 2)                       AS avg_render_ms,
        MAX(DurationMs)                                 AS max_render_ms,
        PERCENTILE_APPROX(DurationMs, 0.95)             AS p95_render_ms,

        SUM(CASE WHEN Status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
        SUM(CASE WHEN Status = 'FAILURE' THEN 1 ELSE 0 END) AS failure_count,
        ROUND(
            100.0 * SUM(CASE WHEN Status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*),
        2)                                              AS success_rate_pct,

        -- Performance tier for the visual
        CASE
            WHEN AVG(DurationMs) < 500    THEN 'Fast (<0.5s)'
            WHEN AVG(DurationMs) < 2000   THEN 'Moderate (0.5-2s)'
            WHEN AVG(DurationMs) < 10000  THEN 'Slow (2-10s)'
            ELSE 'Critical (>10s)'
        END                                             AS render_performance_tier,

        current_timestamp()                             AS _gold_loaded_at

    FROM sem_logs
    WHERE report_id IS NOT NULL    -- Only rows triggered by a visual render
    GROUP BY
        to_date(Timestamp),
        WorkspaceName, ItemName,
        report_id, report_name,
        page_id, page_name,
        visual_id, visual_name,
        client_type
    ORDER BY log_date DESC, total_renders DESC
""")

df_visual_usage.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_VISUAL_USAGE)
print(f"[Gold-Analytics] {OUT_VISUAL_USAGE} → {df_visual_usage.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Gold Table: DAX Query Performance
#           Answers: Which DAX queries are slowest?
#                    Which report/visual triggers the heaviest queries?
#                    Which users generate the most CPU-intensive operations?
# ──────────────────────────────────────────────────────────────
df_dax_perf = spark.sql("""
    SELECT
        to_date(Timestamp)                              AS log_date,
        WorkspaceName,
        ItemName                                        AS model_name,
        upn                                             AS user_upn,
        report_name,
        visual_name,
        page_name,
        client_type,

        COUNT(*)                                        AS dax_execution_count,
        ROUND(AVG(DurationMs), 2)                       AS avg_duration_ms,
        MAX(DurationMs)                                 AS max_duration_ms,
        PERCENTILE_APPROX(DurationMs, 0.95)             AS p95_duration_ms,
        ROUND(AVG(CpuTimeMs), 2)                        AS avg_cpu_ms,
        SUM(CpuTimeMs)                                  AS total_cpu_ms,

        SUM(CASE WHEN Status = 'FAILURE' THEN 1 ELSE 0 END) AS failed_executions,

        -- Flag CPU-bound vs IO-bound
        CASE
            WHEN AVG(CpuTimeMs) > AVG(DurationMs) * 0.8 THEN 'CPU_Bound'
            WHEN AVG(CpuTimeMs) < AVG(DurationMs) * 0.2 THEN 'IO_Bound'
            ELSE 'Mixed'
        END                                             AS execution_profile,

        CASE
            WHEN AVG(DurationMs) < 500    THEN 'Fast (<0.5s)'
            WHEN AVG(DurationMs) < 2000   THEN 'Moderate (0.5-2s)'
            WHEN AVG(DurationMs) < 10000  THEN 'Slow (2-10s)'
            ELSE 'Critical (>10s)'
        END                                             AS performance_tier,

        current_timestamp()                             AS _gold_loaded_at

    FROM sem_logs
    WHERE OperationName LIKE '%DAX%'
      AND DurationMs IS NOT NULL
    GROUP BY
        to_date(Timestamp),
        WorkspaceName, ItemName,
        upn, report_name, visual_name, page_name, client_type
    ORDER BY avg_duration_ms DESC
""")

df_dax_perf.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_DAX_PERF)
print(f"[Gold-Analytics] {OUT_DAX_PERF} → {df_dax_perf.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Gold Table: User Report Activity
#           Answers: Who uses which reports and how often?
#                    Which users hit the most failures?
#                    Active vs passive users by engagement level.
# ──────────────────────────────────────────────────────────────
df_user_activity = spark.sql("""
    SELECT
        to_date(Timestamp)                              AS log_date,
        WorkspaceName,
        ItemName                                        AS model_name,
        upn                                             AS user_upn,
        client_type,

        COUNT(*)                                        AS total_operations,
        COUNT(DISTINCT report_id)                       AS distinct_reports_accessed,
        COUNT(DISTINCT visual_id)                       AS distinct_visuals_triggered,
        COUNT(DISTINCT page_id)                         AS distinct_pages_visited,

        SUM(CASE WHEN Status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_ops,
        SUM(CASE WHEN Status = 'FAILURE' THEN 1 ELSE 0 END) AS failed_ops,
        ROUND(
            100.0 * SUM(CASE WHEN Status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*),
        2)                                              AS success_rate_pct,

        ROUND(AVG(DurationMs), 2)                       AS avg_op_duration_ms,
        SUM(CpuTimeMs)                                  AS total_cpu_ms,

        -- Engagement tier
        CASE
            WHEN COUNT(*) >= 500  THEN 'Power User'
            WHEN COUNT(*) >= 100  THEN 'Active User'
            WHEN COUNT(*) >= 20   THEN 'Regular User'
            ELSE 'Casual User'
        END                                             AS engagement_tier,

        current_timestamp()                             AS _gold_loaded_at

    FROM sem_logs
    WHERE upn IS NOT NULL
    GROUP BY
        to_date(Timestamp),
        WorkspaceName, ItemName,
        upn, client_type
    ORDER BY log_date DESC, total_operations DESC
""")

df_user_activity.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_USER_ACTIVITY)
print(f"[Gold-Analytics] {OUT_USER_ACTIVITY} → {df_user_activity.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Gold Table: Model Usage Daily Summary
#           Top-level daily health rollup per semantic model
# ──────────────────────────────────────────────────────────────
df_model_summary = spark.sql("""
    SELECT
        to_date(Timestamp)                              AS log_date,
        WorkspaceName,
        ItemName                                        AS model_name,
        ModelMode,

        COUNT(*)                                        AS total_operations,
        COUNT(DISTINCT upn)                             AS distinct_users,
        COUNT(DISTINCT report_id)                       AS distinct_reports,
        COUNT(DISTINCT visual_id)                       AS distinct_visuals,

        SUM(CASE WHEN OperationName LIKE '%DAX%'     THEN 1 ELSE 0 END) AS dax_ops,
        SUM(CASE WHEN OperationName LIKE '%Refresh%' THEN 1 ELSE 0 END) AS refresh_ops,

        ROUND(AVG(DurationMs), 2)                       AS avg_duration_ms,
        MAX(DurationMs)                                 AS max_duration_ms,
        ROUND(AVG(CpuTimeMs), 2)                        AS avg_cpu_ms,

        SUM(CASE WHEN Status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
        SUM(CASE WHEN Status = 'FAILURE' THEN 1 ELSE 0 END) AS failure_count,
        ROUND(
            100.0 * SUM(CASE WHEN Status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*),
        2)                                              AS overall_success_rate_pct,

        current_timestamp()                             AS _gold_loaded_at

    FROM sem_logs
    GROUP BY
        to_date(Timestamp),
        WorkspaceName, ItemName, ModelMode
    ORDER BY log_date DESC, total_operations DESC
""")

df_model_summary.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(OUT_MODEL_SUMMARY)
print(f"[Gold-Analytics] {OUT_MODEL_SUMMARY} → {df_model_summary.count():,} rows")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Usage Intelligence Summary
# ──────────────────────────────────────────────────────────────
print("\n── Top 10 Most Used Reports ──")
display(
    spark.read.format("delta").table(OUT_VISUAL_USAGE)
    .groupBy("report_name", "model_name")
    .agg(spark_sum("total_renders").alias("total_renders"),
         countDistinct("user_upn" if "user_upn" in spark.read.format("delta")
                       .table(OUT_VISUAL_USAGE).columns else "distinct_users").alias("users"))
    .orderBy("total_renders", ascending=False)
    .limit(10)
)

print("\n── Slowest Visuals (p95 render > 5s) ──")
display(
    spark.read.format("delta").table(OUT_VISUAL_USAGE)
    .filter(col("p95_render_ms") > 5000)
    .select("report_name", "page_name", "visual_name",
            "avg_render_ms", "p95_render_ms", "total_renders",
            "render_performance_tier")
    .orderBy("p95_render_ms", ascending=False)
    .limit(15)
)

print("\n── User Engagement Distribution ──")
display(
    spark.read.format("delta").table(OUT_USER_ACTIVITY)
    .groupBy("engagement_tier")
    .agg(count("user_upn").alias("user_count"),
         spark_sum("total_operations").alias("total_ops"))
    .orderBy("total_ops", ascending=False)
)

print("\n═══════════════════════════════════════════════════════════")
print("           SEMANTIC MODEL USAGE GOLD BUILD COMPLETE        ")
print("═══════════════════════════════════════════════════════════")
for tbl in [OUT_VISUAL_USAGE, OUT_DAX_PERF, OUT_USER_ACTIVITY, OUT_MODEL_SUMMARY]:
    rc = spark.read.format("delta").table(tbl).count()
    print(f"  ✔ {tbl:<45s} → {rc:>8,} rows")
print("═══════════════════════════════════════════════════════════")
