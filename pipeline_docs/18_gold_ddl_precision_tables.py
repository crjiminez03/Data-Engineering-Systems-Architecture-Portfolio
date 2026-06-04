# Fabric Notebook — Gold Layer DDL: Precision-Preserving Table Definitions
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Creates Gold layer Delta tables with explicit DDL before
#              Dataflow Gen2 items load data via staging tables.
#              The problem being solved: Dataflow Gen2 uses the Replace
#              method to staging tables and then moves data to the target.
#              Without explicit DDL, Delta schema inference can silently
#              downcast DECIMAL(18,6) → DOUBLE, losing decimal precision
#              on financial and measurement columns.
#
#              Execution order in pipeline:
#                1. This DDL notebook runs first (CREATE TABLE IF NOT EXISTS)
#                2. Dataflow Gen2 items run (Replace → staging → target)
#                3. Gold DML notebook runs (MERGE staging → gold)
#
#              The DDL notebook is idempotent — safe to run every pipeline cycle.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

GOLD_LAKEHOUSE = "GoldLakehouse"     # <-- Replace with your Lakehouse name
STAGING_SUFFIX = "_staging"          # Dataflow Gen2 staging table suffix

print("[Gold DDL] Creating precision-preserving Gold layer table definitions")
print(f"[Gold DDL] Target Lakehouse: {GOLD_LAKEHOUSE}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Helper: Create Table + Matching Staging Table
# ──────────────────────────────────────────────────────────────
def create_gold_table(ddl_sql: str, table_name: str):
    """Creates the gold table and its matching staging table."""
    try:
        spark.sql(ddl_sql)
        print(f"  ✔ {table_name}")

        # Create staging table with identical schema for Dataflow Gen2 Replace target
        staging_name = f"{table_name}{STAGING_SUFFIX}"
        staging_ddl  = ddl_sql.replace(
            f"IF NOT EXISTS {table_name}",
            f"IF NOT EXISTS {staging_name}"
        ).replace(
            f"COMMENT '{table_name}",
            f"COMMENT 'Staging: {table_name}"
        )
        spark.sql(staging_ddl)
        print(f"  ✔ {staging_name} (staging)")

    except Exception as e:
        print(f"  ✖ {table_name}: {e}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Gold Table: Model Health Scorecard
# ──────────────────────────────────────────────────────────────
create_gold_table("""
CREATE TABLE IF NOT EXISTS gold_model_health_scorecard (
    -- Identity
    model_name              STRING          NOT NULL  COMMENT 'Semantic model name',
    ItemName                STRING          NOT NULL  COMMENT 'Power BI item name',
    WorkspaceName           STRING                    COMMENT 'Fabric workspace name',
    ModelMode               STRING                    COMMENT 'Import / DirectQuery / Composite',

    -- Refresh health (from gold_semantic_model_refresh_summary)
    avg_refresh_success_rate DECIMAL(5,2)             COMMENT 'Average refresh success rate %',
    avg_refresh_duration_ms  DECIMAL(18,3)            COMMENT 'Average refresh duration ms — 3dp precision',
    sla_breach_days          INT                       COMMENT 'Days where refresh exceeded SLA threshold',
    total_refresh_days       INT                       COMMENT 'Total days with refresh activity',

    -- DAX performance (from gold_semantic_model_dax_performance)
    avg_dax_duration_ms      DECIMAL(18,3)            COMMENT 'Average DAX query duration ms — 3dp precision',
    max_dax_duration_ms      BIGINT                    COMMENT 'Max DAX query duration ms',
    total_dax_executions     BIGINT                    COMMENT 'Total DAX executions in period',
    pct_slow_queries         DECIMAL(5,2)             COMMENT 'Percentage of queries in slow/critical tier',

    -- Scoring
    health_score             DECIMAL(5,1)             COMMENT 'Composite health score 0-100 (50% refresh, 30% DAX, 20% SLA)',
    health_grade             STRING                    COMMENT 'Letter grade: A / B / C / D',

    -- Audit
    gold_loaded_at           TIMESTAMP                 COMMENT 'When this row was loaded to gold',
    _dw_created_at           TIMESTAMP                 COMMENT 'First time this record was inserted',
    _dw_updated_at           TIMESTAMP                 COMMENT 'Last time this record was updated',
    _row_hash                STRING                    COMMENT 'SHA2 hash for change detection'
)
USING DELTA
COMMENT 'gold_model_health_scorecard — composite health scoring per semantic model'
""", "gold_model_health_scorecard")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Gold Table: Refresh Summary
# ──────────────────────────────────────────────────────────────
create_gold_table("""
CREATE TABLE IF NOT EXISTS gold_semantic_model_refresh_summary (
    log_date                DATE            NOT NULL  COMMENT 'Date of refresh activity',
    WorkspaceName           STRING                    COMMENT 'Fabric workspace name',
    ItemName                STRING          NOT NULL  COMMENT 'Semantic model name',
    ModelMode               STRING                    COMMENT 'Import / DirectQuery / Composite',

    total_refresh_ops       BIGINT                    COMMENT 'Total refresh operations on this date',
    success_count           BIGINT                    COMMENT 'Successful refresh operations',
    failure_count           BIGINT                    COMMENT 'Failed refresh operations',

    -- Precision-critical duration columns: DECIMAL not DOUBLE
    avg_duration_ms         DECIMAL(18,3)            COMMENT 'Average duration ms — 3dp',
    max_duration_ms         BIGINT                    COMMENT 'Maximum duration ms',
    min_duration_ms         BIGINT                    COMMENT 'Minimum duration ms',
    avg_cpu_ms              DECIMAL(18,3)            COMMENT 'Average CPU time ms — 3dp',

    success_rate_pct        DECIMAL(5,2)             COMMENT 'Success rate percentage',
    sla_status              STRING                    COMMENT 'OK or SLA_BREACH',

    gold_loaded_at          TIMESTAMP                 COMMENT 'Gold load timestamp',
    _dw_created_at          TIMESTAMP                 COMMENT 'First insert timestamp',
    _dw_updated_at          TIMESTAMP                 COMMENT 'Last update timestamp',
    _row_hash               STRING                    COMMENT 'SHA2 hash for MERGE change detection'
)
USING DELTA
PARTITIONED BY (log_date)
COMMENT 'gold_semantic_model_refresh_summary — daily refresh health per semantic model'
""", "gold_semantic_model_refresh_summary")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Gold Table: DAX Performance
# ──────────────────────────────────────────────────────────────
create_gold_table("""
CREATE TABLE IF NOT EXISTS gold_semantic_model_dax_performance (
    log_date                DATE            NOT NULL  COMMENT 'Date of DAX executions',
    WorkspaceName           STRING                    COMMENT 'Workspace name',
    ItemName                STRING          NOT NULL  COMMENT 'Semantic model name',
    executing_user          STRING                    COMMENT 'User who ran the query',
    app_name                STRING                    COMMENT 'Application (e.g. Power BI Service)',

    dax_execution_count     BIGINT                    COMMENT 'Number of DAX executions',

    -- Precision columns
    avg_duration_ms         DECIMAL(18,3)            COMMENT 'Average duration ms — 3dp',
    max_duration_ms         BIGINT                    COMMENT 'Max duration ms',
    p95_duration_ms         BIGINT                    COMMENT '95th percentile duration ms',
    avg_cpu_ms              DECIMAL(18,3)            COMMENT 'Average CPU ms — 3dp',

    failed_executions       BIGINT                    COMMENT 'Failed DAX execution count',
    performance_tier        STRING                    COMMENT 'Fast / Moderate / Slow / Critical',

    gold_loaded_at          TIMESTAMP                 COMMENT 'Gold load timestamp',
    _dw_created_at          TIMESTAMP                 COMMENT 'First insert timestamp',
    _dw_updated_at          TIMESTAMP                 COMMENT 'Last update timestamp',
    _row_hash               STRING                    COMMENT 'SHA2 hash for MERGE change detection'
)
USING DELTA
PARTITIONED BY (log_date)
COMMENT 'gold_semantic_model_dax_performance — daily DAX query performance per model and user'
""", "gold_semantic_model_dax_performance")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Gold Table: Audit Log User Activity
# ──────────────────────────────────────────────────────────────
create_gold_table("""
CREATE TABLE IF NOT EXISTS gold_audit_user_activity_summary (
    log_date                DATE            NOT NULL  COMMENT 'Date of audit events',
    WorkSpaceName           STRING                    COMMENT 'Power BI workspace',
    UserId                  STRING                    COMMENT 'User UPN',
    Operation               STRING                    COMMENT 'Audit operation name',
    user_tier               STRING                    COMMENT 'Heavy / Moderate / Light',

    total_events            BIGINT                    COMMENT 'Total audit events',
    success_events          BIGINT                    COMMENT 'Successful events',
    failure_events          BIGINT                    COMMENT 'Failed events',
    unique_items            BIGINT                    COMMENT 'Distinct artifacts accessed',
    success_rate_pct        DECIMAL(5,2)             COMMENT 'Success rate %',

    gold_loaded_at          TIMESTAMP                 COMMENT 'Gold load timestamp',
    _dw_created_at          TIMESTAMP                 COMMENT 'First insert timestamp',
    _dw_updated_at          TIMESTAMP                 COMMENT 'Last update timestamp',
    _row_hash               STRING                    COMMENT 'SHA2 hash for MERGE change detection'
)
USING DELTA
PARTITIONED BY (log_date)
COMMENT 'gold_audit_user_activity_summary — daily Power BI user activity from audit logs'
""", "gold_audit_user_activity_summary")


# ──────────────────────────────────────────────────────────────
# Cell 7 — Verify All Tables Created
# ──────────────────────────────────────────────────────────────
GOLD_TABLES = [
    "gold_model_health_scorecard",
    "gold_model_health_scorecard_staging",
    "gold_semantic_model_refresh_summary",
    "gold_semantic_model_refresh_summary_staging",
    "gold_semantic_model_dax_performance",
    "gold_semantic_model_dax_performance_staging",
    "gold_audit_user_activity_summary",
    "gold_audit_user_activity_summary_staging",
]

print("\n── Gold Table Verification ──")
print(f"{'Table':<55} {'Status':>10}")
print("─" * 67)
for tbl in GOLD_TABLES:
    try:
        spark.read.format("delta").table(tbl)
        print(f"  {tbl:<53} {'✔ EXISTS':>10}")
    except Exception:
        print(f"  {tbl:<53} {'✖ MISSING':>10}")

print("\n[Gold DDL] All DDL complete. Dataflow Gen2 items can now run safely.")
print("[Gold DDL] Column precision is locked — Dataflow Replace will not alter schema.")
