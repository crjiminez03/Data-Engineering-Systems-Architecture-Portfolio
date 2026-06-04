# Fabric Notebook — Pipeline Audit Log: DDL + Audit_sp Definition
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Creates the pipeline_audit_log Delta table (the target of Audit_sp)
#              and defines the Audit_sp logic as a reusable PySpark function
#              callable from any notebook in the medallion pipeline.
#              Also includes a reporting section to query audit history.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, TimestampType, BooleanType
)
from datetime import datetime, timezone
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

AUDIT_TABLE = "pipeline_audit_log"
print(f"[Audit_sp] Initialising pipeline audit log: {AUDIT_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — DDL: pipeline_audit_log
# ──────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
    -- Surrogate key (auto via monotonically_increasing_id or UUID)
    audit_id                STRING      NOT NULL  COMMENT 'Unique audit event ID (UUID)',

    -- Pipeline context
    pipeline_name           STRING      NOT NULL  COMMENT 'Name of the master or child pipeline',
    notebook_name           STRING                COMMENT 'Notebook name if triggered from notebook',
    control_id              INT                   COMMENT 'FK to control_table.control_id',

    -- Table routing
    source_table            STRING                COMMENT 'Source table read from',
    destination_table       STRING      NOT NULL  COMMENT 'Destination Delta table written to',
    destination_layer       STRING                COMMENT 'BRONZE / SILVER / GOLD',
    load_type               STRING                COMMENT 'INCREMENTAL or FULL',

    -- Run timing
    run_start               TIMESTAMP   NOT NULL  COMMENT 'Pipeline run start timestamp (UTC)',
    run_end                 TIMESTAMP             COMMENT 'Pipeline run end timestamp (UTC)',
    duration_seconds        INT                   COMMENT 'Total run duration in seconds',

    -- Volume stats
    rows_read               LONG                  COMMENT 'Rows read from source',
    rows_written            LONG                  COMMENT 'Rows written to destination',
    rows_updated            LONG                  COMMENT 'Rows updated (MERGE)',
    rows_deleted            LONG                  COMMENT 'Rows deleted (if applicable)',
    rows_skipped            LONG                  COMMENT 'Rows skipped/filtered out',

    -- Status
    run_status              STRING      NOT NULL  COMMENT 'SUCCESS / FAILURE / PARTIAL',
    error_message           STRING                COMMENT 'Error details if run_status = FAILURE',
    error_step              STRING                COMMENT 'Which step or cell failed',

    -- Watermark tracking
    watermark_start         TIMESTAMP             COMMENT 'Start of the data window processed',
    watermark_end           TIMESTAMP             COMMENT 'End of the data window processed',
    new_watermark_value     TIMESTAMP             COMMENT 'Watermark advanced to after success',

    -- Server/connection context
    connection_name         STRING                COMMENT 'Named connection used (server switch)',
    source_database         STRING                COMMENT 'Source database on-prem or cloud',

    -- Audit metadata
    triggered_by            STRING                COMMENT 'User or system that triggered the run',
    fabric_run_id           STRING                COMMENT 'Fabric activity/pipeline run ID',
    logged_at               TIMESTAMP             COMMENT 'When this audit row was written',

    -- Partition
    audit_date              DATE                  COMMENT 'Date of the run (partition column)'
)
USING DELTA
PARTITIONED BY (audit_date)
COMMENT 'Audit log for all pipeline and notebook runs across the medallion architecture'
""")
print(f"[Audit_sp] DDL applied: {AUDIT_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Audit_sp Function Definition
#           Call this at the end of every successful pipeline step
# ──────────────────────────────────────────────────────────────
import uuid

def audit_sp(
    pipeline_name:      str,
    destination_table:  str,
    run_status:         str,
    run_start:          datetime,
    run_end:            datetime           = None,
    notebook_name:      str                = None,
    control_id:         int                = None,
    source_table:       str                = None,
    destination_layer:  str                = None,
    load_type:          str                = None,
    rows_read:          int                = 0,
    rows_written:       int                = 0,
    rows_updated:       int                = 0,
    rows_deleted:       int                = 0,
    rows_skipped:       int                = 0,
    error_message:      str                = None,
    error_step:         str                = None,
    watermark_start:    datetime           = None,
    watermark_end:      datetime           = None,
    new_watermark_value:datetime           = None,
    connection_name:    str                = None,
    source_database:    str                = None,
    triggered_by:       str                = None,
    fabric_run_id:      str                = None,
) -> str:
    """
    Write one audit record to pipeline_audit_log.
    Returns the audit_id of the row written.

    Usage:
        from pipeline_audit import audit_sp
        audit_id = audit_sp(
            pipeline_name     = "SilverMasterNotebook",
            destination_table = "silver_orders",
            run_status        = "SUCCESS",
            run_start         = run_start_dt,
            run_end           = datetime.now(timezone.utc),
            rows_read         = 50000,
            rows_written      = 49987,
            load_type         = "INCREMENTAL",
            destination_layer = "SILVER",
        )
    """
    run_end     = run_end or datetime.now(timezone.utc)
    audit_id    = str(uuid.uuid4())
    duration    = int((run_end - run_start).total_seconds())
    audit_date  = run_start.date()

    def _ts(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%S") if dt else None

    row = [(
        audit_id,
        pipeline_name,
        notebook_name,
        control_id,
        source_table,
        destination_table,
        destination_layer,
        load_type,
        _ts(run_start),
        _ts(run_end),
        duration,
        rows_read,
        rows_written,
        rows_updated,
        rows_deleted,
        rows_skipped,
        run_status,
        error_message,
        error_step,
        _ts(watermark_start),
        _ts(watermark_end),
        _ts(new_watermark_value),
        connection_name,
        source_database,
        triggered_by or "SYSTEM",
        fabric_run_id,
        datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        str(audit_date),
    )]

    schema_cols = [
        "audit_id", "pipeline_name", "notebook_name", "control_id",
        "source_table", "destination_table", "destination_layer", "load_type",
        "run_start", "run_end", "duration_seconds",
        "rows_read", "rows_written", "rows_updated", "rows_deleted", "rows_skipped",
        "run_status", "error_message", "error_step",
        "watermark_start", "watermark_end", "new_watermark_value",
        "connection_name", "source_database",
        "triggered_by", "fabric_run_id", "logged_at", "audit_date"
    ]

    df_audit = spark.createDataFrame(row, schema=schema_cols)

    # Cast timestamp strings to actual timestamp types
    for ts_col in ["run_start","run_end","watermark_start","watermark_end","new_watermark_value","logged_at"]:
        df_audit = df_audit.withColumn(ts_col, to_timestamp(col(ts_col)))

    df_audit = df_audit.withColumn("audit_date", col("audit_date").cast("date"))
    df_audit = df_audit.withColumn("control_id",  col("control_id").cast("int"))
    for lc in ["rows_read","rows_written","rows_updated","rows_deleted","rows_skipped"]:
        df_audit = df_audit.withColumn(lc, col(lc).cast("long"))
    df_audit = df_audit.withColumn("duration_seconds", col("duration_seconds").cast("int"))

    df_audit.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("audit_date") \
        .saveAsTable(AUDIT_TABLE)

    return audit_id


# ──────────────────────────────────────────────────────────────
# Cell 4 — Example: How to Call Audit_sp in a Child Notebook
# ──────────────────────────────────────────────────────────────
"""
USAGE EXAMPLE — paste this pattern into any bronze/silver notebook:

from datetime import datetime, timezone

run_start_dt = datetime.now(timezone.utc)

try:
    # ... your ingestion logic here ...
    rows_read    = df_source.count()
    rows_written = df_dest.count()
    run_status   = "SUCCESS"
    error_msg    = None
except Exception as e:
    rows_read = rows_written = 0
    run_status = "FAILURE"
    error_msg  = str(e)
    raise
finally:
    audit_sp(
        pipeline_name     = "SilverMasterNotebook",
        destination_table = "silver_orders",
        run_status        = run_status,
        run_start         = run_start_dt,
        run_end           = datetime.now(timezone.utc),
        destination_layer = "SILVER",
        load_type         = "INCREMENTAL",
        source_table      = "bronze_orders",
        rows_read         = rows_read,
        rows_written      = rows_written,
        error_message     = error_msg,
        connection_name   = "SSMS_Server1",
        source_database   = "OperationsDB",
    )
"""


# ──────────────────────────────────────────────────────────────
# Cell 5 — Test: Write a sample audit record
# ──────────────────────────────────────────────────────────────
test_audit_id = audit_sp(
    pipeline_name     = "TEST_RUN",
    destination_table = "pipeline_audit_log_test",
    run_status        = "SUCCESS",
    run_start         = datetime(2024, 6, 1, 2, 0, 0, tzinfo=timezone.utc),
    run_end           = datetime(2024, 6, 1, 2, 4, 37, tzinfo=timezone.utc),
    destination_layer = "SILVER",
    load_type         = "INCREMENTAL",
    rows_read         = 125_000,
    rows_written      = 124_987,
    rows_updated      = 45,
    rows_skipped      = 13,
    connection_name   = "SSMS_Server1",
    source_database   = "OperationsDB",
    triggered_by      = "SilverMasterNotebook",
)
print(f"[Audit_sp] Test record written. audit_id: {test_audit_id}")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Audit Log Reporting Queries
# ──────────────────────────────────────────────────────────────
df_audit = spark.read.format("delta").table(AUDIT_TABLE)

print("\n── Pipeline Run History (last 14 days) ──")
display(
    df_audit
    .filter(col("audit_date") >= (
        spark.sql("SELECT DATE_SUB(CURRENT_DATE, 14)").collect()[0][0]
    ))
    .select("audit_date", "pipeline_name", "destination_table",
            "run_status", "rows_read", "rows_written",
            "duration_seconds", "load_type", "connection_name")
    .orderBy("run_start", ascending=False)
)

print("\n── Failure Summary ──")
display(
    df_audit.filter(col("run_status") == "FAILURE")
    .select("audit_date", "pipeline_name", "destination_table",
            "error_step", "error_message", "duration_seconds")
    .orderBy("run_start", ascending=False)
    .limit(25)
)

print("\n── Row Volume Trends by Destination Table ──")
display(
    df_audit
    .filter(col("run_status") == "SUCCESS")
    .groupBy("destination_table", "destination_layer", "load_type")
    .agg(
        count("*").alias("total_runs"),
        spark_round(avg("rows_read"),    0).alias("avg_rows_read"),
        spark_round(avg("rows_written"), 0).alias("avg_rows_written"),
        spark_round(avg("duration_seconds"), 0).alias("avg_duration_sec"),
        spark_max("rows_written").alias("max_rows_written"),
    )
    .orderBy("avg_rows_written", ascending=False)
)

print("\n── Slowest Pipeline Runs ──")
display(
    df_audit
    .orderBy("duration_seconds", ascending=False)
    .select("audit_date", "pipeline_name", "destination_table",
            "duration_seconds", "rows_read", "rows_written", "run_status")
    .limit(15)
)

from pyspark.sql.functions import count, avg as spark_avg, spark_max

print("\n── Daily Run Success Rate ──")
display(
    df_audit.groupBy("audit_date")
    .agg(
        count("*").alias("total_runs"),
        count(when(col("run_status") == "SUCCESS", 1)).alias("successes"),
        spark_round(
            count(when(col("run_status") == "SUCCESS", 1)) / count("*") * 100, 1
        ).alias("success_rate_pct")
    )
    .orderBy("audit_date", ascending=False)
)
