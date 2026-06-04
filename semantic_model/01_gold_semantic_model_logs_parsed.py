# Fabric Notebook — Gold Layer: SemanticModelLogs Ingestion + Parsed Table
# Portfolio Reference: Fabric Workspace Observability Project
# Layer: Gold (SemanticModelLogs lives at Gold — it tracks activity ON top of Gold tables)
#
# Description:
#   Reads raw SemanticModelLogs from the Eventhouse KQL database,
#   parses the JSON payloads in ApplicationContext and EventText to extract:
#     - UPN (user principal name)
#     - ReportId, ReportName
#     - VisualId, VisualName
#     - PageId, PageName
#     - DAX query text
#     - Calling application / client tool
#   Writes two Gold tables:
#     1. gold_semantic_model_logs          — cleansed, typed, deduplicated raw log
#     2. gold_semantic_model_logs_parsed   — fully parsed with all JSON fields extracted
#
# Architecture Note:
#   SemanticModelLogs does NOT have Bronze or Silver layers.
#   The semantic model sits ON TOP of Gold Delta tables.
#   These logs track report/visual usage patterns AGAINST those Gold tables.
#   There is no upstream raw layer — the KQL Eventhouse IS the source of truth.
#
# Execution:
#   Runs as part of Gold + Semantic Model Master Pipeline
#   after Gold Delta tables are confirmed loaded (Step 3 complete).

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, get_json_object, to_timestamp, trim,
    upper, lower, coalesce, lit, current_timestamp,
    when, sha2, concat_ws, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)
from delta.tables import DeltaTable
from datetime import datetime, timedelta, timezone

spark = SparkSession.builder.getOrCreate()

# ── Configuration ──────────────────────────────────────────────
KQL_CLUSTER_URI  = "https://your-cluster.kusto.fabric.microsoft.com"  # <-- Replace
KQL_DATABASE     = "YourKQLDatabase"                                    # <-- Replace

GOLD_LOGS_TABLE   = "gold_semantic_model_logs"
GOLD_PARSED_TABLE = "gold_semantic_model_logs_parsed"

LOAD_TYPE      = "incremental"     # "full" or "incremental"
WATERMARK_DAYS = 1

utc_now   = datetime.now(timezone.utc)
end_dt    = utc_now
start_dt  = end_dt - timedelta(days=WATERMARK_DAYS) if LOAD_TYPE == "incremental" \
            else datetime(2020, 1, 1, tzinfo=timezone.utc)

START_STR = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
END_STR   = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"[Gold-SML] SemanticModelLogs ingestion | {LOAD_TYPE}")
print(f"[Gold-SML] Window: {START_STR}  →  {END_STR}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — KQL Query: Pull SemanticModelLogs from Eventhouse
# ──────────────────────────────────────────────────────────────
kql_query = f"""
WorkspaceLogs
| where Timestamp >= datetime('{START_STR}')
  and   Timestamp <  datetime('{END_STR}')
| where ItemKind == 'SemanticModel'
| project
    Timestamp,
    OperationName,
    OperationDetailName,
    OperationId,
    DurationMs,
    EventText,
    ExecutingUser,
    Status,
    StatusCode,
    CpuTimeMs,
    Identity,
    ApplicationContext,
    ItemId,
    ItemKind,
    ItemName,
    WorkspaceId,
    WorkspaceName,
    ModelMode
"""

df_raw = (
    spark.read
    .format("com.microsoft.kusto.spark.datasource")
    .option("kustoCluster",  KQL_CLUSTER_URI)
    .option("kustoDatabase", KQL_DATABASE)
    .option("kustoQuery",    kql_query)
    .option("accessToken",   mssparkutils.credentials.getToken(KQL_CLUSTER_URI))
    .load()
)

raw_count = df_raw.count()
print(f"[Gold-SML] Rows read from KQL: {raw_count:,}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — DDL: gold_semantic_model_logs (cleansed base table)
# ──────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_LOGS_TABLE} (
    OperationId             STRING      COMMENT 'Unique ID correlating events across log records',
    Timestamp               TIMESTAMP   COMMENT 'UTC timestamp of the log entry',
    OperationName           STRING      COMMENT 'Semantic model operation e.g. Execute DAX, Refresh',
    OperationDetailName     STRING      COMMENT 'Further detail on the operation or state',
    Status                  STRING      COMMENT 'SUCCESS / FAILURE / STARTED / COMPLETED',
    StatusCode              STRING      COMMENT 'HTTP or system status code',
    DurationMs              LONG        COMMENT 'Operation duration in milliseconds',
    CpuTimeMs               LONG        COMMENT 'CPU time consumed in milliseconds',
    ExecutingUser           STRING      COMMENT 'User or system identity that ran the operation',
    EventText               STRING      COMMENT 'Raw event payload — DAX query text or refresh status',
    Identity                STRING      COMMENT 'Raw Identity JSON string',
    ApplicationContext      STRING      COMMENT 'Raw ApplicationContext JSON string',
    ItemId                  STRING      COMMENT 'Semantic model unique identifier',
    ItemKind                STRING      COMMENT 'SemanticModel',
    ItemName                STRING      COMMENT 'Power BI semantic model name',
    WorkspaceId             STRING      COMMENT 'Workspace GUID',
    WorkspaceName           STRING      COMMENT 'Fabric workspace name',
    ModelMode               STRING      COMMENT 'Import / DirectQuery / Composite',
    _gold_loaded_at         TIMESTAMP   COMMENT 'Row load timestamp in Gold',
    _row_hash               STRING      COMMENT 'SHA2 hash for change detection'
)
USING DELTA
COMMENT 'Gold layer: cleansed SemanticModelLogs from Eventhouse KQL'
""")
print(f"[Gold-SML] DDL applied: {GOLD_LOGS_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 4 — DDL: gold_semantic_model_logs_parsed
#           The key table — every JSON field extracted to its own column
# ──────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOLD_PARSED_TABLE} (
    OperationId             STRING      COMMENT 'Unique operation ID — MERGE key',
    Timestamp               TIMESTAMP   COMMENT 'UTC timestamp',
    OperationName           STRING      COMMENT 'Operation e.g. Execute DAX, Refresh',
    OperationDetailName     STRING      COMMENT 'Detail on the specific action',
    Status                  STRING      COMMENT 'SUCCESS / FAILURE / STARTED / COMPLETED',
    StatusCode              STRING      COMMENT 'Status code',
    DurationMs              LONG        COMMENT 'Duration ms',
    CpuTimeMs               LONG        COMMENT 'CPU time ms',

    -- Executing identity
    ExecutingUser           STRING      COMMENT 'Raw executing user field from log',

    -- ── Parsed from Identity JSON ─────────────────────────────
    upn                     STRING      COMMENT 'User Principal Name extracted from Identity JSON',
    user_object_id          STRING      COMMENT 'Azure AD Object ID from Identity JSON',
    tenant_id               STRING      COMMENT 'Tenant ID from Identity JSON',

    -- ── Parsed from ApplicationContext JSON ───────────────────
    -- ApplicationContext carries report/visual/page context
    -- when the operation is triggered by a Power BI visual render
    report_id               STRING      COMMENT 'ReportId extracted from ApplicationContext JSON',
    report_name             STRING      COMMENT 'ReportName from ApplicationContext JSON',
    visual_id               STRING      COMMENT 'VisualId from ApplicationContext JSON',
    visual_name             STRING      COMMENT 'VisualName from ApplicationContext JSON',
    page_id                 STRING      COMMENT 'PageId from ApplicationContext JSON',
    page_name               STRING      COMMENT 'PageName from ApplicationContext JSON',
    client_type             STRING      COMMENT 'Client application type e.g. PBIClient, Excel',
    activity_id             STRING      COMMENT 'ActivityId correlation from ApplicationContext',

    -- ── Parsed from EventText JSON ────────────────────────────
    -- EventText contains the actual DAX query text for Execute DAX operations
    -- and refresh payload details for Refresh operations
    dax_query_text          STRING      COMMENT 'DAX query text extracted from EventText (Execute DAX ops)',
    event_operation_status  STRING      COMMENT 'Operation status detail from EventText',
    event_error_message     STRING      COMMENT 'Error message from EventText if Status=FAILURE',
    event_text_raw          STRING      COMMENT 'Full raw EventText string preserved for reference',

    -- Item / Workspace
    ItemId                  STRING      COMMENT 'Semantic model GUID',
    ItemName                STRING      COMMENT 'Semantic model name',
    WorkspaceId             STRING      COMMENT 'Workspace GUID',
    WorkspaceName           STRING      COMMENT 'Workspace name',
    ModelMode               STRING      COMMENT 'Import / DirectQuery / Composite',

    -- Audit
    _gold_loaded_at         TIMESTAMP   COMMENT 'Gold load timestamp',
    _row_hash               STRING      COMMENT 'SHA2 hash for MERGE change detection'
)
USING DELTA
COMMENT 'Gold layer: fully parsed SemanticModelLogs with UPN, ReportId, VisualId, PageId extracted'
""")
print(f"[Gold-SML] DDL applied: {GOLD_PARSED_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Parse ApplicationContext JSON
#
# ApplicationContext structure (when triggered by a Power BI visual):
# {
#   "Sources": [{
#     "ReportId":   "guid",
#     "ReportName": "My Report",
#     "VisualId":   "guid",
#     "VisualName": "Sales Bar Chart",
#     "PageId":     "guid",
#     "PageName":   "Overview"
#   }],
#   "Version":      "1.0",
#   "ActivityId":   "guid",
#   "ClientType":   "PBIClient"
# }
# ──────────────────────────────────────────────────────────────
df_parsed = (
    df_raw

    # ── Cast Timestamp ──────────────────────────────────────────
    .withColumn("Timestamp", to_timestamp(col("Timestamp")))

    # ── Parse Identity JSON ─────────────────────────────────────
    .withColumn("upn",
        coalesce(
            get_json_object(col("Identity"), "$.Upn"),
            get_json_object(col("Identity"), "$.upn"),
            col("ExecutingUser")
        )
    )
    .withColumn("user_object_id",
        get_json_object(col("Identity"), "$.ObjectId")
    )
    .withColumn("tenant_id",
        get_json_object(col("Identity"), "$.TenantId")
    )

    # ── Parse ApplicationContext JSON ────────────────────────────
    # ReportId, ReportName, VisualId, VisualName, PageId, PageName
    # sit inside the Sources[0] array
    .withColumn("report_id",
        coalesce(
            get_json_object(col("ApplicationContext"), "$.Sources[0].ReportId"),
            get_json_object(col("ApplicationContext"), "$.ReportId")
        )
    )
    .withColumn("report_name",
        coalesce(
            get_json_object(col("ApplicationContext"), "$.Sources[0].ReportName"),
            get_json_object(col("ApplicationContext"), "$.ReportName")
        )
    )
    .withColumn("visual_id",
        coalesce(
            get_json_object(col("ApplicationContext"), "$.Sources[0].VisualId"),
            get_json_object(col("ApplicationContext"), "$.VisualId")
        )
    )
    .withColumn("visual_name",
        coalesce(
            get_json_object(col("ApplicationContext"), "$.Sources[0].VisualName"),
            get_json_object(col("ApplicationContext"), "$.VisualName")
        )
    )
    .withColumn("page_id",
        coalesce(
            get_json_object(col("ApplicationContext"), "$.Sources[0].PageId"),
            get_json_object(col("ApplicationContext"), "$.PageId")
        )
    )
    .withColumn("page_name",
        coalesce(
            get_json_object(col("ApplicationContext"), "$.Sources[0].PageName"),
            get_json_object(col("ApplicationContext"), "$.PageName")
        )
    )
    .withColumn("client_type",
        get_json_object(col("ApplicationContext"), "$.ClientType")
    )
    .withColumn("activity_id",
        get_json_object(col("ApplicationContext"), "$.ActivityId")
    )

    # ── Parse EventText JSON ──────────────────────────────────────
    # Execute DAX: EventText contains {"Text": "EVALUATE ..."}
    # Refresh:     EventText contains {"OperationStatus": "...", "ErrorMessage": "..."}
    .withColumn("dax_query_text",
        when(
            upper(trim(col("OperationName"))).isin(
                "EXECUTE DAX", "EXECUTEDAX", "QUERYDAX"
            ),
            coalesce(
                get_json_object(col("EventText"), "$.Text"),
                get_json_object(col("EventText"), "$.QueryText"),
            )
        ).otherwise(lit(None).cast(StringType()))
    )
    .withColumn("event_operation_status",
        get_json_object(col("EventText"), "$.OperationStatus")
    )
    .withColumn("event_error_message",
        coalesce(
            get_json_object(col("EventText"), "$.ErrorMessage"),
            get_json_object(col("EventText"), "$.Error"),
        )
    )
    .withColumn("event_text_raw", col("EventText"))

    # ── Cleanse strings ───────────────────────────────────────────
    .withColumn("OperationName",       trim(col("OperationName")))
    .withColumn("OperationDetailName", trim(col("OperationDetailName")))
    .withColumn("Status",              trim(upper(col("Status"))))
    .withColumn("ExecutingUser",       trim(lower(col("ExecutingUser"))))
    .withColumn("upn",                 trim(lower(col("upn"))))
    .withColumn("ItemName",            trim(col("ItemName")))
    .withColumn("WorkspaceName",       trim(col("WorkspaceName")))
    .withColumn("ModelMode",           trim(col("ModelMode")))

    # ── Cast numerics ─────────────────────────────────────────────
    .withColumn("DurationMs", col("DurationMs").cast(LongType()))
    .withColumn("CpuTimeMs",  col("CpuTimeMs").cast(LongType()))

    # ── Audit columns ─────────────────────────────────────────────
    .withColumn("_gold_loaded_at", current_timestamp())
    .withColumn("_row_hash",
        sha2(concat_ws("|",
            col("OperationId"),
            col("Timestamp"),
            col("Status"),
            col("DurationMs"),
            col("report_id"),
            col("visual_id")
        ), 256)
    )

    # ── Dedup ─────────────────────────────────────────────────────
    .dropDuplicates(["OperationId", "Timestamp"])

    # ── Select final columns ──────────────────────────────────────
    .select(
        "OperationId", "Timestamp",
        "OperationName", "OperationDetailName",
        "Status", "StatusCode",
        "DurationMs", "CpuTimeMs",
        "ExecutingUser",
        "upn", "user_object_id", "tenant_id",
        "report_id", "report_name",
        "visual_id", "visual_name",
        "page_id", "page_name",
        "client_type", "activity_id",
        "dax_query_text",
        "event_operation_status", "event_error_message", "event_text_raw",
        "ItemId", "ItemName",
        "WorkspaceId", "WorkspaceName", "ModelMode",
        "_gold_loaded_at", "_row_hash"
    )
)

parsed_count = df_parsed.count()
print(f"[Gold-SML] Parsed rows: {parsed_count:,}")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Build Cleansed Base Table (gold_semantic_model_logs)
# ──────────────────────────────────────────────────────────────
df_base = (
    df_raw
    .withColumn("Timestamp",   to_timestamp(col("Timestamp")))
    .withColumn("Status",      trim(upper(col("Status"))))
    .withColumn("DurationMs",  col("DurationMs").cast(LongType()))
    .withColumn("CpuTimeMs",   col("CpuTimeMs").cast(LongType()))
    .withColumn("ExecutingUser", trim(lower(col("ExecutingUser"))))
    .withColumn("ItemName",    trim(col("ItemName")))
    .withColumn("_gold_loaded_at", current_timestamp())
    .withColumn("_row_hash",
        sha2(concat_ws("|",
            col("OperationId"), col("Timestamp"), col("Status")
        ), 256)
    )
    .dropDuplicates(["OperationId", "Timestamp"])
    .select(
        "OperationId", "Timestamp",
        "OperationName", "OperationDetailName",
        "Status", "StatusCode",
        "DurationMs", "CpuTimeMs", "ExecutingUser",
        "EventText", "Identity", "ApplicationContext",
        "ItemId", "ItemKind", "ItemName",
        "WorkspaceId", "WorkspaceName", "ModelMode",
        "_gold_loaded_at", "_row_hash"
    )
)


# ──────────────────────────────────────────────────────────────
# Cell 7 — MERGE into Both Gold Tables
# ──────────────────────────────────────────────────────────────
def merge_or_write(df, table_name: str, merge_keys: list):
    condition = " AND ".join([f"tgt.{k} = src.{k}" for k in merge_keys])
    if DeltaTable.isDeltaTable(spark, f"Tables/{table_name}"):
        dt = DeltaTable.forName(spark, table_name)
        (
            dt.alias("tgt")
            .merge(df.alias("src"), condition)
            .whenMatchedUpdate(
                condition="tgt._row_hash <> src._row_hash",
                set={c: f"src.{c}" for c in df.columns}
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[Gold-SML] MERGE complete → {table_name}")
    else:
        df.write.format("delta").mode("overwrite") \
          .option("overwriteSchema", "true") \
          .saveAsTable(table_name)
        print(f"[Gold-SML] Initial write → {table_name}: {df.count():,} rows")

merge_or_write(df_base,   GOLD_LOGS_TABLE,   ["OperationId", "Timestamp"])
merge_or_write(df_parsed, GOLD_PARSED_TABLE, ["OperationId", "Timestamp"])


# ──────────────────────────────────────────────────────────────
# Cell 8 — Quick Validation & Exploration
# ──────────────────────────────────────────────────────────────
df_out = spark.read.format("delta").table(GOLD_PARSED_TABLE)
print(f"\n[Gold-SML] {GOLD_PARSED_TABLE} total rows: {df_out.count():,}")

print("\n── Rows with Report/Visual context (Power BI visual renders) ──")
display(
    df_out.filter(col("report_id").isNotNull())
    .select("Timestamp", "upn", "report_name", "visual_name",
            "page_name", "client_type", "DurationMs", "Status")
    .orderBy("Timestamp", ascending=False)
    .limit(20)
)

print("\n── Top Reports by DAX Execution Count ──")
display(
    df_out.filter(col("report_id").isNotNull())
    .groupBy("report_name", "report_id")
    .count()
    .withColumnRenamed("count", "dax_calls")
    .orderBy("dax_calls", ascending=False)
    .limit(15)
)

print("\n── Top Visuals by Execution Count ──")
display(
    df_out.filter(col("visual_id").isNotNull())
    .groupBy("report_name", "page_name", "visual_name")
    .count()
    .withColumnRenamed("count", "render_count")
    .orderBy("render_count", ascending=False)
    .limit(15)
)

print("\n── Top Users by DAX Operations ──")
display(
    df_out.groupBy("upn")
    .count()
    .withColumnRenamed("count", "total_ops")
    .orderBy("total_ops", ascending=False)
    .limit(15)
)

print("\n── Null Rate on Key Parsed Fields ──")
from pyspark.sql.functions import isnull, round as spark_round
total = df_out.count()
for field in ["upn", "report_id", "visual_id", "page_id", "dax_query_text"]:
    null_cnt  = df_out.filter(isnull(col(field))).count()
    null_pct  = round(null_cnt / total * 100, 1) if total > 0 else 0
    pop_pct   = round(100 - null_pct, 1)
    bar       = "█" * int(pop_pct / 5) + "░" * (20 - int(pop_pct / 5))
    print(f"  {field:25s} populated: {pop_pct:5.1f}%  [{bar}]  "
          f"({total - null_cnt:,} of {total:,} rows)")
