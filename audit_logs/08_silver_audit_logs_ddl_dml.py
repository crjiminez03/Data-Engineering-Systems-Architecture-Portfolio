# Fabric Notebook — Silver Layer: Audit Logs DDL + DML
# Portfolio Reference: Fabric Workspace Observability Project
# Layer: Silver (Cleanse, Conform, Deduplicate)
# Description: Reads from bronze_audit_logs_powerbi, applies DDL to create
#              the silver table if not exists, then runs DML to cleanse,
#              cast, parse nested JSON fields, deduplicate on Id,
#              and upsert via MERGE into silver_audit_logs_powerbi.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, to_timestamp, when, lit,
    current_timestamp, get_json_object, sha2, concat_ws,
    coalesce, regexp_replace
)
from pyspark.sql.types import IntegerType, BooleanType, StringType
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

BRONZE_TABLE = "bronze_audit_logs_powerbi"
SILVER_TABLE = "silver_audit_logs_powerbi"
MERGE_KEY    = "Id"

print(f"[Silver-Audit] Starting DDL + DML for {SILVER_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 2 — DDL: Create Silver Table
# ──────────────────────────────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
    -- Surrogate / dedup key
    Id                      STRING      NOT NULL   COMMENT 'Unique audit event ID from O365',

    -- Timestamps
    CreationTime            TIMESTAMP              COMMENT 'UTC time the audit event was created',
    log_date                DATE                   COMMENT 'Partition date derived from CreationTime',

    -- Event classification
    RecordType              INT                    COMMENT 'Record type: 21 = Power BI',
    Operation               STRING                 COMMENT 'Audit operation name',
    Activity                STRING                 COMMENT 'Friendly activity description',
    Workload                STRING                 COMMENT 'Service workload (PowerBI)',

    -- User context
    UserId                  STRING                 COMMENT 'UPN of the user who performed the action',
    UserKey                 STRING                 COMMENT 'Unique user key in Azure AD',
    UserType                INT                    COMMENT 'User type code',
    ClientIP                STRING                 COMMENT 'Client IP address',
    UserAgent               STRING                 COMMENT 'Browser or client user agent',

    -- Power BI artifact context
    ItemName                STRING                 COMMENT 'Name of the artifact acted on',
    WorkSpaceName           STRING                 COMMENT 'Power BI workspace name',
    WorkspaceId             STRING                 COMMENT 'Power BI workspace GUID',
    DatasetName             STRING                 COMMENT 'Dataset name',
    DatasetId               STRING                 COMMENT 'Dataset GUID',
    ReportName              STRING                 COMMENT 'Report name',
    ReportId                STRING                 COMMENT 'Report GUID',
    ReportType              STRING                 COMMENT 'Report type (PowerBIReport, etc.)',
    ObjectId                STRING                 COMMENT 'Object acted upon',

    -- Status
    IsSuccess               BOOLEAN                COMMENT 'Whether the operation succeeded',
    OrganizationId          STRING                 COMMENT 'Azure AD tenant/org ID',

    -- Distribution context
    DistributionMethod      STRING                 COMMENT 'How content was distributed',
    ConsumptionMethod       STRING                 COMMENT 'How content was consumed',

    -- Parsed extras from raw_json
    app_id                  STRING                 COMMENT 'Application ID from raw JSON',
    capacity_id             STRING                 COMMENT 'Premium capacity ID if applicable',
    artifact_kind           STRING                 COMMENT 'ArtifactKind from raw JSON',

    -- Audit metadata
    RequestId               STRING                 COMMENT 'Correlation request ID',
    ActivityId              STRING                 COMMENT 'Activity correlation ID',
    _batch_date             STRING                 COMMENT 'Source batch date YYYY-MM-DD',
    _silver_loaded_at       TIMESTAMP              COMMENT 'Silver load timestamp',
    _row_hash               STRING                 COMMENT 'SHA2 hash for change detection'
)
USING DELTA
PARTITIONED BY (log_date)
COMMENT 'Silver layer: cleansed Power BI audit logs from O365 Management API'
""")
print(f"[Silver-Audit] DDL applied: {SILVER_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 3 — Read Bronze (incremental: only new batch dates)
# ──────────────────────────────────────────────────────────────
# Find max batch date already in silver
try:
    max_silver_date = spark.sql(f"SELECT MAX(_batch_date) AS max_dt FROM {SILVER_TABLE}") \
                           .collect()[0]["max_dt"]
except Exception:
    max_silver_date = None

print(f"[Silver-Audit] Max date already in silver: {max_silver_date}")

df_bronze = spark.read.format("delta").table(BRONZE_TABLE)

if max_silver_date:
    df_bronze = df_bronze.filter(col("_batch_date") > max_silver_date)

bronze_count = df_bronze.count()
print(f"[Silver-Audit] Bronze rows to process: {bronze_count:,}")

if bronze_count == 0:
    print("[Silver-Audit] Nothing new to process. Exiting.")
    dbutils.notebook.exit("No new data")


# ──────────────────────────────────────────────────────────────
# Cell 4 — DML: Cleanse, Parse, Transform
# ──────────────────────────────────────────────────────────────
df_silver = (
    df_bronze

    # ── Timestamps ──────────────────────────────────────────────
    .withColumn("CreationTime", to_timestamp(col("CreationTime")))
    .withColumn("log_date",     col("CreationTime").cast("date"))

    # ── Cast types ──────────────────────────────────────────────
    .withColumn("RecordType", col("RecordType").cast(IntegerType()))
    .withColumn("UserType",   col("UserType").cast(IntegerType()))
    .withColumn("IsSuccess",
        when(upper(trim(col("IsSuccess"))) == "TRUE",  lit(True))
       .when(upper(trim(col("IsSuccess"))) == "FALSE", lit(False))
       .otherwise(lit(None).cast(BooleanType()))
    )

    # ── Trim strings ──────────────────────────────────────────────
    .withColumn("Operation",    trim(col("Operation")))
    .withColumn("Activity",     trim(col("Activity")))
    .withColumn("Workload",     trim(col("Workload")))
    .withColumn("UserId",       trim(lower(col("UserId"))))
    .withColumn("WorkSpaceName",trim(col("WorkSpaceName")))
    .withColumn("DatasetName",  trim(col("DatasetName")))
    .withColumn("ReportName",   trim(col("ReportName")))
    .withColumn("ItemName",     trim(col("ItemName")))

    # ── Parse raw_json for additional fields ─────────────────────
    .withColumn("app_id",       get_json_object(col("raw_json"), "$.AppId"))
    .withColumn("capacity_id",  get_json_object(col("raw_json"), "$.CapacityId"))
    .withColumn("artifact_kind",get_json_object(col("raw_json"), "$.ArtifactKind"))

    # ── Audit columns ─────────────────────────────────────────────
    .withColumn("_silver_loaded_at", current_timestamp())
    .withColumn("_row_hash",
        sha2(concat_ws("|",
            col("Id"), col("Operation"),
            col("UserId"), col("CreationTime")
        ), 256)
    )

    # ── Select final columns ──────────────────────────────────────
    .select(
        "Id", "CreationTime", "log_date",
        "RecordType", "Operation", "Activity", "Workload",
        "UserId", "UserKey", "UserType", "ClientIP", "UserAgent",
        "ItemName", "WorkSpaceName", "WorkspaceId",
        "DatasetName", "DatasetId",
        "ReportName", "ReportId", "ReportType",
        "ObjectId", "IsSuccess", "OrganizationId",
        "DistributionMethod", "ConsumptionMethod",
        "app_id", "capacity_id", "artifact_kind",
        "RequestId", "ActivityId",
        "_batch_date", "_silver_loaded_at", "_row_hash"
    )
    .dropDuplicates(["Id"])
)

print(f"[Silver-Audit] Transformed rows: {df_silver.count():,}")


# ──────────────────────────────────────────────────────────────
# Cell 5 — MERGE Upsert into Silver
# ──────────────────────────────────────────────────────────────
if DeltaTable.isDeltaTable(spark, f"Tables/{SILVER_TABLE}"):
    silver_dt = DeltaTable.forName(spark, SILVER_TABLE)
    (
        silver_dt.alias("tgt")
        .merge(
            df_silver.alias("src"),
            f"tgt.{MERGE_KEY} = src.{MERGE_KEY}"
        )
        .whenMatchedUpdate(
            condition="tgt._row_hash <> src._row_hash",
            set={c: f"src.{c}" for c in df_silver.columns}
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"[Silver-Audit] MERGE upsert complete → {SILVER_TABLE}")
else:
    df_silver.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .partitionBy("log_date") \
        .saveAsTable(SILVER_TABLE)
    print(f"[Silver-Audit] Initial write complete → {SILVER_TABLE}")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Post-Load Validation
# ──────────────────────────────────────────────────────────────
df_check = spark.read.format("delta").table(SILVER_TABLE)
print(f"\n[Silver-Audit] Total silver rows: {df_check.count():,}")

print("\n── Operations Distribution ──")
display(df_check.groupBy("Operation").count().orderBy("count", ascending=False).limit(20))

print("\n── Daily Volume (last 14 days) ──")
display(df_check.groupBy("log_date").count().orderBy("log_date", ascending=False).limit(14))

print("\n── Top Users by Activity ──")
display(
    df_check.groupBy("UserId")
    .count()
    .orderBy("count", ascending=False)
    .limit(15)
)
