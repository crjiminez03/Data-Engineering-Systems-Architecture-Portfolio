# Fabric Notebook — Bronze Layer: Audit Log Ingestion via Microsoft Graph API
# Portfolio Reference: Fabric Workspace Observability Project
# Layer: Bronze (Raw Ingestion)
# Description: Calls Microsoft Graph API /auditLogs/signIns equivalent via
#              Office 365 Management Activity API to pull Power BI audit records
#              (RecordType = 21) for a full 24hr window with full pagination,
#              bypassing the 5000-row limit that plagued the PowerShell approach.
#              Designed to run daily at 2am to capture the prior day's data.
#
# API Used: Office 365 Management Activity API
#   https://manage.office.com/api/v1.0/{tenant_id}/activity/feed/subscriptions/content
#   ContentType: Audit.General  (contains Power BI RecordType=21 events)
#
# Auth: Managed Identity / Service Principal via mssparkutils

# ──────────────────────────────────────────────────────────────
# Cell 1 — Imports & Config
# ──────────────────────────────────────────────────────────────
import requests
import json
import time
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    BooleanType, TimestampType
)

spark = SparkSession.builder.getOrCreate()

# ── Configuration ───────────────────────────────────────────────
TENANT_ID          = mssparkutils.secrets.get("KeyVaultName", "TenantId")        # <-- KV secret
CLIENT_ID          = mssparkutils.secrets.get("KeyVaultName", "AuditClientId")   # <-- KV secret
CLIENT_SECRET      = mssparkutils.secrets.get("KeyVaultName", "AuditClientSecret") # <-- KV secret

BRONZE_TABLE       = "bronze_audit_logs_powerbi"
RECORD_TYPE_FILTER = 21           # Power BI audit record type
CONTENT_TYPE       = "Audit.General"
MAX_RETRIES        = 3
RETRY_DELAY_SEC    = 5
PAGE_SIZE          = 1000         # API returns up to 1000 per blob; we paginate across blobs

# ── Date window: yesterday full day (UTC) ──────────────────────
utc_now   = datetime.now(timezone.utc)
# Run at 2am => capture previous full day
end_dt    = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
start_dt  = end_dt - timedelta(days=1)

START_STR = start_dt.strftime("%Y-%m-%dT%H:%M:%S")
END_STR   = end_dt.strftime("%Y-%m-%dT%H:%M:%S")

print(f"[Bronze-Audit] Window: {START_STR}  →  {END_STR}")
print(f"[Bronze-Audit] RecordType filter: {RECORD_TYPE_FILTER} (Power BI)")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Authenticate: Get Bearer Token
# ──────────────────────────────────────────────────────────────
def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> str:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scope":         "https://manage.office.com/.default",
    }
    resp = requests.post(url, data=payload, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    print(f"[Auth] Token acquired (length: {len(token)})")
    return token

TOKEN = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type":  "application/json",
}


# ──────────────────────────────────────────────────────────────
# Cell 3 — Ensure Subscription is Active
# ──────────────────────────────────────────────────────────────
def ensure_subscription(tenant_id: str, content_type: str, headers: dict):
    """Start subscription if not already active."""
    list_url  = f"https://manage.office.com/api/v1.0/{tenant_id}/activity/feed/subscriptions/list"
    start_url = f"https://manage.office.com/api/v1.0/{tenant_id}/activity/feed/subscriptions/start?contentType={content_type}"

    resp = requests.get(list_url, headers=headers, timeout=30)
    resp.raise_for_status()
    subs = resp.json()

    active = any(
        s.get("contentType") == content_type and s.get("status") == "enabled"
        for s in subs
    )
    if not active:
        r = requests.post(start_url, headers=headers, timeout=30)
        r.raise_for_status()
        print(f"[Subscription] Started subscription for: {content_type}")
    else:
        print(f"[Subscription] Already active: {content_type}")

ensure_subscription(TENANT_ID, CONTENT_TYPE, HEADERS)


# ──────────────────────────────────────────────────────────────
# Cell 4 — Fetch Content Blob URIs (handles NextPageUri pagination)
# ──────────────────────────────────────────────────────────────
def fetch_content_uris(tenant_id: str, content_type: str,
                        start: str, end: str, headers: dict) -> list:
    """
    The Management API paginates content blob URIs via NextPageUri header.
    Each URI points to a blob containing up to 1000 audit records.
    This is how we break the 5000-row PowerShell limit — we collect ALL blobs.
    """
    base_url = (
        f"https://manage.office.com/api/v1.0/{tenant_id}/activity/feed/subscriptions/content"
        f"?contentType={content_type}&startTime={start}&endTime={end}"
    )

    all_uris   = []
    next_url   = base_url
    page_num   = 0

    while next_url:
        page_num += 1
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(next_url, headers=headers, timeout=60)
                resp.raise_for_status()
                break
            except requests.RequestException as e:
                print(f"  [Retry {attempt}/{MAX_RETRIES}] URI page {page_num}: {e}")
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(RETRY_DELAY_SEC)

        blobs = resp.json()
        uris  = [b["contentUri"] for b in blobs if "contentUri" in b]
        all_uris.extend(uris)
        print(f"  [Content URIs] Page {page_num}: {len(uris)} blobs found (total so far: {len(all_uris)})")

        # Check for next page in response header
        next_url = resp.headers.get("NextPageUri", None)

    print(f"[Content URIs] Total blobs to fetch: {len(all_uris)}")
    return all_uris


content_uris = fetch_content_uris(TENANT_ID, CONTENT_TYPE, START_STR, END_STR, HEADERS)


# ──────────────────────────────────────────────────────────────
# Cell 5 — Fetch All Records from Each Blob URI
# ──────────────────────────────────────────────────────────────
def fetch_blob_records(uri: str, headers: dict, record_type_filter: int) -> list:
    """Download one content blob and filter to target RecordType."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(uri, headers=headers, timeout=120)
            resp.raise_for_status()
            records = resp.json()
            return [r for r in records if r.get("RecordType") == record_type_filter]
        except requests.RequestException as e:
            print(f"  [Retry {attempt}/{MAX_RETRIES}] Blob fetch error: {e}")
            if attempt == MAX_RETRIES:
                return []
            time.sleep(RETRY_DELAY_SEC)

all_records = []
for i, uri in enumerate(content_uris, 1):
    recs = fetch_blob_records(uri, HEADERS, RECORD_TYPE_FILTER)
    all_records.extend(recs)
    if i % 10 == 0 or i == len(content_uris):
        print(f"  [Blobs] {i}/{len(content_uris)} processed — {len(all_records):,} PBI records so far")

print(f"\n[Bronze-Audit] Total Power BI audit records fetched: {len(all_records):,}")


# ──────────────────────────────────────────────────────────────
# Cell 6 — Flatten Records to Spark DataFrame
# ──────────────────────────────────────────────────────────────
def flatten_record(r: dict) -> dict:
    """Flatten common audit log fields + Power BI specific fields."""
    return {
        # Core audit fields
        "Id":                  str(r.get("Id", "")),
        "RecordType":          int(r.get("RecordType", 0)),
        "CreationTime":        str(r.get("CreationTime", "")),
        "Operation":           str(r.get("Operation", "")),
        "OrganizationId":      str(r.get("OrganizationId", "")),
        "UserType":            int(r.get("UserType", -1)),
        "UserKey":             str(r.get("UserKey", "")),
        "Workload":            str(r.get("Workload", "")),
        "UserId":              str(r.get("UserId", "")),
        "ClientIP":            str(r.get("ClientIP", "")),
        "UserAgent":           str(r.get("UserAgent", "")),
        "Activity":            str(r.get("Activity", "")),
        "ItemName":            str(r.get("ItemName", "")),
        "WorkSpaceName":       str(r.get("WorkSpaceName", "")),
        "DatasetName":         str(r.get("DatasetName", "")),
        "DatasetId":           str(r.get("DatasetId", "")),
        "ReportName":          str(r.get("ReportName", "")),
        "ReportId":            str(r.get("ReportId", "")),
        "ReportType":          str(r.get("ReportType", "")),
        "WorkspaceId":         str(r.get("WorkspaceId", "")),
        "ObjectId":            str(r.get("ObjectId", "")),
        "IsSuccess":           str(r.get("IsSuccess", "")),
        "RequestId":           str(r.get("RequestId", "")),
        "ActivityId":          str(r.get("ActivityId", "")),
        "DistributionMethod":  str(r.get("DistributionMethod", "")),
        "ConsumptionMethod":   str(r.get("ConsumptionMethod", "")),
        # Raw JSON for full fidelity
        "raw_json":            json.dumps(r),
    }

flat_records = [flatten_record(r) for r in all_records]

if flat_records:
    df_bronze = spark.createDataFrame(flat_records)
    df_bronze = (
        df_bronze
        .withColumn("CreationTime", to_timestamp(col("CreationTime"), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source",              lit("O365_Management_API"))
        .withColumn("_batch_date",          lit(start_dt.strftime("%Y-%m-%d")))
        .withColumn("_record_type",         lit(RECORD_TYPE_FILTER))
    )
    print(f"[Bronze-Audit] DataFrame created: {df_bronze.count():,} rows, {len(df_bronze.columns)} columns")
else:
    print("[Bronze-Audit] WARNING: No records returned for this window.")
    df_bronze = spark.createDataFrame([], schema=StructType([
        StructField("Id", StringType(), True),
        StructField("raw_json", StringType(), True),
    ]))


# ──────────────────────────────────────────────────────────────
# Cell 7 — Write to Bronze Delta Table (append by batch date)
# ──────────────────────────────────────────────────────────────
df_bronze.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("_batch_date") \
    .saveAsTable(BRONZE_TABLE)

print(f"[Bronze-Audit] Written to {BRONZE_TABLE} partitioned by _batch_date")


# ──────────────────────────────────────────────────────────────
# Cell 8 — Validation Summary
# ──────────────────────────────────────────────────────────────
df_check = spark.read.format("delta").table(BRONZE_TABLE)
print(f"\n[Bronze-Audit] Total rows in table: {df_check.count():,}")
print(f"[Bronze-Audit] Partitions (batch dates):")
display(df_check.groupBy("_batch_date").count().orderBy("_batch_date", ascending=False).limit(30))

print("\n── Top Operations in This Batch ──")
display(
    df_check.filter(col("_batch_date") == start_dt.strftime("%Y-%m-%d"))
    .groupBy("Operation", "Workload")
    .count()
    .orderBy("count", ascending=False)
    .limit(20)
)
