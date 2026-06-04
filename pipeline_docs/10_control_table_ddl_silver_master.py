# Fabric Notebook — Control Table DDL + Silver Master Orchestrator
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Part 1 — DDL to create the control_table in SilverWarehouse
#              that drives all pipeline refresh decisions (incremental vs full,
#              server/DB switching, source/destination routing).
#              Part 2 — Silver Master Notebook that reads the control_table
#              and calls each silver DML notebook in sequence, then triggers
#              Audit_sp to log successful ingestion details.

# ══════════════════════════════════════════════════════════════
# PART 1 — CONTROL TABLE DDL
# ══════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────
# Cell 1 — Create control_table in Silver Warehouse
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, to_date
from datetime import datetime, timezone

spark = SparkSession.builder.getOrCreate()

# ── DDL ───────────────────────────────────────────────────────
spark.sql("""
CREATE TABLE IF NOT EXISTS silver_warehouse.dbo.control_table (
    -- Identity
    control_id              INT             NOT NULL   COMMENT 'Auto-increment PK',
    pipeline_name           STRING          NOT NULL   COMMENT 'Master pipeline or notebook name',

    -- Source routing
    connection_name         STRING          NOT NULL   COMMENT 'Named connection: SSMS_Server1, SSMS_Server2 etc.',
    source_schema           STRING          NOT NULL   COMMENT 'Source schema e.g. dbo',
    source_table            STRING          NOT NULL   COMMENT 'Source table name on-prem',
    source_watermark_column STRING                     COMMENT 'Column used for incremental watermark e.g. ModifiedDate',

    -- Destination routing
    destination_schema      STRING          NOT NULL   COMMENT 'Destination schema in Fabric Lakehouse',
    destination_table       STRING          NOT NULL   COMMENT 'Destination Delta table name',

    -- Load control
    load_type               STRING          NOT NULL   COMMENT 'INCREMENTAL or FULL',
    source_cutoff_date      TIMESTAMP                  COMMENT 'Last successfully loaded watermark value',
    is_active               BOOLEAN         NOT NULL   COMMENT 'Whether this entry is active in pipeline runs',

    -- Server/DB switch config
    server_switch           STRING                     COMMENT 'Server switch key used in If-condition expressions',
    source_database         STRING                     COMMENT 'Source database name on the connection server',

    -- Audit tracking
    last_run_start          TIMESTAMP                  COMMENT 'Start time of last pipeline run for this table',
    last_run_end            TIMESTAMP                  COMMENT 'End time of last pipeline run for this table',
    last_run_status         STRING                     COMMENT 'SUCCESS / FAILURE / RUNNING',
    last_rows_read          LONG                       COMMENT 'Rows read from source in last run',
    last_rows_written       LONG                       COMMENT 'Rows written to destination in last run',
    last_updated_by         STRING                     COMMENT 'Pipeline or notebook that last updated this row',
    last_updated_at         TIMESTAMP                  COMMENT 'Timestamp of last metadata update'
)
USING DELTA
COMMENT 'Master control table driving all medallion layer pipeline refresh decisions'
""")
print("[Control Table] DDL applied: control_table")


# ──────────────────────────────────────────────────────────────
# Cell 2 — Seed Sample Control Table Rows
# ──────────────────────────────────────────────────────────────
sample_rows = [
    # (control_id, pipeline_name, connection_name, source_schema, source_table,
    #  source_watermark_column, destination_schema, destination_table,
    #  load_type, source_cutoff_date, is_active, server_switch, source_database,
    #  last_run_start, last_run_end, last_run_status,
    #  last_rows_read, last_rows_written, last_updated_by, last_updated_at)
    (1, "MasterPipeline", "SSMS_Server1", "dbo", "Customers",
     "ModifiedDate", "bronze", "bronze_customers",
     "INCREMENTAL", "2024-01-01 00:00:00", True, "SERVER1", "OperationsDB",
     None, None, None, None, None, "SEED", "2024-01-01 00:00:00"),

    (2, "MasterPipeline", "SSMS_Server1", "dbo", "Orders",
     "OrderDate", "bronze", "bronze_orders",
     "INCREMENTAL", "2024-01-01 00:00:00", True, "SERVER1", "OperationsDB",
     None, None, None, None, None, "SEED", "2024-01-01 00:00:00"),

    (3, "MasterPipeline", "SSMS_Server2", "dbo", "Products",
     None, "bronze", "bronze_products",
     "FULL", None, True, "SERVER2", "CatalogDB",
     None, None, None, None, None, "SEED", "2024-01-01 00:00:00"),

    (4, "MasterPipeline", "SSMS_Server2", "dbo", "Inventory",
     "LastUpdated", "bronze", "bronze_inventory",
     "INCREMENTAL", "2024-01-01 00:00:00", True, "SERVER2", "CatalogDB",
     None, None, None, None, None, "SEED", "2024-01-01 00:00:00"),

    (5, "AuditLogPipeline", "O365_Graph_API", "api", "AuditLogs_PowerBI",
     "CreationTime", "bronze", "bronze_audit_logs_powerbi",
     "INCREMENTAL", "2024-01-01 00:00:00", True, "GRAPH_API", "O365",
     None, None, None, None, None, "SEED", "2024-01-01 00:00:00"),
]

columns = [
    "control_id", "pipeline_name", "connection_name", "source_schema", "source_table",
    "source_watermark_column", "destination_schema", "destination_table",
    "load_type", "source_cutoff_date", "is_active", "server_switch", "source_database",
    "last_run_start", "last_run_end", "last_run_status",
    "last_rows_read", "last_rows_written", "last_updated_by", "last_updated_at"
]

df_seed = spark.createDataFrame(sample_rows, schema=columns)
df_seed.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("silver_warehouse.dbo.control_table")

print("[Control Table] Seed rows inserted.")
display(spark.read.format("delta").table("silver_warehouse.dbo.control_table"))


# ══════════════════════════════════════════════════════════════
# PART 2 — SILVER MASTER NOTEBOOK
# ══════════════════════════════════════════════════════════════

# ──────────────────────────────────────────────────────────────
# Cell 3 — Read Active Control Table Entries
# ──────────────────────────────────────────────────────────────
df_control = (
    spark.read.format("delta")
    .table("silver_warehouse.dbo.control_table")
    .filter(col("is_active") == True)
    .orderBy("control_id")
)

control_entries = df_control.collect()
print(f"[Silver Master] Active control entries: {len(control_entries)}")


# ──────────────────────────────────────────────────────────────
# Cell 4 — Silver DML Notebook Registry
# ──────────────────────────────────────────────────────────────
# Maps destination_table → notebook path in the Fabric workspace
SILVER_NOTEBOOK_REGISTRY = {
    "silver_semantic_model_logs_parsed": "/Silver/02_silver_semantic_model_logs_parsed",
    "silver_audit_logs_powerbi":         "/Silver/08_silver_audit_logs_ddl_dml",
    "silver_customers":                  "/Silver/DML/silver_customers_dml",
    "silver_orders":                     "/Silver/DML/silver_orders_dml",
    "silver_products":                   "/Silver/DML/silver_products_dml",
    "silver_inventory":                  "/Silver/DML/silver_inventory_dml",
    # Add more as tables are onboarded
}

print("[Silver Master] Notebook registry loaded:")
for k, v in SILVER_NOTEBOOK_REGISTRY.items():
    print(f"  {k:45s} → {v}")


# ──────────────────────────────────────────────────────────────
# Cell 5 — Execute Each Silver Notebook & Update Control Table
# ──────────────────────────────────────────────────────────────
from delta.tables import DeltaTable

run_results = []

for entry in control_entries:
    dest_table   = entry["destination_table"]
    load_type    = entry["load_type"]
    watermark    = entry["source_watermark_column"]
    cutoff       = entry["source_cutoff_date"]
    server       = entry["server_switch"]
    control_id   = entry["control_id"]

    notebook_path = SILVER_NOTEBOOK_REGISTRY.get(dest_table)
    if not notebook_path:
        print(f"  [SKIP] No notebook registered for: {dest_table}")
        continue

    print(f"\n[Silver Master] Running: {dest_table} ({load_type}) via {notebook_path}")
    run_start = datetime.now(timezone.utc)
    status    = "FAILURE"
    rows_read = 0
    rows_written = 0

    try:
        # Pass parameters to child notebook
        result = mssparkutils.notebook.run(
            notebook_path,
            timeout=1800,   # 30 min timeout per notebook
            arguments={
                "load_type":           load_type,
                "source_cutoff_date":  str(cutoff) if cutoff else "1900-01-01",
                "server_switch":       server or "",
                "destination_table":   dest_table,
            }
        )
        run_end   = datetime.now(timezone.utc)
        status    = "SUCCESS"
        # Child notebooks should return JSON: {"rows_read": N, "rows_written": M}
        import json as _json
        try:
            result_json = _json.loads(result)
            rows_read    = result_json.get("rows_read", 0)
            rows_written = result_json.get("rows_written", 0)
        except Exception:
            pass

        print(f"  ✔ SUCCESS | rows_read={rows_read:,} | rows_written={rows_written:,} "
              f"| duration={(run_end - run_start).seconds}s")

    except Exception as ex:
        run_end = datetime.now(timezone.utc)
        print(f"  ✖ FAILURE: {ex}")

    run_results.append({
        "control_id":     control_id,
        "dest_table":     dest_table,
        "status":         status,
        "run_start":      run_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_end":        run_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows_read":      rows_read,
        "rows_written":   rows_written,
    })

    # ── Update control_table with run stats ───────────────────
    ctrl_dt = DeltaTable.forName(spark, "silver_warehouse.dbo.control_table")
    ctrl_dt.update(
        condition=f"control_id = {control_id}",
        set={
            "last_run_start":   f"'{run_start.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
            "last_run_end":     f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
            "last_run_status":  f"'{status}'",
            "last_rows_read":   str(rows_read),
            "last_rows_written":str(rows_written),
            "last_updated_by":  "'SilverMasterNotebook'",
            "last_updated_at":  f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
            # Only advance watermark on success
            "source_cutoff_date": f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'"
                                   if status == "SUCCESS" else "source_cutoff_date",
        }
    )


# ──────────────────────────────────────────────────────────────
# Cell 6 — Trigger Audit_sp for Successful Runs
# ──────────────────────────────────────────────────────────────
# Audit stored procedure (Audit_sp) logs full run details to audit_log table
# Only called on SUCCESS to record confirmed ingestion events

def call_audit_sp(entry: dict):
    """
    Writes one row to the pipeline_audit_log table.
    In production this would call a SQL stored procedure via JDBC.
    Here we write directly to a Delta audit table.
    """
    audit_row = [(
        entry["dest_table"],
        entry["control_id"],
        "SilverMasterNotebook",
        entry["status"],
        entry["run_start"],
        entry["run_end"],
        entry["rows_read"],
        entry["rows_written"],
        (datetime.fromisoformat(entry["run_end"]) -
         datetime.fromisoformat(entry["run_start"])).seconds,
    )]
    audit_cols = [
        "table_name", "control_id", "pipeline_name", "run_status",
        "run_start", "run_end", "rows_read", "rows_written", "duration_seconds"
    ]
    df_audit = spark.createDataFrame(audit_row, schema=audit_cols)
    df_audit = df_audit.withColumn("logged_at", current_timestamp())
    df_audit.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("pipeline_audit_log")

for r in run_results:
    if r["status"] == "SUCCESS":
        call_audit_sp(r)
        print(f"  [Audit_sp] Logged: {r['dest_table']}")

print("\n[Silver Master] All notebooks executed.")


# ──────────────────────────────────────────────────────────────
# Cell 7 — Final Run Summary
# ──────────────────────────────────────────────────────────────
print("\n═══════════════════════════════════════════════════════════")
print("                 SILVER MASTER RUN SUMMARY                ")
print("═══════════════════════════════════════════════════════════")
for r in run_results:
    icon = "✔" if r["status"] == "SUCCESS" else "✖"
    print(f"  {icon} [{r['status']:7s}] {r['dest_table']:45s} "
          f"R:{r['rows_read']:>8,}  W:{r['rows_written']:>8,}")
print("═══════════════════════════════════════════════════════════")

success_cnt = sum(1 for r in run_results if r["status"] == "SUCCESS")
failure_cnt = len(run_results) - success_cnt
print(f"\n  Total: {len(run_results)} | SUCCESS: {success_cnt} | FAILURE: {failure_cnt}")

if failure_cnt > 0:
    mssparkutils.notebook.exit("PARTIAL_FAILURE")
else:
    mssparkutils.notebook.exit("SUCCESS")
