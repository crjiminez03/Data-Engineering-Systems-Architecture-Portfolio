# Fabric Notebook — Bronze Master Pipeline: Architecture & Expression Reference
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Documents the Bronze Master Pipeline design and provides
#              the PySpark equivalent logic for the control-table-driven
#              Full and Incremental refresh pipelines.
#
#              Actual orchestration is handled by Fabric Data Pipelines
#              (Invoke Pipeline activities) — this notebook documents
#              the expression logic, switch/if-condition patterns,
#              and serves as the code reference for the pipeline design.
#
# Pipeline Hierarchy:
#   Master Pipeline
#     └── Invoke: Bronze Master Pipeline
#           ├── Invoke: Bronze_Full_Refresh_Pipeline     (load_type = 'FULL')
#           └── Invoke: Bronze_Incremental_Pipeline      (load_type = 'INCREMENTAL')
#                         Both driven by control_table in Silver Warehouse

# ──────────────────────────────────────────────────────────────
# Cell 1 — Pipeline Expression Reference
#           These are the dynamic expressions used inside the
#           Fabric Pipeline activities (Copy, Switch, If Condition)
# ──────────────────────────────────────────────────────────────

PIPELINE_EXPRESSIONS = """
╔══════════════════════════════════════════════════════════════════════╗
║         BRONZE MASTER PIPELINE — EXPRESSION REFERENCE               ║
╚══════════════════════════════════════════════════════════════════════╝

These expressions are used inside Fabric Pipeline ForEach / Switch /
If Condition / Copy Data activities. The pipeline reads control_table
rows and passes each row as an item to a ForEach loop.

── ForEach Item Reference ─────────────────────────────────────────────
  @item().source_table
  @item().destination_table
  @item().source_schema
  @item().source_watermark_column
  @item().source_cutoff_date
  @item().load_type
  @item().connection_name
  @item().source_database
  @item().server_switch

── If Condition: Route to Full vs Incremental Sub-Pipeline ───────────
  Expression:
    @equals(item().load_type, 'FULL')

  True  → Invoke Activity: Bronze_Full_Refresh_Pipeline
  False → Invoke Activity: Bronze_Incremental_Pipeline
  (Parameters passed: all @item() fields above)

── Switch Activity: Server/Connection Switch ──────────────────────────
  Switch on: @item().server_switch

  Case 'SERVER1':
    → Use Linked Service: OnPrem_SQL_Server1
    → Database expression: @item().source_database

  Case 'SERVER2':
    → Use Linked Service: OnPrem_SQL_Server2
    → Database expression: @item().source_database

  Case 'GRAPH_API':
    → Use Linked Service: O365_Management_API
    → No database expression needed

  Default:
    → Fail activity with message: 'Unknown server_switch value'

── Copy Data Activity: Source Query (Incremental) ─────────────────────
  Source SQL Query (dynamic expression):
    @concat(
      'SELECT * FROM ',
      item().source_schema, '.', item().source_table,
      ' WHERE ', item().source_watermark_column,
      ' > ''', item().source_cutoff_date, ''''
    )

── Copy Data Activity: Source Query (Full Refresh) ────────────────────
  Source SQL Query (dynamic expression):
    @concat(
      'SELECT * FROM ',
      item().source_schema, '.', item().source_table
    )

── Copy Data Activity: Sink (Bronze Lakehouse) ────────────────────────
  Sink Table Name (dynamic expression):
    @item().destination_table

  Write Behavior:
    Full:        Overwrite entire table
    Incremental: Append

── On Success: Update control_table watermark ─────────────────────────
  Stored Proc / Script Activity runs after successful Copy:
    UPDATE control_table
    SET source_cutoff_date = GETUTCDATE(),
        last_run_status    = 'SUCCESS',
        last_run_end       = GETUTCDATE(),
        last_rows_read     = @activity('CopyData').output.rowsRead,
        last_rows_written  = @activity('CopyData').output.rowsCopied,
        last_updated_at    = GETUTCDATE()
    WHERE control_id = @item().control_id

── On Failure: Log failure to control_table ───────────────────────────
    UPDATE control_table
    SET last_run_status = 'FAILURE',
        last_run_end    = GETUTCDATE(),
        last_updated_at = GETUTCDATE()
    WHERE control_id = @item().control_id
"""

print(PIPELINE_EXPRESSIONS)


# ──────────────────────────────────────────────────────────────
# Cell 2 — PySpark Equivalent: Bronze Full Refresh Pipeline
#           Mirrors the Fabric Pipeline logic in code for reference
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timezone
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()

# Server → JDBC URL mapping (mirrors pipeline Linked Service switch)
SERVER_JDBC_MAP = {
    "SERVER1": {
        "url":    "jdbc:sqlserver://your-server1.domain.com:1433",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    },
    "SERVER2": {
        "url":    "jdbc:sqlserver://your-server2.domain.com:1433",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    },
}

def get_jdbc_options(server_switch: str, source_database: str) -> dict:
    """Mirrors the Switch activity in the Fabric Pipeline."""
    if server_switch not in SERVER_JDBC_MAP:
        raise ValueError(f"Unknown server_switch: '{server_switch}'. "
                         f"Valid values: {list(SERVER_JDBC_MAP.keys())}")
    base = SERVER_JDBC_MAP[server_switch]
    return {
        "url":      f"{base['url']};databaseName={source_database}",
        "driver":   base["driver"],
        # Auth via Fabric secret / managed identity token
        "accessToken": mssparkutils.credentials.getToken(base["url"].split(":")[0]),
    }


def bronze_full_refresh(control_row: dict) -> dict:
    """
    Full refresh: truncate + reload destination table from source.
    Mirrors Bronze_Full_Refresh_Pipeline logic.
    """
    src_schema   = control_row["source_schema"]
    src_table    = control_row["source_table"]
    dst_table    = control_row["destination_table"]
    server_sw    = control_row["server_switch"]
    src_db       = control_row["source_database"]

    jdbc_opts = get_jdbc_options(server_sw, src_db)
    query     = f"(SELECT * FROM {src_schema}.{src_table}) AS t"

    df_src = (
        spark.read.format("jdbc")
        .option("url",        jdbc_opts["url"])
        .option("dbtable",    query)
        .option("driver",     jdbc_opts["driver"])
        .option("accessToken",jdbc_opts["accessToken"])
        .load()
    )

    rows_read = df_src.count()

    df_src = (
        df_src
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_server",       lit(server_sw))
        .withColumn("_source_database",     lit(src_db))
        .withColumn("_load_type",           lit("FULL"))
    )

    df_src.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(dst_table)

    rows_written = spark.read.format("delta").table(dst_table).count()

    return {
        "rows_read":    rows_read,
        "rows_written": rows_written,
        "status":       "SUCCESS",
    }


def bronze_incremental(control_row: dict) -> dict:
    """
    Incremental refresh: append rows newer than source_cutoff_date.
    Mirrors Bronze_Incremental_Pipeline logic.
    """
    src_schema     = control_row["source_schema"]
    src_table      = control_row["source_table"]
    dst_table      = control_row["destination_table"]
    server_sw      = control_row["server_switch"]
    src_db         = control_row["source_database"]
    watermark_col  = control_row["source_watermark_column"]
    cutoff_date    = control_row["source_cutoff_date"]

    jdbc_opts = get_jdbc_options(server_sw, src_db)

    # Incremental predicate — mirrors Copy Data source query expression
    query = f"""(
        SELECT * FROM {src_schema}.{src_table}
        WHERE {watermark_col} > '{cutoff_date}'
    ) AS t"""

    df_src = (
        spark.read.format("jdbc")
        .option("url",        jdbc_opts["url"])
        .option("dbtable",    query)
        .option("driver",     jdbc_opts["driver"])
        .option("accessToken",jdbc_opts["accessToken"])
        .load()
    )

    rows_read = df_src.count()

    if rows_read == 0:
        print(f"  [Bronze Incr] No new rows for {dst_table} since {cutoff_date}")
        return {"rows_read": 0, "rows_written": 0, "status": "SUCCESS_NO_NEW_DATA"}

    df_src = (
        df_src
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_server",       lit(server_sw))
        .withColumn("_source_database",     lit(src_db))
        .withColumn("_load_type",           lit("INCREMENTAL"))
    )

    df_src.write.format("delta").mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(dst_table)

    return {
        "rows_read":    rows_read,
        "rows_written": rows_read,
        "status":       "SUCCESS",
    }


# ──────────────────────────────────────────────────────────────
# Cell 3 — Bronze Master Orchestrator
#           Reads control_table, routes Full vs Incremental,
#           handles server switch, updates watermark on success
# ──────────────────────────────────────────────────────────────
def run_bronze_master():
    print(f"\n[Bronze Master] Starting at {datetime.now(timezone.utc).isoformat()}")

    df_control = (
        spark.read.format("delta")
        .table("silver_warehouse.dbo.control_table")
        .filter(col("is_active") == True)
        .orderBy("control_id")
    )
    control_rows = df_control.collect()
    print(f"[Bronze Master] Active control entries: {len(control_rows)}")

    results = []

    for row in control_rows:
        entry       = row.asDict()
        dst_table   = entry["destination_table"]
        load_type   = entry["load_type"]
        control_id  = entry["control_id"]

        print(f"\n  [{load_type}] {entry['source_schema']}.{entry['source_table']}"
              f" ({entry['server_switch']}/{entry['source_database']})"
              f" → {dst_table}")

        run_start = datetime.now(timezone.utc)
        result    = {"rows_read": 0, "rows_written": 0, "status": "FAILURE"}

        try:
            # ── If Condition: route based on load_type ────────────
            if load_type == "FULL":
                result = bronze_full_refresh(entry)
            else:
                result = bronze_incremental(entry)

            run_end = datetime.now(timezone.utc)
            print(f"    ✔ {result['status']} | "
                  f"read={result['rows_read']:,} | "
                  f"written={result['rows_written']:,} | "
                  f"duration={(run_end - run_start).seconds}s")

            # ── Update control_table watermark on success ─────────
            if result["status"].startswith("SUCCESS"):
                dt = DeltaTable.forName(spark, "silver_warehouse.dbo.control_table")
                dt.update(
                    condition=f"control_id = {control_id}",
                    set={
                        "source_cutoff_date": f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
                        "last_run_start":     f"'{run_start.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
                        "last_run_end":       f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
                        "last_run_status":    "'SUCCESS'",
                        "last_rows_read":     str(result["rows_read"]),
                        "last_rows_written":  str(result["rows_written"]),
                        "last_updated_at":    f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
                        "last_updated_by":    "'BronzeMasterPipeline'",
                    }
                )

        except Exception as ex:
            run_end = datetime.now(timezone.utc)
            result["status"] = "FAILURE"
            print(f"    ✖ FAILURE: {ex}")
            dt = DeltaTable.forName(spark, "silver_warehouse.dbo.control_table")
            dt.update(
                condition=f"control_id = {control_id}",
                set={
                    "last_run_status":  "'FAILURE'",
                    "last_run_end":     f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
                    "last_updated_at":  f"'{run_end.strftime('%Y-%m-%dT%H:%M:%SZ')}'",
                }
            )

        results.append({**entry, **result,
                        "run_start": run_start, "run_end": run_end})

    # ── Summary ────────────────────────────────────────────────
    print("\n═══════════════════════════════════════════════════════════")
    print("              BRONZE MASTER RUN SUMMARY                   ")
    print("═══════════════════════════════════════════════════════════")
    for r in results:
        icon = "✔" if r["status"].startswith("SUCCESS") else "✖"
        print(f"  {icon} [{r['load_type']:11s}] [{r['status']:20s}] "
              f"{r['destination_table']:45s} "
              f"R:{r['rows_read']:>8,}  W:{r['rows_written']:>8,}")
    print("═══════════════════════════════════════════════════════════")

    return results

# Uncomment to run:
# run_bronze_master()
print("[Bronze Master] Pipeline reference notebook loaded.")
print("Call run_bronze_master() to execute, or use the Fabric Pipeline for scheduled runs.")
