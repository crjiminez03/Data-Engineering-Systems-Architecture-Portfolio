# Fabric Notebook — Master Pipeline Architecture Reference
# Portfolio Reference: Fabric Workspace Observability Project
# Description: Documents the complete Master Pipeline hierarchy,
#              schedule design, inter-pipeline dependencies, and
#              the timing buffer strategy between layer refreshes.
#              Includes the full pipeline expression cheat-sheet.

# ──────────────────────────────────────────────────────────────
# Cell 1 — Full Pipeline Hierarchy
# ──────────────────────────────────────────────────────────────

ARCHITECTURE = """
╔══════════════════════════════════════════════════════════════════════════════╗
║              MASTER PIPELINE ARCHITECTURE — FULL HIERARCHY                  ║
╚══════════════════════════════════════════════════════════════════════════════╝

SCHEDULE 1: Bronze + Silver Master  (runs daily, e.g. 6:00 AM)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ┌─────────────────────────────────────────────────────────┐
  │  Master_Pipeline                                         │
  │  (trigger: scheduled 6:00 AM UTC)                        │
  └──────────────┬──────────────────────────────────────────┘
                 │ Invoke Pipeline Activity
                 ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Bronze_Master_Pipeline                                  │
  │  (reads control_table → routes Full vs Incremental)      │
  └──────┬────────────────────────────┬─────────────────────┘
         │ ForEach row in             │ ForEach row in
         │ control_table              │ control_table
         │ WHERE load_type = 'FULL'  │ WHERE load_type = 'INCREMENTAL'
         ▼                            ▼
  ┌──────────────────┐   ┌─────────────────────────────────┐
  │ Bronze_Full_      │   │  Bronze_Incremental_Pipeline     │
  │ Refresh_Pipeline  │   │                                  │
  │                   │   │  ForEach(control_table rows):    │
  │ ForEach:          │   │  ┌──────────────────────────┐   │
  │ ┌─────────────┐  │   │  │ Switch(server_switch)     │   │
  │ │Switch:      │  │   │  │  Case SERVER1:            │   │
  │ │ SERVER1     │  │   │  │   Copy → BronzeLakehouse  │   │
  │ │  →Copy Full │  │   │  │  Case SERVER2:            │   │
  │ │ SERVER2     │  │   │  │   Copy → BronzeLakehouse  │   │
  │ │  →Copy Full │  │   │  │  Case GRAPH_API:          │   │
  │ └─────────────┘  │   │  │   Invoke AuditLog NB      │   │
  │ On Success:       │   │  └──────────────────────────┘   │
  │  Update ctrl tbl  │   │  On Success:                    │
  │ On Failure:       │   │   Update ctrl tbl watermark     │
  │  Log failure      │   │  On Failure:                    │
  └──────────────────┘   │   Log failure                   │
                          └─────────────────────────────────┘
         │
         │ On Success (both bronze pipelines complete)
         ▼
  ┌─────────────────────────────────────────────────────────┐
  │  Silver_Master_Notebook                                  │
  │  (Invoke Notebook Activity)                              │
  │                                                          │
  │  Reads control_table (is_active=true)                    │
  │  For each silver table:                                  │
  │    mssparkutils.notebook.run(silver_dml_notebook)        │
  │    → Update control_table last_run_* columns             │
  │    → Call audit_sp() on SUCCESS                          │
  └─────────────────────────────────────────────────────────┘


SCHEDULE 2: Gold + Semantic Model Master  (runs 1 hr after Schedule 1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  ┌─────────────────────────────────────────────────────────┐
  │  Gold_SemanticModel_Master_Pipeline                      │
  │  (trigger: scheduled 7:00 AM UTC — 1hr buffer after     │
  │   Bronze/Silver to allow Delta snapshots to stabilise)   │
  └──────────────────────────────────────────────────────────┘
         │
         ▼ Step 1: Lock Schema
  ┌─────────────────────────────────────────────────────────┐
  │  Invoke Notebook: Gold_DDL_Notebook                      │
  │  (18_gold_ddl_precision_tables.py)                       │
  │  CREATE TABLE IF NOT EXISTS for all gold + staging tbls  │
  │  Ensures DECIMAL precision is locked before Dataflow runs│
  └──────────────────────────────────────────────────────────┘
         │ On Success
         ▼ Step 2: Populate Staging Tables
  ┌─────────────────────────────────────────────────────────┐
  │  Dataflow Gen2 Activities (run in parallel)              │
  │                                                          │
  │  ┌─────────────────────┐  ┌────────────────────────┐   │
  │  │ DF_RefreshSummary   │  │ DF_DAXPerformance       │   │
  │  │ M Query transforms  │  │ M Query transforms      │   │
  │  │ Replace method →    │  │ Replace method →        │   │
  │  │ _staging table      │  │ _staging table          │   │
  │  └─────────────────────┘  └────────────────────────┘   │
  │  ┌─────────────────────┐  ┌────────────────────────┐   │
  │  │ DF_AuditActivity    │  │ DF_ModelHealthScorecard │   │
  │  │ M Query transforms  │  │ M Query transforms      │   │
  │  │ Replace method →    │  │ Replace method →        │   │
  │  │ _staging table      │  │ _staging table          │   │
  │  └─────────────────────┘  └────────────────────────┘   │
  └──────────────────────────────────────────────────────────┘
         │ On Success (all Dataflows complete)
         ▼ Step 3: Merge Staging → Gold
  ┌─────────────────────────────────────────────────────────┐
  │  Invoke Notebook: Gold_DML_Notebook                      │
  │  (19_gold_dml_merge_staging_to_gold.py)                  │
  │  MERGE _staging → gold for all tables                    │
  │  Preserves _dw_created_at, updates on hash change only   │
  │  Post-merge row count validation                         │
  └──────────────────────────────────────────────────────────┘
         │ On Success
         ▼ Step 4: Refresh Semantic Model
  ┌─────────────────────────────────────────────────────────┐
  │  Semantic Model Refresh Activities                       │
  │  (Fabric native refresh or REST API trigger)             │
  │  Refreshes the .pbix-backed semantic model in workspace  │
  └──────────────────────────────────────────────────────────┘
         │ On Success
         ▼ Step 5: Audit Log
  ┌─────────────────────────────────────────────────────────┐
  │  Script Activity / Notebook: audit_sp()                  │
  │  Logs full pipeline run to pipeline_audit_log            │
  └──────────────────────────────────────────────────────────┘


KEY DESIGN DECISIONS
━━━━━━━━━━━━━━━━━━━━
  1. WHY 1HR BUFFER BETWEEN SCHEDULES?
     Dataflow Gen2 reads Silver Delta tables using a snapshot at
     query time. If the Gold pipeline runs immediately after Silver
     completes, Delta's internal transaction log may still be
     compacting or the checkpoint hasn't flushed. The 1hr buffer
     ensures the Silver snapshot seen by Dataflow Gen2 is stable
     and complete — no partial reads.

  2. WHY DATAFLOW GEN2 REPLACE → STAGING (NOT DIRECT TO GOLD)?
     Dataflow Gen2's Replace method drops and recreates the target
     table on every run. Writing directly to gold would lose the
     precision DDL (DECIMAL columns become DOUBLE after recreation).
     The staging → MERGE pattern isolates Dataflow from the gold
     table schema, letting DDL lock precision independently.

  3. WHY CI/CD VIA DATAFLOW GEN2 M QUERIES?
     Fabric Dataflow Gen2 items can be exported as JSON definitions
     and committed to Git. The M query transformations version-control
     cleanly, enabling PR reviews of transformation logic changes.
     This is not possible with notebook-based gold transforms
     at the same granularity.

  4. WHY SWITCH ACTIVITY FOR SERVER ROUTING?
     On-prem data lives across multiple SQL Servers and databases.
     A Switch activity on server_switch (from control_table) avoids
     hardcoding connection strings per pipeline. Adding a new server
     means adding one Case to the Switch and one row in control_table
     — no pipeline duplication.

  5. WHY SEPARATE GOLD + SEMANTIC MODEL PIPELINE?
     Semantic model refresh is expensive and only valid once Gold
     tables are confirmed correct. Keeping it in a separate scheduled
     pipeline means: (a) it can be retried independently without
     re-running Bronze/Silver, (b) it can be paused during
     Gold schema migrations without breaking the Bronze/Silver cadence.
"""

print(ARCHITECTURE)


# ──────────────────────────────────────────────────────────────
# Cell 2 — Pipeline Expression Cheat-Sheet
#           All dynamic expressions used across the hierarchy
# ──────────────────────────────────────────────────────────────

EXPRESSION_CHEATSHEET = """
╔══════════════════════════════════════════════════════════════════════════════╗
║              FABRIC PIPELINE EXPRESSION CHEAT-SHEET                         ║
╚══════════════════════════════════════════════════════════════════════════════╝

── Lookup Activity: Read control_table ───────────────────────────────────────
  Activity Type: Lookup
  Dataset:       Silver Warehouse → control_table
  Query:
    SELECT * FROM control_table
    WHERE is_active = 1
    ORDER BY control_id

── ForEach Activity: Iterate control_table rows ──────────────────────────────
  Items:         @activity('Lookup_ControlTable').output.value
  Batch Count:   1  (sequential; set >1 for parallel if servers can handle it)

── If Condition: Full vs Incremental routing ─────────────────────────────────
  Expression:    @equals(item().load_type, 'FULL')
  True Branch:   Invoke Bronze_Full_Refresh_Pipeline
  False Branch:  Invoke Bronze_Incremental_Pipeline

── Switch: Server routing ────────────────────────────────────────────────────
  On:            @item().server_switch
  Cases:         SERVER1, SERVER2, GRAPH_API
  Default:       Fail activity

── Copy Data: Incremental source query ───────────────────────────────────────
  @concat(
    'SELECT * FROM ',
    item().source_schema, '.', item().source_table,
    ' WHERE ', item().source_watermark_column,
    ' > ''', formatDateTime(item().source_cutoff_date, 'yyyy-MM-dd HH:mm:ss'), ''''
  )

── Copy Data: Full refresh source query ──────────────────────────────────────
  @concat(
    'SELECT * FROM ',
    item().source_schema, '.', item().source_table
  )

── Copy Data: Sink table name (dynamic) ──────────────────────────────────────
  @item().destination_table

── Copy Data: Row count capture ──────────────────────────────────────────────
  @activity('CopyData_Incremental').output.rowsRead
  @activity('CopyData_Incremental').output.rowsCopied

── Set Variable: Capture run start time ──────────────────────────────────────
  @utcNow()

── Set Variable: Build watermark string ──────────────────────────────────────
  @formatDateTime(utcNow(), 'yyyy-MM-ddTHH:mm:ssZ')

── Script Activity: Update control_table on success ──────────────────────────
  UPDATE dbo.control_table
  SET   source_cutoff_date = '@{formatDateTime(utcNow(),''yyyy-MM-ddTHH:mm:ssZ'')}',
        last_run_status    = 'SUCCESS',
        last_run_start     = '@{variables(''PipelineStartTime'')}',
        last_run_end       = '@{formatDateTime(utcNow(),''yyyy-MM-ddTHH:mm:ssZ'')}',
        last_rows_read     = @{activity('CopyData').output.rowsRead},
        last_rows_written  = @{activity('CopyData').output.rowsCopied},
        last_updated_by    = 'BronzeMasterPipeline',
        last_updated_at    = '@{formatDateTime(utcNow(),''yyyy-MM-ddTHH:mm:ssZ'')}'
  WHERE control_id = @{item().control_id}

── Script Activity: Update control_table on failure ──────────────────────────
  UPDATE dbo.control_table
  SET   last_run_status = 'FAILURE',
        last_run_end    = '@{formatDateTime(utcNow(),''yyyy-MM-ddTHH:mm:ssZ'')}',
        last_updated_at = '@{formatDateTime(utcNow(),''yyyy-MM-ddTHH:mm:ssZ'')}'
  WHERE control_id = @{item().control_id}

── Invoke Pipeline: Pass parameters to sub-pipeline ──────────────────────────
  Parameters passed from ForEach to sub-pipeline:
    p_control_id          : @item().control_id
    p_source_table        : @item().source_table
    p_source_schema       : @item().source_schema
    p_destination_table   : @item().destination_table
    p_source_watermark_col: @item().source_watermark_column
    p_source_cutoff_date  : @item().source_cutoff_date
    p_server_switch       : @item().server_switch
    p_source_database     : @item().source_database
    p_load_type           : @item().load_type
    p_connection_name     : @item().connection_name
"""

print(EXPRESSION_CHEATSHEET)


# ──────────────────────────────────────────────────────────────
# Cell 3 — Refresh Schedule Summary Table
# ──────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

schedule_data = [
    ("1", "Master_Pipeline (Bronze + Silver)",
     "6:00 AM UTC", "Daily",
     "Bronze_Full_Refresh + Bronze_Incremental + Silver_Master_Notebook",
     "SilverWarehouse.control_table"),

    ("2", "Gold_SemanticModel_Master_Pipeline",
     "7:00 AM UTC", "Daily",
     "Gold_DDL → Dataflow_Gen2_Items → Gold_DML_Merge → SemanticModel_Refresh",
     "1hr buffer after Schedule 1 to ensure Silver Delta snapshots are stable"),

    ("3", "AuditLog_Bronze_Pipeline",
     "2:00 AM UTC", "Daily",
     "Graph API ingestion of prior day O365 audit logs (RecordType=21)",
     "Runs early morning before main pipeline to stage audit data"),
]

df_sched = spark.createDataFrame(
    schedule_data,
    ["schedule_id","pipeline_name","trigger_time","frequency","activities","notes"]
)

print("\n── Pipeline Refresh Schedule ──")
display(df_sched)
