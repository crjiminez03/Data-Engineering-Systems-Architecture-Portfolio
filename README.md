# Microsoft Fabric Workspace Observability — Portfolio Project

> **Author:** Christopher J.  
> **Role:** Data Engineer, IT Business Systems Analyst
>
> **Platform:** Microsoft Fabric (Lakehouse, Eventhouse, Dataflow Gen2, Pipelines)  
> **Stack:** PySpark, KQL, Power Query M, Delta Lake, Office 365 Management API

---

## Project Overview

This project implements a full **Medallion Architecture (Bronze → Silver → Gold)** observability platform built inside Microsoft Fabric. It monitors semantic model health, workspace activity, and Power BI audit logs at scale — solving real operational problems including:

- **Silent load failures** where pipelines report SUCCESS but data is missing
- **5,000-row API limit** on audit log exports via PowerShell (solved with blob pagination)
- **Incremental vs full load orchestration** driven by a control table with server-switching logic
- **Deep column profiling** to proactively identify data quality issues before they reach reports

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SOURCE SYSTEMS                                    │
│   Eventhouse KQL  │  O365 Management API  │  On-Prem SSMS (via GW) │
└──────────┬────────┴──────────┬────────────┴──────────┬─────────────┘
           │                   │                        │
           ▼                   ▼                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                             │
│  bronze_semantic_model_logs  │  bronze_audit_logs_powerbi           │
│  Append-only Delta tables, partitioned by batch date                │
│  Audit columns: _ingestion_timestamp, _source, _batch_date         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Silver Master Notebook
                               ▼  (calls DML notebooks via mssparkutils)
┌─────────────────────────────────────────────────────────────────────┐
│                      SILVER LAYER (Cleansed)                        │
│  silver_semantic_model_logs_parsed  │  silver_audit_logs_powerbi    │
│  MERGE upsert on OperationId / Id                                   │
│  JSON parsing: Identity, ApplicationContext, EventText              │
│  Type casting, deduplication, row hash for change detection        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Dataflow Gen2 (M queries)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       GOLD LAYER (Analytics)                        │
│  gold_semantic_model_refresh_summary  (daily refresh health)        │
│  gold_semantic_model_dax_performance  (slow query analysis)         │
│  gold_semantic_model_user_activity    (user behaviour)              │
│  gold_audit_user_activity_summary     (audit event aggregates)      │
│  gold_model_health_scorecard          (A/B/C/D grades per model)    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## File Index

### KQL Queries (`/kql/`)
| File | Description |
|------|-------------|
| `workspace_logs_query.kql` | 8 KQL queries for general workspace activity logs from Eventhouse |
| `semantic_model_logs_query.kql` | 12 KQL queries targeting SemanticModel operations: refresh health, slow DAX, CPU vs IO profiling, watermark checks, real-time feed |

### SemanticModelLogs Medallion (`/semantic_model/`)
| File | Layer | Description |
|------|-------|-------------|
| `01_bronze_semantic_model_logs.py` | Bronze | KQL → Delta ingestion via Kusto Spark connector. Incremental/full modes with watermark. |
| `02_silver_semantic_model_logs_parsed.py` | Silver | DDL + MERGE upsert. Parses Identity, ApplicationContext, EventText JSON. Row hash, dedup. |
| `03_gold_semantic_model_logs_analytics.py` | Gold | 3 gold tables: refresh summary, DAX performance tiers, user activity. SLA flagging. |

### Audit Logs (`/audit_logs/`)
| File | Layer | Description |
|------|-------|-------------|
| `07_bronze_audit_logs_graph_api.py` | Bronze | O365 Management API pagination to bypass 5k-row limit. Full 24hr window. Token auth via KV. |
| `08_silver_audit_logs_ddl_dml.py` | Silver | DDL table creation + MERGE upsert. IsSuccess cast, JSON field extraction, incremental watermark. |

### Metadata & Profiling (`/metadata/`)
| File | Description |
|------|-------------|
| `04_semantic_model_system_tables.py` | Reads DAX Studio CSV exports (Partitions, Tables, Columns, Measures, StorageTables) → metadata_master Delta table |
| `05_metadata_health_checks.py` | Null rate analysis, column usage, measure catalog, row count summary across all 3 layers |
| `06_column_profiling.py` | Full statistical profiling: min/max/avg/stddev/percentiles for numeric; length stats for strings; cardinality; health flags (CRITICAL/WARN/OK) |

### Validation (`/validation/`)
| File | Description |
|------|-------------|
| `09_rowcount_validation_pre_post.py` | Pre vs post ingestion row count comparison. Detects CRITICAL_MISSING, WARN_SHORT_LOAD, WARN_OVER_LOADED. **Silent failure detection**: cross-references SUCCESS status against actual loaded rows. |

### Pipeline Orchestration (`/pipeline_docs/`)
| File | Description |
|------|-------------|
| `10_control_table_ddl_silver_master.py` | control_table DDL with watermark, load type, server switch, and Audit_sp logging. Silver Master Notebook that orchestrates all DML notebooks via mssparkutils and updates control_table on completion. |
| `11_gold_dataflow_gen2_m_queries.pq` | Power Query M examples for Dataflow Gen2: refresh health, audit user activity, model health scorecard with A/B/C/D grading |

---

## Key Engineering Decisions

### Bypassing the 5,000-Row Audit Log Limit
The PowerShell approach using `Search-UnifiedAuditLog` hits a hard 5,000-row cap per call. This solution uses the **Office 365 Management Activity API** directly, which returns paginated content blob URIs via `NextPageUri` response headers. Each blob contains up to 1,000 records, and we iterate all blobs for the full 24hr window — collecting every record regardless of volume.

### Silent Failure Detection
A pipeline reporting SUCCESS does not guarantee all data loaded. The validation notebook cross-references:
1. Operation status from silver logs (`Status = 'SUCCESS'`)
2. Actual row counts: source vs destination per batch date

If success rate ≥ 95% but loaded row % < 95%, the row is flagged as a **silent failure** — a scenario that would otherwise go completely undetected.

### Control Table Server Switching
The master pipeline uses the `server_switch` column in `control_table` combined with If-condition expressions in Fabric Pipelines to dynamically route to different on-prem SQL Server connections (`SSMS_Server1`, `SSMS_Server2`) and databases — avoiding hardcoded connection strings per table.

### Incremental Watermark Strategy
The `source_cutoff_date` column in `control_table` stores the last successfully loaded high-water mark per table. On each run it is read, passed as a notebook parameter, and only advanced after a confirmed SUCCESS. This prevents data gaps from failed runs.

---

## How to Run

1. **Upload** notebooks to your Fabric workspace (Lakehouse → New Notebook → Import)
2. **Configure** KQL cluster URI, Lakehouse IDs, and Key Vault secret names in Cell 1 of each notebook
3. **Seed** the control_table using `10_control_table_ddl_silver_master.py` Cell 2
4. **Schedule** `07_bronze_audit_logs_graph_api.py` at 2:00 AM daily via Master Pipeline
5. **Run order:**
   - Bronze notebooks → Silver Master Notebook → Dataflow Gen2 (Gold) → Validation

---

## Technologies Used

| Technology | Usage |
|------------|-------|
| Microsoft Fabric Lakehouse | Delta Lake storage for all medallion layers |
| Fabric Eventhouse (KQL) | Source for real-time workspace and semantic model logs |
| Kusto Spark Connector | Reads KQL query results directly into PySpark DataFrames |
| PySpark / Delta Lake | All Bronze and Silver transformations |
| Power Query M / Dataflow Gen2 | Gold layer transformations and aggregations |
| Office 365 Management API | Full audit log retrieval with blob pagination |
| Azure Key Vault | Secure storage of tenant ID, client ID, client secret |
| mssparkutils | Notebook chaining, secret retrieval, token acquisition |
| DAX Studio | System table extraction from .pbix semantic models |


---

## Pipeline Architecture (Added Context)

### Refresh Schedule

| Schedule | Pipeline | Time | Covers |
|----------|----------|------|--------|
| 1 | Master Pipeline (Bronze + Silver) | 6:00 AM UTC | Bronze Full + Incremental → Silver Master |
| 2 | Gold + Semantic Model Master | 7:00 AM UTC | Gold DDL → Dataflow Gen2 → Gold DML → Model Refresh |
| 3 | Audit Log Bronze Pipeline | 2:00 AM UTC | O365 Graph API full day audit pull |

### Why the 1-Hour Buffer Between Schedules?
Dataflow Gen2 reads Silver Delta tables using a point-in-time snapshot. Running Gold immediately after Silver completes risks reading a partially-flushed Delta transaction log. The 1hr buffer ensures Silver snapshots are stable before Dataflow Gen2 queries them.

### Why Staging Tables for Gold?
Dataflow Gen2's **Replace** write method drops and recreates the target table on every run. Writing directly to Gold would destroy the precision DDL (`DECIMAL(18,6)` → silently downcasts to `DOUBLE`) on every refresh. The staging → MERGE pattern isolates Dataflow from the Gold schema:
1. DDL notebook locks precision column types
2. Dataflow writes to `_staging` (Replace is safe there)
3. DML notebook MERGEs staging → Gold preserving the schema

### Bronze Pipeline Control Table Routing
Every bronze table load is driven by one row in `control_table`. The Fabric Pipeline uses:
- **ForEach** to iterate active rows
- **If Condition** (`load_type = 'FULL'`) to branch Full vs Incremental sub-pipelines
- **Switch** on `server_switch` to route to the correct on-prem SQL Server linked service
- Dynamic expressions to build source queries, sink table names, and watermark updates

### Additional Files

| File | Description |
|------|-------------|
| `17_bronze_master_pipeline_reference.py` | Full Bronze Master architecture with expression reference, JDBC server switch logic, and PySpark equivalent of the pipeline copy/watermark logic |
| `18_gold_ddl_precision_tables.py` | Gold + staging table DDL with explicit DECIMAL precision columns — runs before Dataflow Gen2 on every cycle |
| `19_gold_dml_merge_staging_to_gold.py` | MERGE from Dataflow Gen2 staging tables → Gold, with hash-based change detection, `_dw_created_at` preservation, and post-merge validation |
| `20_master_pipeline_architecture.py` | Full pipeline hierarchy diagram (ASCII), schedule design, all Fabric Pipeline dynamic expressions in one cheat-sheet |
