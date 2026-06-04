# /pipeline_docs — Pipeline Architecture, Orchestration & Reference

## Overview

Documentation and reference code for the full Master Pipeline hierarchy
that orchestrates all medallion layer refreshes. Covers Bronze ingestion
control, Silver orchestration, Gold precision DDL, Dataflow Gen2 staging
pattern, audit logging, and the complete Fabric Pipeline expression reference.

## Pipeline Execution Order

```
2:00 AM  ─── Audit Log Bronze Pipeline
              └── 07_bronze_audit_logs_graph_api.py

6:00 AM  ─── Master Pipeline (Bronze + Silver)
              │
              ├── Bronze_Master_Pipeline  [ref: 17_bronze_master_pipeline_reference.py]
              │     ├── Bronze_Full_Refresh_Pipeline    (load_type = FULL)
              │     └── Bronze_Incremental_Pipeline     (load_type = INCREMENTAL)
              │           Both driven by control_table  [ref: 10_control_table_ddl_silver_master.py]
              │
              └── Silver_Master_Notebook  [ref: 10_control_table_ddl_silver_master.py]
                    └── Calls all Silver DML notebooks via mssparkutils.notebook.run()
                    └── Updates control_table watermark on success
                    └── Calls audit_sp()                [ref: 16_pipeline_audit_log_ddl_audit_sp.py]

7:00 AM  ─── Gold + Semantic Model Master Pipeline
              │
              ├── Step 1: Gold DDL Notebook             [ref: 18_gold_ddl_precision_tables.py]
              │     └── CREATE TABLE IF NOT EXISTS for all Gold + staging tables
              │     └── Locks DECIMAL precision before Dataflow runs
              │
              ├── Step 2: Dataflow Gen2 Items (parallel) [ref: 11_gold_dataflow_gen2_m_queries.pq]
              │     └── M query transforms → Replace method → _staging tables
              │
              ├── Step 3: Gold DML Notebook             [ref: 19_gold_dml_merge_staging_to_gold.py]
              │     └── MERGE _staging → Gold for all tables
              │     └── Hash-based change detection
              │     └── Post-merge row count validation
              │
              ├── Step 4: SemanticModelLogs + System Tables
              │     └── /semantic_model/ notebooks run here
              │
              └── Step 5: Semantic Model Refresh
                    └── Fabric native refresh activity
                    └── audit_sp() called on completion
```

## Files

| File | Type | Purpose |
|------|------|---------|
| `10_control_table_ddl_silver_master.py` | DDL + Orchestrator | control_table schema with seed data + Silver Master Notebook that calls all DML notebooks in sequence |
| `11_gold_dataflow_gen2_m_queries.pq` | Power Query M | Three Dataflow Gen2 M query transformations: refresh summary, audit activity, model health scorecard |
| `12_long_running_jobs_monitor.sql` | T-SQL | 3-section SQL Server job monitoring query — see `/sql_server_ops/` for full context |
| `16_pipeline_audit_log_ddl_audit_sp.py` | DDL + Function | `pipeline_audit_log` table DDL + `audit_sp()` Python function callable from any notebook |
| `17_bronze_master_pipeline_reference.py` | Reference | Bronze pipeline design: all Fabric Pipeline expressions, Switch/If Condition logic, JDBC server map, PySpark equivalent of copy/watermark logic |
| `18_gold_ddl_precision_tables.py` | DDL | Gold + staging table definitions with explicit DECIMAL precision — idempotent, runs every pipeline cycle |
| `19_gold_dml_merge_staging_to_gold.py` | DML | Staging → Gold MERGE with validation, hash change detection, `_dw_created_at` preservation |
| `20_master_pipeline_architecture.py` | Architecture | Full pipeline hierarchy diagram, both schedules, 5 key design decisions, complete Fabric Pipeline expression cheat-sheet |

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| 1-hour buffer between Schedule 1 and Schedule 2 | Delta snapshot stability — Dataflow Gen2 reads Silver at query time; buffer ensures transaction log is fully flushed |
| DDL before Dataflow Gen2 runs | Prevents Dataflow Replace method from silently downcasting DECIMAL → DOUBLE |
| Staging → MERGE pattern | Isolates Dataflow schema from Gold schema; enables SCD Type 1 updates and row-level audit tracking |
| control_table watermark advancement only on SUCCESS | Prevents data gaps — failed runs do not advance the watermark, ensuring data is re-attempted on next cycle |
| audit_sp() called in finally block | Guarantees audit record is written regardless of success or failure — always have a complete run history |

| `21_deployment_pipeline_devops_integration.md` | Architecture | Dev → Prod Fabric Deployment Pipeline + Azure DevOps Git Integration. Environment strategy, promotion process, repo structure, rollback procedure, setup reference. |
