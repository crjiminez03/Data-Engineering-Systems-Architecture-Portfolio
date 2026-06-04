# /metadata — Metadata Master, Lineage, and Column Profiling

## Overview

A two-source metadata system that joins semantic model object definitions
(from the live .pbix XMLA connection) with physical lakehouse column
metadata (from Fabric system tables) to produce a unified catalog with
full end-to-end lineage. This catalog drives all column profiling,
health checks, and data quality analysis across the medallion layers.

## Two-Source Architecture

```
SOURCE 1: Semantic Model (sempy live connection)         SOURCE 2: Lakehouse (spark.catalog)
  sys_sem_tables                                           lakehouse_column_catalog
  sys_sem_columns        ──────────────────────────────►  (Bronze + Silver + Gold layers)
  sys_sem_measures              JOIN via lineage_map       is_nullable, is_partition_col
  sys_sem_partitions            (sem_table → lh_table)    table_row_count, last_modified
  sys_sem_storage_cols                                     last_operation, last_modified_by
         │                                                            │
         └──────────────────────────┬───────────────────────────────┘
                                    ▼
                          metadata_master_full
                   (one row per object with full lineage,
                    type alignment check, coverage flags,
                    storage size, semantic model context)
                                    │
                                    ▼
                     column_profiling_results_v2
                (null rates, cardinality, distributions,
                 health flags — all carrying lineage context
                 so you know which reports are impacted)
```

## Files (in recommended execution order)

| # | File | Purpose |
|---|------|---------|
| 1 | `13_lakehouse_system_tables_catalog.py` | Iterates all 3 Lakehouses via `spark.catalog.listTables()` and Delta history API. Builds `lakehouse_column_catalog` — physical column inventory with row counts, last modified, and partition info. |
| 2 | `14_metadata_master_full_lineage.py` | Joins semantic model objects (from `/semantic_model/02`) + lakehouse catalog + lineage map. Detects type mismatches, orphaned columns, hidden objects. Writes `metadata_master_full` and `metadata_lineage_map`. |
| 3 | `15_column_profiling_metadata_driven.py` | Profiles only columns with confirmed lakehouse mappings. Carries full semantic lineage on every result row. Flags CRITICAL (key columns with nulls, >80% null rate), WARN (type mismatches, high variance, >30% null), and OK. |
| — | `04_semantic_model_system_tables.py` | Earlier version of system tables loader (reference only — superseded by live sempy connection in `/semantic_model/02`). |
| — | `05_metadata_health_checks.py` | Standalone health check queries — null rates, measure catalog, layer row counts. Useful for quick checks without running full profiling. |
| — | `06_column_profiling.py` | Earlier version of column profiling (reference only — superseded by `15` which is metadata-catalog-driven). |

## Key Outputs

| Table | Description |
|-------|-------------|
| `lakehouse_column_catalog` | Physical column inventory across Bronze/Silver/Gold |
| `metadata_master_full` | Unified catalog with semantic + physical lineage |
| `metadata_lineage_map` | Explicit mapping of semantic model tables → lakehouse tables |
| `column_profiling_results_v2` | Statistical profile of every mapped column with health flags |

## Health Flags Explained

| Flag | Meaning | Action |
|------|---------|--------|
| `CRITICAL` | Key column has nulls, or >80% null rate | Immediate investigation required |
| `WARN` | Type mismatch, >30% null, single distinct value, or high variance | Review before next release |
| `OK` | No issues detected | No action needed |
